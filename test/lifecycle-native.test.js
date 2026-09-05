import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import {
  CapabilitySet,
  TaskScheduler,
  TaskGroup,
  WorkerPool,
  createStreams,
  createHttpServer,
  connectTcp,
  spawnProcess,
  createCrypto,
  ParallelPermissionError,
  buildNativeKernel,
  linkNativeProbe,
  nativeRuntimeManifest
} from '../src/index.js';

const here = path.dirname(fileURLToPath(import.meta.url));

test('permission denial matrix covers timers, files, environment, network, process, crypto, and workers', () => {
  const caps = new CapabilitySet({ filesystem:[], environment:[], network:[], process:false, crypto:false, timers:false, workers:false });
  assert.throws(() => caps.assertFile('/tmp/nope'), ParallelPermissionError);
  assert.throws(() => caps.assertEnv('SECRET'), ParallelPermissionError);
  assert.throws(() => caps.assertHost('127.0.0.1', 80), ParallelPermissionError);
  assert.throws(() => caps.assertProcess(), ParallelPermissionError);
  assert.throws(() => caps.assertCrypto(), ParallelPermissionError);
  assert.throws(() => caps.assertTimers(), ParallelPermissionError);
  assert.throws(() => caps.assertWorkers(), ParallelPermissionError);
  assert.throws(() => createCrypto(caps).randomBytes(1), ParallelPermissionError);
});

test('scheduler and structured concurrency settle failures and reject new work after shutdown', async () => {
  const scheduler = new TaskScheduler();
  const failure = scheduler.schedule(async () => { throw new Error('scheduled failure'); });
  await assert.rejects(failure, /scheduled failure/);
  scheduler.close();
  assert.throws(() => scheduler.schedule(() => 1), /closed/);

  const group = new TaskGroup();
  group.run(async (signal) => new Promise((resolve) => signal.addEventListener('abort', () => resolve('cancelled'), { once:true })));
  group.run(async () => { throw new Error('child failure'); });
  group.cancel(new Error('cancel proof'));
  const settled = await group.join();
  assert.equal(settled.length, 2);
  assert.ok(settled.some((entry) => entry.status === 'rejected' && /child failure/.test(entry.reason.message)));
});

test('worker pool propagates errors and terminates workers after completed work', async () => {
  const caps = new CapabilitySet({ workers:true });
  const pool = new WorkerPool({ size:2, workerUrl:pathToFileURL(path.join(here,'fixtures','worker.mjs')), capabilities:caps });
  assert.equal(await pool.exec({ value:42 }), 42);
  await assert.rejects(pool.exec({ fail:true }), /worker failure/);
  await pool.close();
  assert.equal(pool.workers.length, 2);
});

test('streams close resources on success and propagate transform failures', async () => {
  const streams = createStreams();
  let closed = false;
  const chunks = [];
  await streams.pipeline(
    streams.readable(['a','b']),
    streams.transform((chunk) => String(chunk).toUpperCase()),
    streams.writable({ write:(chunk) => chunks.push(chunk.toString()), close:() => { closed = true; } })
  );
  assert.equal(closed, true);
  assert.equal(chunks.join(''), 'AB');
  await assert.rejects(streams.pipeline(
    streams.readable(['x']),
    streams.transform(() => { throw new Error('transform failed'); }),
    streams.writable({ write:() => {} })
  ), /transform failed/);
});

test('HTTP handler failures become bounded 500 responses and server closes cleanly', async () => {
  const server = createHttpServer(async () => { throw new Error('handler exploded'); });
  await new Promise((resolve,reject) => { server.once('error',reject); server.listen(0,'127.0.0.1',resolve); });
  const address = server.address();
  const response = await fetch(`http://127.0.0.1:${address.port}/fail`);
  assert.equal(response.status, 500);
  assert.deepEqual(await response.json(), { error:'internal_error', message:'handler exploded' });
  await new Promise((resolve) => server.close(resolve));
  assert.equal(server.listening, false);
});

test('TCP refusal, process failure, and capability denial are observable failure paths', async () => {
  const denied = new CapabilitySet({ network:[], process:false });
  assert.throws(() => connectTcp('127.0.0.1', 9, denied), ParallelPermissionError);
  assert.throws(() => spawnProcess(process.execPath, ['-e','process.exit(1)'], {}, denied), ParallelPermissionError);

  const processCaps = new CapabilitySet({ process:true });
  const child = spawnProcess(process.execPath, ['-e','process.stderr.write("nope");process.exit(7)'], {}, processCaps);
  let stderr=''; child.stderr.setEncoding('utf8'); child.stderr.on('data', (chunk) => stderr += chunk);
  const code = await new Promise((resolve,reject) => { child.once('error',reject); child.once('close',resolve); });
  assert.equal(code, 7);
  assert.equal(stderr, 'nope');
});

test('native Parallel kernel compiles independently of Node and runs tasks, timers, and descriptor readiness', async (t) => {
  if (process.platform === 'win32') t.skip('native proof currently targets POSIX C11 runners');
  const root = await fs.mkdtemp(path.join(os.tmpdir(),'parallel-native-'));
  t.after(() => fs.rm(root, { recursive:true, force:true }));
  const kernel = await buildNativeKernel({ outDir:path.join(root,'object') });
  assert.equal(kernel.ok, true, kernel.result?.stderr ?? kernel.result?.runtime?.stderr ?? kernel.result?.tcp?.stderr ?? kernel.result?.process?.stderr);
  assert.equal(kernel.manifest.abiVersion, 5);
  assert.ok(kernel.manifest.eventLoop.includes('descriptor-readiness'));
  assert.ok(kernel.manifest.eventLoop.includes('poll-reactor'));
  assert.ok(kernel.manifest.eventLoop.includes('native-tcp-connect'));
  assert.ok(kernel.manifest.eventLoop.includes('native-process-spawn'));
  assert.equal(nativeRuntimeManifest().boundary, 'native-kernel');

  const probe = await linkNativeProbe(`#include <stdio.h>\n#include <unistd.h>\n#include <stdint.h>\n#include \"parallel_runtime.h\"\nstatic int add(void *ctx){ int *v=(int*)ctx; *v += 1; return 0; }\nstatic int add_ten(void *ctx){ int *v=(int*)ctx; *v += 10; return 0; }\nstatic int on_readable(int fd,uint32_t events,void *ctx){ int *v=(int*)ctx; char c=0; if(!(events & PARALLEL_IO_READABLE))return -1; if(read(fd,&c,1)!=1)return -2; *v += (int)c; return 0; }\nint main(void){ parallel_runtime rt; int value=30; int fds[2]; parallel_timer_id timer=0,cancelled=0; parallel_watch_id watch=0; if(pipe(fds)!=0)return 1; if(parallel_runtime_init(&rt, PARALLEL_CAP_TIMERS|PARALLEL_CAP_FILESYSTEM|PARALLEL_CAP_CRYPTO)!=0)return 2; if(rt.abi_version!=5)return 3; if(!parallel_runtime_has_capability(&rt,PARALLEL_CAP_TIMERS))return 4; if(parallel_runtime_post(&rt,add,&value)!=0)return 5; if(parallel_runtime_set_timeout(&rt,1,add_ten,&value,&timer)!=0||timer==0)return 6; if(parallel_runtime_set_timeout(&rt,1,add_ten,&value,&cancelled)!=0)return 7; if(parallel_runtime_cancel_timer(&rt,cancelled)!=1)return 8; if(parallel_runtime_watch_descriptor(&rt,fds[0],PARALLEL_IO_READABLE,on_readable,&value,&watch)!=0||watch==0)return 9; char byte=2; if(write(fds[1],&byte,1)!=1)return 10; if(parallel_runtime_run_once(&rt,50)<0)return 11; if(parallel_runtime_unwatch_descriptor(&rt,watch)!=1)return 12; if(parallel_runtime_run(&rt)!=0)return 13; if(value!=43)return 14; if(rt.tasks_executed!=3||rt.timers_executed!=1||rt.io_events_executed!=1)return 15; close(fds[0]); close(fds[1]); if(parallel_runtime_close(&rt)!=0)return 16; if(parallel_runtime_post(&rt,add,&value)!=-2)return 17; printf(\"%s:%d:%llu:%llu:%llu\\n\", parallel_runtime_version(), value, (unsigned long long)rt.tasks_executed, (unsigned long long)rt.timers_executed, (unsigned long long)rt.io_events_executed); return 0; }`, { outDir:path.join(root,'probe') });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.equal(probe.stdout.trim(), 'parallel-native/0.5-abi5:43:3:1:1');
});

test('native Parallel TCP owns loopback connect, accept, write, read, and close without Node networking', async (t) => {
  if (process.platform === 'win32') t.skip('native TCP proof currently targets POSIX C11 runners');
  const root = await fs.mkdtemp(path.join(os.tmpdir(),'parallel-native-tcp-'));
  t.after(() => fs.rm(root, { recursive:true, force:true }));

  const probe = await linkNativeProbe(`#include <stdio.h>\n#include <stdint.h>\n#include <string.h>\n#include <sys/socket.h>\n#include <netinet/in.h>\n#include <arpa/inet.h>\n#include \"parallel_runtime.h\"\ntypedef struct state { parallel_runtime *rt; int listener; int client; int server; int connected; int accepted; int read_ok; parallel_watch_id listener_watch; parallel_watch_id client_watch; parallel_watch_id server_watch; } state;\nstatic int on_server(int fd,uint32_t events,void *ctx);\nstatic int on_listener(int fd,uint32_t events,void *ctx){ state *s=(state*)ctx; if(!(events&PARALLEL_IO_READABLE))return -1; int c=-1; int r=parallel_tcp_accept(s->rt,fd,&c); if(r==1){ s->server=c; s->accepted=1; parallel_runtime_unwatch_descriptor(s->rt,s->listener_watch); if(parallel_runtime_watch_descriptor(s->rt,c,PARALLEL_IO_READABLE,on_server,ctx,&s->server_watch)!=0)return -2; } return 0; }\nstatic int on_client(int fd,uint32_t events,void *ctx){ state *s=(state*)ctx; if(!(events&(PARALLEL_IO_WRITABLE|PARALLEL_IO_ERROR)))return -1; int r=parallel_tcp_finish_connect(fd); if(r==1){ s->connected=1; parallel_runtime_unwatch_descriptor(s->rt,s->client_watch); const char msg[] = \"ping\"; if(parallel_tcp_write(fd,msg,4)!=4)return -2; } return 0; }\nstatic int on_server(int fd,uint32_t events,void *ctx){ state *s=(state*)ctx; if(!(events&PARALLEL_IO_READABLE))return -1; char buf[8]={0}; int64_t n=parallel_tcp_read(fd,buf,sizeof(buf)); if(n==4 && memcmp(buf,\"ping\",4)==0){ s->read_ok=1; parallel_runtime_unwatch_descriptor(s->rt,s->server_watch); } return 0; }\nint main(void){ parallel_runtime rt; if(parallel_runtime_init(&rt,PARALLEL_CAP_NETWORK)!=0)return 1; if(rt.abi_version!=5)return 2; int listener=-1; if(parallel_tcp_listen(&rt,\"127.0.0.1\",0,16,&listener)!=0)return 3; struct sockaddr_in addr; socklen_t addrlen=sizeof(addr); if(getsockname(listener,(struct sockaddr*)&addr,&addrlen)!=0)return 4; uint16_t port=ntohs(addr.sin_port); if(port==0)return 5; state s={.rt=&rt,.listener=listener,.client=-1,.server=-1}; if(parallel_runtime_watch_descriptor(&rt,listener,PARALLEL_IO_READABLE,on_listener,&s,&s.listener_watch)!=0)return 6; int cr=parallel_tcp_connect(&rt,\"127.0.0.1\",port,&s.client); if(cr<0)return 7; if(cr==1){ s.connected=1; const char msg[] = \"ping\"; if(parallel_tcp_write(s.client,msg,4)!=4)return 8; } else if(parallel_runtime_watch_descriptor(&rt,s.client,PARALLEL_IO_WRITABLE,on_client,&s,&s.client_watch)!=0)return 9; for(int i=0;i<100 && !(s.connected&&s.accepted&&s.read_ok);i++){ int r=parallel_runtime_run_once(&rt,20); if(r<0)return 10; } if(!s.connected||!s.accepted||!s.read_ok)return 11; if(parallel_tcp_set_nodelay(s.client,1)!=0)return 12; if(parallel_tcp_close(s.server)!=0)return 13; if(parallel_tcp_close(s.client)!=0)return 14; if(parallel_tcp_close(listener)!=0)return 15; if(parallel_runtime_close(&rt)!=0)return 16; printf(\"tcp:%d:%d:%d:%llu\\n\",s.connected,s.accepted,s.read_ok,(unsigned long long)rt.io_events_executed); return 0; }`, { outDir:path.join(root,'probe') });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.match(probe.stdout.trim(), /^tcp:1:1:1:[1-9][0-9]*$/);
});

test('native Parallel process owns piped stdio, reactor readiness, exit status, and reaping without Node spawn', async (t) => {
  if (process.platform === 'win32') t.skip('native process proof currently targets POSIX C11 runners');
  const root = await fs.mkdtemp(path.join(os.tmpdir(),'parallel-native-process-'));
  t.after(() => fs.rm(root, { recursive:true, force:true }));

  const probe = await linkNativeProbe(`#include <stdio.h>\n#include <stdint.h>\n#include <string.h>\n#include <signal.h>\n#include \"parallel_runtime.h\"\ntypedef struct state { parallel_runtime *rt; parallel_process *process; char output[64]; size_t length; int readable; parallel_watch_id stdout_watch; } state;\nstatic int on_stdout(int fd,uint32_t events,void *ctx){ state *s=(state*)ctx; if(!(events&(PARALLEL_IO_READABLE|PARALLEL_IO_HANGUP)))return 0; int64_t n=parallel_process_read_stdout(s->process,s->output+s->length,sizeof(s->output)-1-s->length); if(n>0){ s->length+=(size_t)n; s->output[s->length]='\\0'; s->readable=1; } if(n==0 || (events&PARALLEL_IO_HANGUP)) parallel_runtime_unwatch_descriptor(s->rt,s->stdout_watch); return 0; }\nint main(void){ parallel_runtime rt; parallel_process child; if(parallel_runtime_init(&rt,PARALLEL_CAP_PROCESS)!=0)return 1; if(rt.abi_version!=5)return 2; char *argv[]={\"cat\",NULL}; if(parallel_process_spawn(&rt,\"/bin/cat\",argv,NULL,NULL,&child)!=0)return 3; state s={.rt=&rt,.process=&child}; if(parallel_runtime_watch_descriptor(&rt,child.stdout_descriptor,PARALLEL_IO_READABLE,on_stdout,&s,&s.stdout_watch)!=0)return 4; const char msg[]=\"parallel-native-process\\n\"; int64_t written=parallel_process_write_stdin(&child,msg,sizeof(msg)-1); if(written!=(int64_t)(sizeof(msg)-1))return 5; if(parallel_process_close_stdin(&child)!=0)return 6; for(int i=0;i<200 && (!s.readable || !child.exited);i++){ int r=parallel_runtime_run_once(&rt,10); if(r<0)return 7; int exited=parallel_process_poll_exit(&child); if(exited<0)return 8; } if(!s.readable)return 9; if(strcmp(s.output,msg)!=0)return 10; if(parallel_process_poll_exit(&child)!=1)return 11; if(child.exit_code!=0||child.term_signal!=0)return 12; if(parallel_process_signal(&child,SIGTERM)!=0)return 13; if(parallel_process_dispose(&child,0)!=0)return 14; if(parallel_runtime_close(&rt)!=0)return 15; printf(\"process:%d:%d:%zu:%llu\\n\",child.exited,child.exit_code,s.length,(unsigned long long)rt.io_events_executed); return 0; }`, { outDir:path.join(root,'probe') });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.match(probe.stdout.trim(), /^process:1:0:[1-9][0-9]*:[1-9][0-9]*$/);
});
