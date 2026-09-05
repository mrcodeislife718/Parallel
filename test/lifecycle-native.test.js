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
  nativeRuntimeManifest,
} from '../src/index.js';

const here = path.dirname(fileURLToPath(import.meta.url));

async function tempRoot(t, prefix) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  t.after(() => fs.rm(root, { recursive:true, force:true }));
  return root;
}

test('permission denial matrix covers runtime authority boundaries', () => {
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

test('scheduler and structured concurrency settle failures and reject work after shutdown', async () => {
  const scheduler = new TaskScheduler();
  await assert.rejects(scheduler.schedule(async () => { throw new Error('scheduled failure'); }), /scheduled failure/);
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

test('hosted compatibility worker pool propagates errors and closes cleanly', async () => {
  const caps = new CapabilitySet({ workers:true });
  const pool = new WorkerPool({ size:2, workerUrl:pathToFileURL(path.join(here,'fixtures','worker.mjs')), capabilities:caps });
  assert.equal(await pool.exec({ value:42 }), 42);
  await assert.rejects(pool.exec({ fail:true }), /worker failure/);
  await pool.close();
  assert.equal(pool.workers.length, 2);
});

test('stream adapters close resources and propagate transform failures', async () => {
  const streams = createStreams();
  let closed = false;
  const chunks = [];
  await streams.pipeline(
    streams.readable(['a','b']),
    streams.transform((chunk) => String(chunk).toUpperCase()),
    streams.writable({ write:(chunk) => chunks.push(chunk.toString()), close:() => { closed = true; } }),
  );
  assert.equal(closed, true);
  assert.equal(chunks.join(''), 'AB');
  await assert.rejects(streams.pipeline(
    streams.readable(['x']),
    streams.transform(() => { throw new Error('transform failed'); }),
    streams.writable({ write:() => {} }),
  ), /transform failed/);
});

test('HTTP handler failures are bounded and server closes cleanly', async () => {
  const server = createHttpServer(async () => { throw new Error('handler exploded'); });
  await new Promise((resolve,reject) => { server.once('error',reject); server.listen(0,'127.0.0.1',resolve); });
  const response = await fetch(`http://127.0.0.1:${server.address().port}/fail`);
  assert.equal(response.status, 500);
  assert.deepEqual(await response.json(), { error:'internal_error', message:'handler exploded' });
  await new Promise((resolve) => server.close(resolve));
  assert.equal(server.listening, false);
});

test('hosted compatibility TCP/process failures remain observable', async () => {
  const denied = new CapabilitySet({ network:[], process:false });
  assert.throws(() => connectTcp('127.0.0.1', 9, denied), ParallelPermissionError);
  assert.throws(() => spawnProcess(process.execPath, ['-e','process.exit(1)'], {}, denied), ParallelPermissionError);

  const child = spawnProcess(process.execPath, ['-e','process.stderr.write("nope");process.exit(7)'], {}, new CapabilitySet({ process:true }));
  let stderr='';
  child.stderr.setEncoding('utf8');
  child.stderr.on('data', (chunk) => stderr += chunk);
  const code = await new Promise((resolve,reject) => { child.once('error',reject); child.once('close',resolve); });
  assert.equal(code, 7);
  assert.equal(stderr, 'nope');
});

test('native ABI 6 kernel compiles independently and runs tasks, timers, and descriptor readiness', async (t) => {
  if (process.platform === 'win32') t.skip('native proof currently targets POSIX C11 runners');
  const root = await tempRoot(t, 'parallel-native-');
  const kernel = await buildNativeKernel({ outDir:path.join(root,'object') });
  assert.equal(kernel.ok, true, kernel.result?.stderr ?? kernel.result?.runtime?.stderr ?? kernel.result?.tcp?.stderr ?? kernel.result?.process?.stderr);
  assert.equal(kernel.manifest.abiVersion, 6);
  for (const feature of ['descriptor-readiness','poll-reactor','native-tcp-connect','native-process-spawn','thread-safe-task-posting','reactor-wakeup-pipe']) {
    assert.ok(kernel.manifest.eventLoop.includes(feature), `missing native feature: ${feature}`);
  }
  assert.equal(nativeRuntimeManifest().boundary, 'native-kernel');

  const probe = await linkNativeProbe(`#include <stdio.h>\n#include <unistd.h>\n#include <stdint.h>\n#include "parallel_runtime.h"\nstatic int add(void *ctx){ int *v=(int*)ctx; *v += 1; return 0; }\nstatic int add_ten(void *ctx){ int *v=(int*)ctx; *v += 10; return 0; }\nstatic int on_readable(int fd,uint32_t events,void *ctx){ int *v=(int*)ctx; char c=0; if(!(events&PARALLEL_IO_READABLE))return -1; if(read(fd,&c,1)!=1)return -2; *v += (int)c; return 0; }\nint main(void){ parallel_runtime rt; int value=30; int fds[2]; parallel_timer_id timer=0,cancelled=0; parallel_watch_id watch=0; if(pipe(fds)!=0)return 1; if(parallel_runtime_init(&rt,PARALLEL_CAP_TIMERS|PARALLEL_CAP_FILESYSTEM|PARALLEL_CAP_CRYPTO)!=0)return 2; if(rt.abi_version!=6)return 3; if(parallel_runtime_post(&rt,add,&value)!=0)return 4; if(parallel_runtime_set_timeout(&rt,1,add_ten,&value,&timer)!=0||timer==0)return 5; if(parallel_runtime_set_timeout(&rt,1,add_ten,&value,&cancelled)!=0)return 6; if(parallel_runtime_cancel_timer(&rt,cancelled)!=1)return 7; if(parallel_runtime_watch_descriptor(&rt,fds[0],PARALLEL_IO_READABLE,on_readable,&value,&watch)!=0)return 8; char byte=2; if(write(fds[1],&byte,1)!=1)return 9; for(int i=0;i<8 && value!=43;i++){ if(parallel_runtime_run_once(&rt,50)<0)return 10; } if(parallel_runtime_unwatch_descriptor(&rt,watch)!=1)return 11; if(value!=43)return 12; close(fds[0]); close(fds[1]); if(parallel_runtime_close(&rt)!=0)return 13; printf("%s:%d:%llu:%llu:%llu\\n",parallel_runtime_version(),value,(unsigned long long)rt.tasks_executed,(unsigned long long)rt.timers_executed,(unsigned long long)rt.io_events_executed); return 0; }`, { outDir:path.join(root,'probe') });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.equal(probe.stdout.trim(), 'parallel-native/0.6-abi6:43:3:1:1');
});

test('native TCP owns loopback connect, accept, write, read, and close', async (t) => {
  if (process.platform === 'win32') t.skip('native TCP proof currently targets POSIX C11 runners');
  const root = await tempRoot(t, 'parallel-native-tcp-');
  const probe = await linkNativeProbe(`#include <stdio.h>\n#include <stdint.h>\n#include <string.h>\n#include <sys/socket.h>\n#include <netinet/in.h>\n#include <arpa/inet.h>\n#include "parallel_runtime.h"\ntypedef struct state{parallel_runtime*rt;int listener,client,server,connected,accepted,read_ok;parallel_watch_id lw,cw,sw;}state;\nstatic int on_server(int fd,uint32_t events,void*ctx);\nstatic int on_listener(int fd,uint32_t events,void*ctx){state*s=ctx;if(!(events&PARALLEL_IO_READABLE))return 0;int c=-1;if(parallel_tcp_accept(s->rt,fd,&c)==1){s->server=c;s->accepted=1;parallel_runtime_unwatch_descriptor(s->rt,s->lw);if(parallel_runtime_watch_descriptor(s->rt,c,PARALLEL_IO_READABLE,on_server,s,&s->sw)!=0)return -1;}return 0;}\nstatic int on_client(int fd,uint32_t events,void*ctx){state*s=ctx;if(!(events&(PARALLEL_IO_WRITABLE|PARALLEL_IO_ERROR)))return 0;if(parallel_tcp_finish_connect(fd)==1){s->connected=1;parallel_runtime_unwatch_descriptor(s->rt,s->cw);if(parallel_tcp_write(fd,"ping",4)!=4)return -1;}return 0;}\nstatic int on_server(int fd,uint32_t events,void*ctx){state*s=ctx;if(!(events&PARALLEL_IO_READABLE))return 0;char b[8]={0};int64_t n=parallel_tcp_read(fd,b,sizeof(b));if(n==4&&memcmp(b,"ping",4)==0){s->read_ok=1;parallel_runtime_unwatch_descriptor(s->rt,s->sw);}return 0;}\nint main(void){parallel_runtime rt;if(parallel_runtime_init(&rt,PARALLEL_CAP_NETWORK)!=0)return 1;if(rt.abi_version!=6)return 2;int listener=-1;if(parallel_tcp_listen(&rt,"127.0.0.1",0,16,&listener)!=0)return 3;struct sockaddr_in a;socklen_t al=sizeof(a);if(getsockname(listener,(struct sockaddr*)&a,&al)!=0)return 4;uint16_t port=ntohs(a.sin_port);state s={.rt=&rt,.listener=listener,.client=-1,.server=-1};if(parallel_runtime_watch_descriptor(&rt,listener,PARALLEL_IO_READABLE,on_listener,&s,&s.lw)!=0)return 5;int cr=parallel_tcp_connect(&rt,"127.0.0.1",port,&s.client);if(cr<0)return 6;if(cr==1){s.connected=1;if(parallel_tcp_write(s.client,"ping",4)!=4)return 7;}else if(parallel_runtime_watch_descriptor(&rt,s.client,PARALLEL_IO_WRITABLE,on_client,&s,&s.cw)!=0)return 8;for(int i=0;i<100&&!(s.connected&&s.accepted&&s.read_ok);i++)if(parallel_runtime_run_once(&rt,20)<0)return 9;if(!s.connected||!s.accepted||!s.read_ok)return 10;if(parallel_tcp_set_nodelay(s.client,1)!=0)return 11;parallel_tcp_close(s.server);parallel_tcp_close(s.client);parallel_tcp_close(listener);if(parallel_runtime_close(&rt)!=0)return 12;printf("tcp:%d:%d:%d:%llu\\n",s.connected,s.accepted,s.read_ok,(unsigned long long)rt.io_events_executed);return 0;}`, { outDir:root });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.match(probe.stdout.trim(), /^tcp:1:1:1:[1-9][0-9]*$/);
});

test('native process owns piped stdio, reactor readiness, exit status, and reaping', async (t) => {
  if (process.platform === 'win32') t.skip('native process proof currently targets POSIX C11 runners');
  const root = await tempRoot(t, 'parallel-native-process-');
  const probe = await linkNativeProbe(`#include <stdio.h>\n#include <stdint.h>\n#include <string.h>\n#include "parallel_runtime.h"\ntypedef struct state{parallel_runtime*rt;parallel_process*process;char output[64];size_t length;parallel_watch_id watch;}state;\nstatic int on_stdout(int fd,uint32_t events,void*ctx){state*s=ctx;if(!(events&(PARALLEL_IO_READABLE|PARALLEL_IO_HANGUP)))return 0;int64_t n=parallel_process_read_stdout(s->process,s->output+s->length,sizeof(s->output)-1-s->length);if(n>0){s->length+=(size_t)n;s->output[s->length]='\\0';}if(n==0||(events&PARALLEL_IO_HANGUP))parallel_runtime_unwatch_descriptor(s->rt,s->watch);return 0;}\nint main(void){parallel_runtime rt;parallel_process child;if(parallel_runtime_init(&rt,PARALLEL_CAP_PROCESS)!=0)return 1;if(rt.abi_version!=6)return 2;char*argv[]={"cat",NULL};if(parallel_process_spawn(&rt,"/bin/cat",argv,NULL,NULL,&child)!=0)return 3;state s={.rt=&rt,.process=&child};if(parallel_runtime_watch_descriptor(&rt,child.stdout_descriptor,PARALLEL_IO_READABLE,on_stdout,&s,&s.watch)!=0)return 4;const char msg[]="parallel-native-process\\n";if(parallel_process_write_stdin(&child,msg,sizeof(msg)-1)!=(int64_t)(sizeof(msg)-1))return 5;if(parallel_process_close_stdin(&child)!=0)return 6;for(int i=0;i<200&&(!child.exited||s.length==0);i++){if(parallel_runtime_run_once(&rt,10)<0)return 7;if(parallel_process_poll_exit(&child)<0)return 8;}if(strcmp(s.output,msg)!=0)return 9;if(parallel_process_poll_exit(&child)!=1)return 10;if(child.exit_code!=0||child.term_signal!=0)return 11;if(parallel_process_dispose(&child,0)!=0)return 12;if(parallel_runtime_close(&rt)!=0)return 13;printf("process:%d:%d:%zu:%llu\\n",child.exited,child.exit_code,s.length,(unsigned long long)rt.io_events_executed);return 0;}`, { outDir:root });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.match(probe.stdout.trim(), /^process:1:0:[1-9][0-9]*:[1-9][0-9]*$/);
});
