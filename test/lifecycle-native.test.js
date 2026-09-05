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

test('native Parallel kernel compiles independently of Node and executes queued tasks and timers', async (t) => {
  if (process.platform === 'win32') t.skip('native proof currently targets POSIX C11 runners');
  const root = await fs.mkdtemp(path.join(os.tmpdir(),'parallel-native-'));
  t.after(() => fs.rm(root, { recursive:true, force:true }));
  const kernel = await buildNativeKernel({ outDir:path.join(root,'object') });
  assert.equal(kernel.ok, true, kernel.result?.stderr);
  assert.equal(kernel.manifest.abiVersion, 2);
  assert.ok(kernel.manifest.eventLoop.includes('one-shot-timers'));
  assert.equal(nativeRuntimeManifest().boundary, 'native-kernel');

  const probe = await linkNativeProbe(`#include <stdio.h>\n#include "parallel_runtime.h"\nstatic int add(void *ctx){ int *v=(int*)ctx; *v += 1; return 0; }\nstatic int add_ten(void *ctx){ int *v=(int*)ctx; *v += 10; return 0; }\nint main(void){ parallel_runtime rt; int value=30; parallel_timer_id timer=0,cancelled=0; if(parallel_runtime_init(&rt, PARALLEL_CAP_TIMERS|PARALLEL_CAP_CRYPTO)!=0)return 1; if(rt.abi_version!=2)return 2; if(!parallel_runtime_has_capability(&rt,PARALLEL_CAP_TIMERS))return 3; if(parallel_runtime_post(&rt,add,&value)!=0)return 4; if(parallel_runtime_set_timeout(&rt,1,add_ten,&value,&timer)!=0||timer==0)return 5; if(parallel_runtime_set_timeout(&rt,1,add_ten,&value,&cancelled)!=0)return 6; if(parallel_runtime_cancel_timer(&rt,cancelled)!=1)return 7; if(parallel_runtime_run(&rt)!=0)return 8; if(value!=41)return 9; if(rt.tasks_executed!=2||rt.timers_executed!=1)return 10; if(parallel_runtime_close(&rt)!=0)return 11; if(parallel_runtime_post(&rt,add,&value)!=-2)return 12; printf("%s:%d:%llu:%llu\\n", parallel_runtime_version(), value, (unsigned long long)rt.tasks_executed, (unsigned long long)rt.timers_executed); return 0; }`, { outDir:path.join(root,'probe') });
  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.equal(probe.stdout.trim(), 'parallel-native/0.2-abi2:41:2:1');
});
