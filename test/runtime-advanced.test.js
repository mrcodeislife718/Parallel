import test from 'node:test';
import assert from 'node:assert/strict';
import { CapabilitySet, TaskScheduler, TaskGroup, createStreams, createHttpServer, createCrypto, createRuntimeModuleAbi, RuntimeProfiler, ParallelPermissionError } from '../src/index.js';

test('capabilities enforce filesystem, environment, and network boundaries', () => {
  const caps = new CapabilitySet({ filesystem: ['/tmp/parallel-test'], environment: ['HOME'], network: ['localhost:8080'], process: false });
  assert.match(caps.assertFile('/tmp/parallel-test/file.txt'), /parallel-test/);
  assert.throws(() => caps.assertFile('/etc/passwd'), ParallelPermissionError);
  assert.throws(() => caps.assertHost('example.com', 443), ParallelPermissionError);
  assert.throws(() => caps.assertProcess(), ParallelPermissionError);
});

test('scheduler honors priority and task groups settle', async () => {
  const scheduler = new TaskScheduler();
  const order = [];
  const a = scheduler.schedule(() => order.push('low'), { priority: 1 });
  const b = scheduler.schedule(() => order.push('high'), { priority: 10 });
  await Promise.all([a,b]);
  assert.deepEqual(order, ['high','low']);
  const group = new TaskGroup();
  group.run(async () => 1); group.run(async () => 2);
  const settled = await group.join();
  assert.equal(settled.length, 2);
});

test('HTTP server, streams, crypto, ABI, and profiler execute', async () => {
  const server = createHttpServer(async () => ({ status: 200, body: { ok: true } }));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  const response = await fetch(`http://127.0.0.1:${address.port}/`);
  assert.deepEqual(await response.json(), { ok: true });
  await new Promise((resolve) => server.close(resolve));

  const cryptoApi = createCrypto(new CapabilitySet({ crypto: true }));
  assert.equal(cryptoApi.digest('sha256', 'x').length, 32);
  const abi = createRuntimeModuleAbi(); abi.register('demo', () => ({ ok: true }));
  assert.deepEqual(abi.load('demo'), { ok: true });
  const profiler = new RuntimeProfiler(); await profiler.measure('work', async () => 42);
  assert.equal(profiler.report()[0].name, 'work');
  assert.equal(typeof createStreams().readable, 'function');
});
