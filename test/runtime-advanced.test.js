import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
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

test('HTTP server rejects declared oversized bodies with a controlled 413', async () => {
  let handled = 0;
  const server = createHttpServer(async () => { handled += 1; return { status: 200 }; }, { maxBodyBytes: 4 });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  try {
    const response = await fetch(`http://127.0.0.1:${port}/upload`, { method: 'POST', body: '12345' });
    assert.equal(response.status, 413);
    assert.equal(response.headers.get('connection'), 'close');
    assert.equal(handled, 0);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});

test('HTTP server rejects chunked oversized bodies without resetting the client connection', async () => {
  let handled = 0;
  const server = createHttpServer(async () => { handled += 1; return { status: 200 }; }, { maxBodyBytes: 4 });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  try {
    const result = await new Promise((resolve, reject) => {
      const request = http.request({ host: '127.0.0.1', port, method: 'POST', path: '/upload', headers: { 'transfer-encoding': 'chunked' } }, (response) => {
        response.resume();
        response.once('end', () => resolve({ statusCode: response.statusCode, connection: response.headers.connection }));
      });
      request.once('error', reject);
      request.write('123');
      request.write('45');
      request.end();
    });
    assert.equal(result.statusCode, 413);
    assert.equal(result.connection, 'close');
    assert.equal(handled, 0);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
});
