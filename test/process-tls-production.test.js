import test from 'node:test';
import assert from 'node:assert/strict';
import { CapabilitySet, ParallelPermissionError, ParallelProcessError, connectTls, runProcess } from '../src/index.js';

test('TLS checks network authority before opening a socket', async () => {
  const caps = new CapabilitySet({ network: [] });
  await assert.rejects(
    connectTls({ host: 'example.com', port: 443, capabilities: caps, timeoutMs: 100 }),
    ParallelPermissionError,
  );
});

test('process execution requires explicit command authority', async () => {
  const caps = new CapabilitySet({ process: false });
  await assert.rejects(runProcess(process.execPath, ['-e', 'process.exit(0)'], { capabilities: caps }), ParallelPermissionError);
});

test('process runtime uses an explicit permission-checked environment', async () => {
  process.env.PARALLEL_VISIBLE = 'visible';
  process.env.PARALLEL_SECRET = 'secret';
  const caps = new CapabilitySet({ process: [process.execPath], environment: ['PARALLEL_VISIBLE'] });
  const result = await runProcess(process.execPath, ['-e', 'process.stdout.write(JSON.stringify({ visible: process.env.PARALLEL_VISIBLE ?? null, secret: process.env.PARALLEL_SECRET ?? null }))'], {
    capabilities: caps,
    inheritEnv: ['PARALLEL_VISIBLE'],
  });
  assert.equal(result.success, true);
  assert.deepEqual(JSON.parse(result.stdout.toString()), { visible: 'visible', secret: null });
  await assert.rejects(runProcess(process.execPath, ['-e', ''], { capabilities: caps, inheritEnv: ['PARALLEL_SECRET'] }), ParallelPermissionError);
});

test('process runtime bounds stdout and terminates oversized children', async () => {
  const caps = new CapabilitySet({ process: [process.execPath] });
  await assert.rejects(
    runProcess(process.execPath, ['-e', 'process.stdout.write("x".repeat(4096))'], { capabilities: caps, maxOutputBytes: 128 }),
    (error) => error instanceof ParallelProcessError && error.code === 'PARALLEL_PROCESS_OUTPUT_LIMIT',
  );
});

test('process runtime enforces timeout and abort cancellation', async () => {
  const caps = new CapabilitySet({ process: [process.execPath] });
  await assert.rejects(
    runProcess(process.execPath, ['-e', 'setTimeout(() => {}, 5000)'], { capabilities: caps, timeoutMs: 50 }),
    (error) => error instanceof ParallelProcessError && error.code === 'PARALLEL_PROCESS_TIMEOUT',
  );

  const controller = new AbortController();
  const pending = runProcess(process.execPath, ['-e', 'setTimeout(() => {}, 5000)'], { capabilities: caps, signal: controller.signal, timeoutMs: 5000 });
  setTimeout(() => controller.abort(new Error('cancelled-by-test')), 20);
  await assert.rejects(pending, /cancelled-by-test/);
});
