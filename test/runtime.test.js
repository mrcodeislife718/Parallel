import test from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';
import { createRuntime, ParallelPermissionError } from '../src/index.js';

test('Parallel sleep resolves through the runtime timer capability', async () => {
  const runtime = createRuntime();
  const start = Date.now();
  await runtime.sleep(5);
  assert.ok(Date.now() - start >= 0);
});

test('Parallel filesystem reads and writes inside allowed roots', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'parallel-'));
  const runtime = createRuntime({ filesystem: [root] });
  const file = path.join(root, 'nested', 'hello.txt');
  await runtime.writeText(file, 'hello');
  assert.equal(await runtime.readText(file), 'hello');
  await fs.rm(root, { recursive: true, force: true });
});

test('Parallel blocks filesystem access outside allowed roots', async () => {
  const runtime = createRuntime({ filesystem: [] });
  await assert.rejects(() => runtime.readText('/tmp/not-allowed.txt'), ParallelPermissionError);
});

test('Parallel environment access is allowlisted', () => {
  process.env.PARALLEL_TEST_VALUE = 'works';
  const runtime = createRuntime({ environment: ['PARALLEL_TEST_VALUE'] });
  assert.equal(runtime.env('PARALLEL_TEST_VALUE'), 'works');
  assert.throws(() => runtime.env('HOME'), ParallelPermissionError);
  delete process.env.PARALLEL_TEST_VALUE;
});
