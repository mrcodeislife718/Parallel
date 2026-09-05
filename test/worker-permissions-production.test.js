import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { CapabilitySet, CapabilityWorkerPool, ParallelWorkerPolicyError, deriveWorkerCapabilityConfig } from '../src/index.js';

const fixture = new URL('./fixtures/worker.mjs', import.meta.url);

test('worker permissions inherit parent authority by default and preserve denies', () => {
  const root = path.join(os.tmpdir(), 'parallel-worker-parent');
  const parent = new CapabilitySet({
    read: [root], write: [root], environment: ['SAFE'], network: ['example.com:443'],
    imports: ['pkg'], process: ['node'], crypto: true, timers: true, workers: true,
    deny: { environment: ['BLOCKED'], network: ['metadata.internal:80'] },
  });
  const child = deriveWorkerCapabilityConfig(parent, 'inherit');
  assert.deepEqual(child.read, [path.resolve(root)]);
  assert.deepEqual(child.environment, ['SAFE']);
  assert.deepEqual(child.network, ['example.com:443']);
  assert.deepEqual(child.imports, ['pkg']);
  assert.equal(child.crypto, true);
  assert.deepEqual(child.deny.environment, ['BLOCKED']);
});

test('worker permissions can only attenuate parent authority', () => {
  const root = path.join(os.tmpdir(), 'parallel-worker-parent');
  const childRoot = path.join(root, 'child');
  const parent = new CapabilitySet({
    read: [root], write: [root], environment: ['SAFE', 'OTHER'], network: ['example.com'],
    imports: ['pkg', 'other'], process: ['node'], crypto: true, timers: true, workers: true,
  });
  const child = deriveWorkerCapabilityConfig(parent, {
    read: [childRoot], write: [], environment: ['SAFE'], network: ['example.com:443'],
    imports: ['pkg'], process: false, crypto: false, timers: true, workers: false,
  });
  assert.deepEqual(child.read, [path.resolve(childRoot)]);
  assert.deepEqual(child.environment, ['SAFE']);
  assert.deepEqual(child.network, ['example.com:443']);
  assert.equal(child.process, false);
  assert.equal(child.crypto, false);
  assert.equal(child.workers, false);
  assert.throws(() => deriveWorkerCapabilityConfig(parent, { read: ['/etc'] }), ParallelWorkerPolicyError);
  assert.throws(() => deriveWorkerCapabilityConfig(parent, { environment: ['SECRET'] }), ParallelWorkerPolicyError);
  assert.throws(() => deriveWorkerCapabilityConfig(parent, { network: ['evil.example:443'] }), ParallelWorkerPolicyError);
  assert.throws(() => deriveWorkerCapabilityConfig(parent, { process: true }), ParallelWorkerPolicyError);
});

test('none worker policy drops all child authority', () => {
  const parent = new CapabilitySet({ filesystem: [os.tmpdir()], environment: ['HOME'], network: ['*'], imports: true, process: true, crypto: true, timers: true, workers: true });
  const child = deriveWorkerCapabilityConfig(parent, 'none');
  assert.deepEqual(child.read, []);
  assert.deepEqual(child.write, []);
  assert.deepEqual(child.environment, []);
  assert.deepEqual(child.network, []);
  assert.equal(child.imports, false);
  assert.equal(child.process, false);
  assert.equal(child.crypto, false);
  assert.equal(child.timers, false);
  assert.equal(child.workers, false);
});

test('capability worker pool passes only attenuated permission snapshot to worker', async () => {
  const parent = new CapabilitySet({ environment: ['SAFE', 'OTHER'], workers: true, timers: true });
  const pool = new CapabilityWorkerPool({
    size: 1,
    workerUrl: fixture,
    capabilities: parent,
    permissions: { environment: ['SAFE'], timers: true, workers: false },
    taskTimeoutMs: 1000,
    maxPending: 4,
    resourceLimits: { maxOldGenerationSizeMb: 64 },
  });
  try {
    const snapshot = await pool.exec({ permissions: true });
    assert.deepEqual(snapshot.environment, ['SAFE']);
    assert.equal(snapshot.timers, true);
    assert.equal(snapshot.workers, false);
    assert.deepEqual(await pool.exec({ value: 42 }), 42);
    assert.equal(pool.snapshot().pending, 0);
  } finally {
    await pool.close();
  }
});

test('worker pool bounds task lifetime, pending work, and supports cancellation', async () => {
  const parent = new CapabilitySet({ workers: true, timers: true });
  const pool = new CapabilityWorkerPool({ size: 1, workerUrl: fixture, capabilities: parent, permissions: 'none', taskTimeoutMs: 30, maxPending: 1 });
  try {
    await assert.rejects(pool.exec({ delayMs: 100, value: 1 }), /timed out/);
    const controller = new AbortController();
    const pending = pool.exec({ delayMs: 100, value: 2 }, { signal: controller.signal, timeoutMs: 500 });
    controller.abort(new Error('cancelled-by-test'));
    await assert.rejects(pending, /cancelled-by-test/);
  } finally {
    await pool.close();
  }
});
