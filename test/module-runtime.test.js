import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { CapabilitySet, ParallelModuleRuntime, createModuleGraphManifest } from '../src/index.js';

const digest = (value) => crypto.createHash('sha256').update(value).digest('hex');

async function fixture(files) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'parallel-modules-'));
  for (const [name, source] of Object.entries(files)) {
    const file = path.join(root, name);
    await fs.mkdir(path.dirname(file), { recursive: true });
    await fs.writeFile(file, source, 'utf8');
  }
  return root;
}

test('Parallel executes a verified static Cannon/Nova module graph', async () => {
  const math = 'export function add(a, b) { return a + b; }\n';
  const main = 'import { add } from "./math"; export function answer(a, b) { return add(a, b); }\n';
  const root = await fixture({ 'main.mjs': main, 'math.mjs': math });
  const manifest = createModuleGraphManifest({
    entry: 'main',
    modules: [
      { id: 'main', file: 'main.mjs', digest: digest(main), dependencies: { './math': 'math' } },
      { id: 'math', file: 'math.mjs', digest: digest(math), dependencies: {} },
    ],
  });
  const runtime = new ParallelModuleRuntime({ projectRoot: root });
  const result = await runtime.execute(manifest, { invoke: { export: 'answer', args: [20, 22] } });
  assert.equal(result.result, 42);
  assert.deepEqual(result.exports, ['answer']);
  assert.match(result.graphDigest, /^[a-f0-9]{64}$/);
});

test('Parallel refuses tampered module bytes before execution', async () => {
  const source = 'export const value = 1;\n';
  const root = await fixture({ 'main.mjs': source });
  const manifest = createModuleGraphManifest({ entry: 'main', modules: [{ id: 'main', file: 'main.mjs', digest: digest(source), dependencies: {} }] });
  await fs.writeFile(path.join(root, 'main.mjs'), 'export const value = 2;\n', 'utf8');
  const runtime = new ParallelModuleRuntime({ projectRoot: root });
  await assert.rejects(() => runtime.prepare(manifest), /digest mismatch/);
});

test('Parallel rejects undeclared static imports at the runtime linker', async () => {
  const dep = 'export const value = 1;\n';
  const main = 'import { value } from "./dep"; export const result = value;\n';
  const root = await fixture({ 'main.mjs': main, 'dep.mjs': dep });
  const manifest = createModuleGraphManifest({
    entry: 'main',
    modules: [
      { id: 'main', file: 'main.mjs', digest: digest(main), dependencies: {} },
      { id: 'dep', file: 'dep.mjs', digest: digest(dep), dependencies: {} },
    ],
  });
  const runtime = new ParallelModuleRuntime({ projectRoot: root });
  await assert.rejects(() => runtime.execute(manifest), /undeclared static import/);
});

test('Parallel dynamic imports are denied only when used and allowed by explicit capability', async () => {
  const optional = 'export const value = 7;\n';
  const main = 'export async function maybe(load) { if (!load) return 0; const mod = await import("optional"); return mod.value; }\n';
  const root = await fixture({ 'main.mjs': main, 'optional.mjs': optional });
  const manifest = createModuleGraphManifest({
    entry: 'main',
    modules: [
      { id: 'main', file: 'main.mjs', digest: digest(main), dependencies: {}, dynamicDependencies: { optional: 'optional' } },
      { id: 'optional', file: 'optional.mjs', digest: digest(optional), dependencies: {} },
    ],
  });
  const denied = new ParallelModuleRuntime({ projectRoot: root, capabilities: new CapabilitySet() });
  assert.equal((await denied.execute(manifest, { invoke: { export: 'maybe', args: [false] } })).result, 0);
  await assert.rejects(() => denied.execute(manifest, { invoke: { export: 'maybe', args: [true] } }), /dynamic import denied/);

  const allowed = new ParallelModuleRuntime({ projectRoot: root, capabilities: new CapabilitySet({ imports: ['optional'] }) });
  assert.equal((await allowed.execute(manifest, { invoke: { export: 'maybe', args: [true] } })).result, 7);
});

test('Parallel rejects module paths and symlinks that escape the project root', async (t) => {
  const source = 'export const value = 1;\n';
  const root = await fixture({ 'main.mjs': source });
  const outsideRoot = await fs.mkdtemp(path.join(os.tmpdir(), 'parallel-outside-'));
  const outside = path.join(outsideRoot, 'outside.mjs');
  await fs.writeFile(outside, source, 'utf8');
  const link = path.join(root, 'escape.mjs');
  try { await fs.symlink(outside, link); } catch (error) { if (error.code === 'EPERM') return t.skip('symlink creation unavailable'); throw error; }
  const manifest = createModuleGraphManifest({ entry: 'main', modules: [{ id: 'main', file: 'escape.mjs', digest: digest(source), dependencies: {} }] });
  const runtime = new ParallelModuleRuntime({ projectRoot: root });
  await assert.rejects(() => runtime.prepare(manifest), /symlink escapes project root/);
});
