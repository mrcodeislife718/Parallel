import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { CapabilitySet, ParallelPermissionError, readFile, writeFile, spawnProcess } from '../src/index.js';

async function fixture(t) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'parallel-security-'));
  const allowed = path.join(root, 'allowed');
  const outside = path.join(root, 'outside');
  await fs.mkdir(allowed); await fs.mkdir(outside);
  t.after(() => fs.rm(root, { recursive: true, force: true }));
  return { root, allowed, outside };
}

test('read and write filesystem capabilities are separated', async (t) => {
  const { allowed } = await fixture(t);
  const readable = path.join(allowed, 'read.txt');
  await fs.writeFile(readable, 'ok');
  const caps = new CapabilitySet({ read: [allowed], write: [] });
  assert.equal((await readFile(readable, caps)).toString(), 'ok');
  await assert.rejects(() => writeFile(path.join(allowed, 'blocked.txt'), 'no', caps), ParallelPermissionError);
});

test('filesystem authority rejects symlink escapes outside an allowed root', async (t) => {
  const { allowed, outside } = await fixture(t);
  const secret = path.join(outside, 'secret.txt');
  await fs.writeFile(secret, 'secret');
  await fs.symlink(secret, path.join(allowed, 'escape'));
  const caps = new CapabilitySet({ filesystem: [allowed] });
  await assert.rejects(() => readFile(path.join(allowed, 'escape'), caps), ParallelPermissionError);
});

test('writes through symlinked directories cannot escape an allowed root', async (t) => {
  const { allowed, outside } = await fixture(t);
  await fs.symlink(outside, path.join(allowed, 'linked-dir'));
  const caps = new CapabilitySet({ write: [allowed] });
  await assert.rejects(() => writeFile(path.join(allowed, 'linked-dir', 'owned.txt'), 'blocked', caps), ParallelPermissionError);
  await assert.rejects(() => fs.access(path.join(outside, 'owned.txt')));
});

test('process capability can restrict executable names instead of granting arbitrary spawn', () => {
  const caps = new CapabilitySet({ process: ['node'] });
  assert.equal(caps.assertProcess(process.execPath), true);
  assert.throws(() => caps.assertProcess('sh'), ParallelPermissionError);
});

test('spawnProcess checks the requested executable before spawning', async () => {
  const caps = new CapabilitySet({ process: ['node'] });
  assert.throws(() => spawnProcess('sh', ['-c', 'echo unsafe'], {}, caps), ParallelPermissionError);
  const child = spawnProcess(process.execPath, ['-e', 'process.stdout.write("ok")'], {}, caps);
  let stdout = '';
  child.stdout.setEncoding('utf8'); child.stdout.on('data', (chunk) => stdout += chunk);
  const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('close', resolve); });
  assert.equal(code, 0);
  assert.equal(stdout, 'ok');
});
