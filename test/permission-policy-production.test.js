import test from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import { CapabilitySet, ParallelPermissionError, PermissionPolicy } from '../src/index.js';

test('explicit deny overrides broad filesystem allow for exact files and subtrees', () => {
  const root = path.join(os.tmpdir(), 'parallel-permissions');
  const secretDir = path.join(root, 'secret');
  const exact = path.join(root, 'blocked.txt');
  const capabilities = new CapabilitySet({
    read: [root],
    write: [root],
    deny: { filesystem: { read: [secretDir, exact] } },
  });
  assert.equal(capabilities.assertFile(path.join(root, 'public.txt'), 'read'), path.join(root, 'public.txt'));
  assert.throws(() => capabilities.assertFile(exact, 'read'), ParallelPermissionError);
  assert.throws(() => capabilities.assertFile(path.join(secretDir, 'nested.txt'), 'read'), ParallelPermissionError);
  assert.equal(capabilities.assertFile(path.join(secretDir, 'nested.txt'), 'write'), path.join(secretDir, 'nested.txt'));
});

test('deny rules override wildcard network, import and process grants', () => {
  const capabilities = new CapabilitySet({
    network: ['*'],
    imports: true,
    process: true,
    deny: {
      network: ['metadata.internal:80'],
      import: ['https://evil.example/mod.js'],
      process: ['sh'],
    },
  });
  assert.equal(capabilities.assertHost('example.com', 443), 'example.com:443');
  assert.throws(() => capabilities.assertHost('metadata.internal', 80), ParallelPermissionError);
  assert.equal(capabilities.assertImport('https://good.example/mod.js'), 'https://good.example/mod.js');
  assert.throws(() => capabilities.assertImport('https://evil.example/mod.js'), ParallelPermissionError);
  assert.throws(() => capabilities.assertProcess('sh'), ParallelPermissionError);
});

test('audit mode records violations without blocking trusted migration runs', () => {
  const events = [];
  const capabilities = new CapabilitySet({
    network: ['example.com:443'],
    permissionMode: 'audit',
    audit: { onViolation: (event) => events.push(event) },
  });
  assert.equal(capabilities.assertHost('unlisted.example', 443), 'unlisted.example:443');
  assert.equal(events.length, 1);
  assert.equal(events[0].capability, 'network');
  assert.equal(events[0].reason, 'not-allowed');
  assert.equal(events[0].mode, 'audit');
  const snapshot = capabilities.permissionAudit();
  assert.equal(snapshot.length, 1);
  assert.equal(snapshot[0].sequence, 1);
  capabilities.clearPermissionAudit();
  assert.deepEqual(capabilities.permissionAudit(), []);
});

test('permission queries expose granted versus denied state without changing policy', () => {
  const capabilities = new CapabilitySet({
    environment: ['SAFE'],
    workers: false,
    deny: { environment: ['BLOCKED'] },
  });
  assert.equal(capabilities.query('environment', 'SAFE').state, 'granted');
  const blocked = capabilities.query('environment', 'BLOCKED');
  assert.equal(blocked.state, 'denied');
  assert.equal(blocked.reason, 'explicit-deny');
  assert.equal(capabilities.query('workers').state, 'denied');
});

test('audit logs are bounded and preserve monotonic event sequence', () => {
  const policy = new PermissionPolicy({ mode: 'audit', audit: { maxEntries: 2 } });
  policy.decide({ capability: 'network', resource: 'one:1', allowed: false });
  policy.decide({ capability: 'network', resource: 'two:2', allowed: false });
  policy.decide({ capability: 'network', resource: 'three:3', allowed: false });
  const events = policy.audit.snapshot();
  assert.equal(events.length, 2);
  assert.deepEqual(events.map((event) => event.sequence), [2, 3]);
  assert.deepEqual(events.map((event) => event.resource), ['two:2', 'three:3']);
});
