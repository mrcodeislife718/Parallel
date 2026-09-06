import test from 'node:test';
import assert from 'node:assert/strict';
import { CapabilitySet, createDnsResolver, ParallelPermissionError } from '../src/index.js';

function fakeResolver(records = {}) {
  let cancelled = false;
  let servers = [];
  return {
    get cancelled() { return cancelled; },
    get servers() { return [...servers]; },
    setServers(next) { servers = [...next]; },
    cancel() { cancelled = true; },
    async resolve(host, type) { return records[`${host}:${type}`] ?? []; },
    async resolve4(host) { return records[`${host}:A`] ?? []; },
    async resolve6(host) { return records[`${host}:AAAA`] ?? []; },
    async reverse(address) { return records[`reverse:${address}`] ?? []; },
  };
}

test('Parallel DNS denies hostname resolution without network authority', async () => {
  const capabilities = new CapabilitySet({ network: [] });
  const dns = createDnsResolver({ capabilities, lookupImpl: async () => ({ address: '203.0.113.1', family: 4 }) });
  await assert.rejects(() => dns.lookup('example.com'), (error) => {
    assert.ok(error instanceof ParallelPermissionError);
    assert.equal(error.capability, 'network.dns');
    assert.equal(error.resource, 'example.com');
    return true;
  });
});

test('Parallel DNS system lookup preserves familiar lookup shape and normalized hostnames', async () => {
  const capabilities = new CapabilitySet({ network: ['example.com:443'] });
  let observed;
  const dns = createDnsResolver({
    capabilities,
    lookupImpl: async (hostname, options) => {
      observed = { hostname, options };
      return { address: '203.0.113.8', family: 4 };
    },
  });
  const result = await dns.lookup('EXAMPLE.com', { family: 'IPv4', order: 'ipv4first' });
  assert.deepEqual(result, { address: '203.0.113.8', family: 4 });
  assert.deepEqual(observed, { hostname: 'example.com', options: { family: 4, all: false, order: 'ipv4first' } });
});

test('Parallel DNS direct backend returns deterministic IPv4-first address records', async () => {
  const capabilities = new CapabilitySet({ network: ['example.com'] });
  const resolver = fakeResolver({
    'example.com:A': ['203.0.113.10', '203.0.113.11'],
    'example.com:AAAA': ['2001:db8::10'],
  });
  const dns = createDnsResolver({ capabilities, resolverFactory: () => resolver });
  const result = await dns.lookup('example.com', { backend: 'dns', all: true, order: 'ipv4first' });
  assert.deepEqual(result, [
    { address: '203.0.113.10', family: 4 },
    { address: '203.0.113.11', family: 4 },
    { address: '2001:db8::10', family: 6 },
  ]);
});

test('Parallel DNS custom nameservers require their own network authority', async () => {
  const capabilities = new CapabilitySet({ network: ['example.com'] });
  const resolver = fakeResolver({ 'example.com:A': ['203.0.113.10'] });
  const dns = createDnsResolver({ capabilities, resolverFactory: () => resolver });
  await assert.rejects(() => dns.resolve4('example.com', { servers: ['1.1.1.1'] }), ParallelPermissionError);

  const allowed = new CapabilitySet({ network: ['example.com', '1.1.1.1:53'] });
  const resolver2 = fakeResolver({ 'example.com:A': ['203.0.113.10'] });
  const dns2 = createDnsResolver({ capabilities: allowed, resolverFactory: () => resolver2 });
  assert.deepEqual(await dns2.resolve4('example.com', { servers: ['1.1.1.1'] }), ['203.0.113.10']);
  assert.deepEqual(resolver2.servers, ['1.1.1.1']);
});

test('Parallel DNS aborts outstanding resolver work and cancels the resolver', async () => {
  const capabilities = new CapabilitySet({ network: ['example.com'] });
  let release;
  const resolver = fakeResolver();
  resolver.resolve = () => new Promise((resolve) => { release = resolve; });
  const dns = createDnsResolver({ capabilities, resolverFactory: () => resolver });
  const controller = new AbortController();
  const pending = dns.resolve('example.com', 'TXT', { signal: controller.signal });
  controller.abort(new Error('stop'));
  await assert.rejects(() => pending, (error) => {
    assert.equal(error.name, 'AbortError');
    assert.equal(error.code, 'PARALLEL_DNS_ABORTED');
    return true;
  });
  assert.equal(resolver.cancelled, true);
  release?.([]);
});

test('Parallel DNS reverse lookup validates IP input', async () => {
  const capabilities = new CapabilitySet({ network: ['127.0.0.1'] });
  const resolver = fakeResolver({ 'reverse:127.0.0.1': ['localhost'] });
  const dns = createDnsResolver({ capabilities, resolverFactory: () => resolver });
  assert.deepEqual(await dns.reverse('127.0.0.1'), ['localhost']);
  await assert.rejects(() => dns.reverse('not-an-ip'), TypeError);
});
