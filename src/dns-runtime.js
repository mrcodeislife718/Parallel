import { Resolver, lookup as systemLookup } from 'node:dns/promises';
import { isIP } from 'node:net';
import { domainToASCII } from 'node:url';
import { ParallelPermissionError } from './index.js';

const VALID_ORDERS = new Set(['verbatim', 'ipv4first', 'ipv6first']);
const VALID_BACKENDS = new Set(['system', 'dns']);

export class ParallelDnsError extends Error {
  constructor(message, { code = 'PARALLEL_DNS_ERROR', hostname = null, cause = null } = {}) {
    super(message, cause ? { cause } : undefined);
    this.name = 'ParallelDnsError';
    this.code = code;
    this.hostname = hostname;
  }
}

export function createDnsResolver({ capabilities, resolverFactory = () => new Resolver(), lookupImpl = systemLookup } = {}) {
  if (typeof resolverFactory !== 'function') throw new TypeError('resolverFactory must be a function');
  if (typeof lookupImpl !== 'function') throw new TypeError('lookupImpl must be a function');

  const api = {
    async lookup(hostname, options = {}) {
      const host = authorizeDns(capabilities, hostname);
      const family = normalizeFamily(options.family ?? 0);
      const all = Boolean(options.all);
      const order = options.order ?? 'verbatim';
      const backend = options.backend ?? 'system';
      if (!VALID_ORDERS.has(order)) throw new TypeError(`unsupported DNS result order: ${order}`);
      if (!VALID_BACKENDS.has(backend)) throw new TypeError(`unsupported DNS backend: ${backend}`);
      throwIfAborted(options.signal);

      try {
        if (backend === 'dns') {
          const addresses = await resolveAddresses(host, { family, order, signal: options.signal, servers: options.servers, resolverFactory, capabilities });
          const value = all ? addresses : addresses[0];
          if (!value) throw new ParallelDnsError(`DNS lookup returned no addresses for ${host}`, { code: 'PARALLEL_DNS_NODATA', hostname: host });
          return value;
        }
        return await raceAbort(Promise.resolve(lookupImpl(host, { family, all, order })), options.signal);
      } catch (error) {
        throw normalizeDnsError(error, host, options.signal);
      }
    },

    async resolve(hostname, rrtype = 'A', options = {}) {
      const host = authorizeDns(capabilities, hostname);
      const type = String(rrtype).toUpperCase();
      throwIfAborted(options.signal);
      const resolver = resolverFactory();
      if (!resolver || typeof resolver.resolve !== 'function') throw new TypeError('resolverFactory must return a DNS Resolver');
      configureServers(resolver, options.servers, capabilities);
      try {
        return await cancellableResolverQuery(resolver, () => resolver.resolve(host, type), options.signal, host);
      } catch (error) {
        throw normalizeDnsError(error, host, options.signal);
      }
    },

    async resolve4(hostname, options = {}) {
      const host = authorizeDns(capabilities, hostname);
      const resolver = resolverFactory();
      if (!resolver || typeof resolver.resolve4 !== 'function') throw new TypeError('resolverFactory must return a DNS Resolver');
      configureServers(resolver, options.servers, capabilities);
      throwIfAborted(options.signal);
      try { return await cancellableResolverQuery(resolver, () => resolver.resolve4(host, options.ttl ? { ttl: true } : undefined), options.signal, host); }
      catch (error) { throw normalizeDnsError(error, host, options.signal); }
    },

    async resolve6(hostname, options = {}) {
      const host = authorizeDns(capabilities, hostname);
      const resolver = resolverFactory();
      if (!resolver || typeof resolver.resolve6 !== 'function') throw new TypeError('resolverFactory must return a DNS Resolver');
      configureServers(resolver, options.servers, capabilities);
      throwIfAborted(options.signal);
      try { return await cancellableResolverQuery(resolver, () => resolver.resolve6(host, options.ttl ? { ttl: true } : undefined), options.signal, host); }
      catch (error) { throw normalizeDnsError(error, host, options.signal); }
    },

    async reverse(address, options = {}) {
      if (typeof address !== 'string' || !isIP(address.trim())) throw new TypeError('reverse(address) requires an IPv4 or IPv6 address');
      const host = authorizeDns(capabilities, address.trim());
      const resolver = resolverFactory();
      if (!resolver || typeof resolver.reverse !== 'function') throw new TypeError('resolverFactory must return a DNS Resolver');
      configureServers(resolver, options.servers, capabilities);
      throwIfAborted(options.signal);
      try { return await cancellableResolverQuery(resolver, () => resolver.reverse(host), options.signal, host); }
      catch (error) { throw normalizeDnsError(error, host, options.signal); }
    },
  };

  return Object.freeze(api);
}

async function resolveAddresses(hostname, { family, order, signal, servers, resolverFactory, capabilities }) {
  const resolver = resolverFactory();
  if (!resolver || typeof resolver.resolve4 !== 'function' || typeof resolver.resolve6 !== 'function') throw new TypeError('resolverFactory must return a DNS Resolver');
  configureServers(resolver, servers, capabilities);
  const query = async () => {
    if (family === 4) return (await resolver.resolve4(hostname)).map((address) => ({ address, family: 4 }));
    if (family === 6) return (await resolver.resolve6(hostname)).map((address) => ({ address, family: 6 }));
    const [v4, v6] = await Promise.allSettled([resolver.resolve4(hostname), resolver.resolve6(hostname)]);
    const addresses = [];
    if (v4.status === 'fulfilled') addresses.push(...v4.value.map((address) => ({ address, family: 4 })));
    if (v6.status === 'fulfilled') addresses.push(...v6.value.map((address) => ({ address, family: 6 })));
    if (!addresses.length) throw (v4.status === 'rejected' ? v4.reason : v6.reason);
    if (order === 'ipv4first') addresses.sort((a, b) => a.family - b.family);
    else if (order === 'ipv6first') addresses.sort((a, b) => b.family - a.family);
    return addresses;
  };
  return cancellableResolverQuery(resolver, query, signal, hostname);
}

function configureServers(resolver, servers, capabilities) {
  if (servers == null) return;
  if (!Array.isArray(servers)) throw new TypeError('DNS servers must be an array');
  const normalized = servers.map((server) => String(server));
  for (const server of normalized) {
    const host = extractServerHost(server);
    if (!capabilities) throw new ParallelPermissionError('network.dns', host);
    if (typeof capabilities.assertDnsHost === 'function') capabilities.assertDnsHost(host);
    else capabilities.assertHost?.(host, 53);
  }
  resolver.setServers(normalized);
}

function authorizeDns(capabilities, hostname) {
  const host = normalizeHostname(hostname);
  if (!capabilities) throw new ParallelPermissionError('network.dns', host);
  if (typeof capabilities.assertDnsHost === 'function') capabilities.assertDnsHost(host);
  else if (typeof capabilities.assertHost === 'function') capabilities.assertHost(host, 53);
  else throw new ParallelPermissionError('network.dns', host);
  return host;
}

function normalizeHostname(value) {
  if (typeof value !== 'string' || !value.trim()) throw new TypeError('DNS hostname must be a non-empty string');
  const input = value.trim();
  if (isIP(input)) return input;
  const ascii = domainToASCII(input);
  if (!ascii || ascii.length > 253) throw new TypeError('DNS hostname is invalid');
  const labels = ascii.split('.');
  if (labels.some((label) => !label || label.length > 63 || !/^[A-Za-z0-9-]+$/.test(label) || label.startsWith('-') || label.endsWith('-'))) throw new TypeError('DNS hostname is invalid');
  return ascii.toLowerCase();
}

function normalizeFamily(value) {
  if (value === 0 || value === 'any') return 0;
  if (value === 4 || value === 'IPv4') return 4;
  if (value === 6 || value === 'IPv6') return 6;
  throw new TypeError('DNS family must be 0, 4, 6, IPv4, IPv6, or any');
}

function extractServerHost(server) {
  if (server.startsWith('[')) {
    const end = server.indexOf(']');
    if (end < 0) throw new TypeError(`invalid DNS server address: ${server}`);
    return server.slice(1, end);
  }
  const first = server.indexOf(':');
  const last = server.lastIndexOf(':');
  return first > 0 && first === last ? server.slice(0, first) : server;
}

async function cancellableResolverQuery(resolver, work, signal, hostname) {
  if (!signal) return work();
  throwIfAborted(signal);
  const onAbort = () => { try { resolver.cancel?.(); } catch {} };
  signal.addEventListener('abort', onAbort, { once: true });
  try { return await raceAbort(Promise.resolve().then(work), signal); }
  catch (error) { if (signal.aborted) throw abortError(signal, hostname); throw error; }
  finally { signal.removeEventListener('abort', onAbort); }
}

function raceAbort(promise, signal) {
  if (!signal) return promise;
  throwIfAborted(signal);
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(abortError(signal));
    signal.addEventListener('abort', onAbort, { once: true });
    promise.then(resolve, reject).finally(() => signal.removeEventListener('abort', onAbort));
  });
}

function throwIfAborted(signal) {
  if (signal?.aborted) throw abortError(signal);
}

function abortError(signal, hostname = null) {
  const error = new ParallelDnsError('DNS operation aborted', { code: 'PARALLEL_DNS_ABORTED', hostname, cause: signal?.reason instanceof Error ? signal.reason : null });
  error.name = 'AbortError';
  return error;
}

function normalizeDnsError(error, hostname, signal) {
  if (signal?.aborted || error?.name === 'AbortError') return abortError(signal, hostname);
  if (error instanceof ParallelDnsError || error instanceof ParallelPermissionError || error instanceof TypeError) return error;
  return new ParallelDnsError(`DNS operation failed for ${hostname}: ${error?.message ?? String(error)}`, { code: error?.code ?? 'PARALLEL_DNS_ERROR', hostname, cause: error });
}
