import path from 'node:path';
import { CapabilitySet as BaseCapabilitySet } from './runtime.js';
import { PermissionPolicy } from './permission-policy.js';
import { ParallelPermissionError } from './index.js';

export class CapabilitySet extends BaseCapabilitySet {
  constructor(config = {}) {
    super(config);
    this.permissionPolicy = new PermissionPolicy({
      mode: config.permissionMode ?? config.mode ?? 'enforce',
      deny: normalizeDenies(config.deny),
      audit: config.audit ?? {},
    });
  }

  assertFile(target, access = 'read') {
    const resource = path.resolve(target);
    let allowed = true;
    try { super.assertFile(resource, access); } catch (error) { if (!(error instanceof ParallelPermissionError)) throw error; allowed = false; }
    this.#enforce(`filesystem.${access}`, resource, allowed);
    return resource;
  }

  assertHost(host, port) {
    let resource;
    let allowed = true;
    try { resource = super.assertHost(host, port); } catch (error) { if (!(error instanceof ParallelPermissionError)) throw error; resource = `${host}:${port}`; allowed = false; }
    this.#enforce('network', resource, allowed);
    return resource;
  }

  assertImport(specifier) {
    let allowed = true;
    try { super.assertImport(specifier); } catch (error) { if (!(error instanceof ParallelPermissionError)) throw error; allowed = false; }
    this.#enforce('import', specifier, allowed);
    return specifier;
  }

  assertEnv(name) {
    let allowed = true;
    try { super.assertEnv(name); } catch (error) { if (!(error instanceof ParallelPermissionError)) throw error; allowed = false; }
    this.#enforce('environment', name, allowed);
    return name;
  }

  assertProcess(command = null) {
    let allowed = true;
    try { super.assertProcess(command); } catch (error) { if (!(error instanceof ParallelPermissionError)) throw error; allowed = false; }
    this.#enforce('process', command ?? null, allowed);
    return true;
  }

  assertCrypto() { return this.#booleanCapability('crypto', () => super.assertCrypto()); }
  assertTimers() { return this.#booleanCapability('timers', () => super.assertTimers()); }
  assertWorkers() { return this.#booleanCapability('workers', () => super.assertWorkers()); }

  query(capability, resource = null) {
    const allowed = this.#baseAllowed(capability, resource);
    return this.permissionPolicy.decide({ capability, resource, allowed });
  }

  permissionAudit() { return this.permissionPolicy.audit.snapshot(); }
  clearPermissionAudit() { this.permissionPolicy.audit.clear(); }

  snapshot() {
    return { ...super.snapshot(), permissions: this.permissionPolicy.snapshot() };
  }

  #booleanCapability(capability, assertion) {
    let allowed = true;
    try { assertion(); } catch (error) { if (!(error instanceof ParallelPermissionError)) throw error; allowed = false; }
    this.#enforce(capability, null, allowed);
    return true;
  }

  #enforce(capability, resource, allowed) {
    const decision = this.permissionPolicy.decide({ capability, resource, allowed });
    if (decision.state === 'denied') throw new ParallelPermissionError(capability, resource ?? undefined);
    return decision;
  }

  #baseAllowed(capability, resource) {
    try {
      if (capability === 'filesystem.read' || capability === 'filesystem.write') super.assertFile(resource, capability.endsWith('write') ? 'write' : 'read');
      else if (capability === 'network') {
        const [host, portText] = splitHostPort(resource);
        super.assertHost(host, Number(portText));
      } else if (capability === 'import') super.assertImport(resource);
      else if (capability === 'environment') super.assertEnv(resource);
      else if (capability === 'process') super.assertProcess(resource);
      else if (capability === 'crypto') super.assertCrypto();
      else if (capability === 'timers') super.assertTimers();
      else if (capability === 'workers') super.assertWorkers();
      else throw new TypeError(`unknown Parallel capability: ${capability}`);
      return true;
    } catch (error) {
      if (error instanceof ParallelPermissionError) return false;
      throw error;
    }
  }
}

function normalizeDenies(deny = {}) {
  const out = { ...deny };
  if (deny.filesystem && typeof deny.filesystem === 'object' && !Array.isArray(deny.filesystem)) {
    if (deny.filesystem.read !== undefined) out['filesystem.read'] = normalizePathRules(deny.filesystem.read);
    if (deny.filesystem.write !== undefined) out['filesystem.write'] = normalizePathRules(deny.filesystem.write);
    delete out.filesystem;
  }
  if (out['filesystem.read'] !== undefined) out['filesystem.read'] = normalizePathRules(out['filesystem.read']);
  if (out['filesystem.write'] !== undefined) out['filesystem.write'] = normalizePathRules(out['filesystem.write']);
  return out;
}

function normalizePathRules(value) {
  if (value === true || !value) return value;
  if (!Array.isArray(value)) throw new TypeError('filesystem deny rules must be boolean or arrays');
  return value.flatMap((entry) => {
    const resolved = path.resolve(String(entry));
    return [resolved, `${resolved}${path.sep}`];
  });
}

function splitHostPort(value) {
  if (typeof value !== 'string') throw new TypeError('network resource must be host:port');
  const index = value.lastIndexOf(':');
  if (index < 1) throw new TypeError('network resource must be host:port');
  return [value.slice(0, index), value.slice(index + 1)];
}
