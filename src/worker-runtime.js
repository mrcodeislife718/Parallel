import path from 'node:path';
import crypto from 'node:crypto';
import { Worker } from 'node:worker_threads';

export class ParallelWorkerPolicyError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ParallelWorkerPolicyError';
  }
}

export function deriveWorkerCapabilityConfig(parentCapabilities, requested = 'inherit') {
  if (!parentCapabilities?.snapshot || !parentCapabilities?.assertWorkers) throw new TypeError('Parallel workers require parent capabilities');
  parentCapabilities.assertWorkers();
  const parent = parentCapabilities.snapshot();
  const inheritedDeny = parent.permissions?.deny ?? {};
  const permissionMode = parent.permissions?.mode ?? 'enforce';

  if (requested === 'none') return Object.freeze({
    read: [], write: [], environment: [], network: [], imports: false, process: false,
    crypto: false, timers: false, workers: false, deny: inheritedDeny, permissionMode,
  });
  if (requested === 'inherit' || requested == null) return Object.freeze({
    read: [...(parent.filesystemRead ?? [])], write: [...(parent.filesystemWrite ?? [])],
    environment: [...(parent.environment ?? [])], network: [...(parent.network ?? [])],
    imports: cloneGrant(parent.imports), process: cloneGrant(parent.process),
    crypto: Boolean(parent.crypto), timers: Boolean(parent.timers), workers: Boolean(parent.workers),
    deny: inheritedDeny, permissionMode,
  });
  if (typeof requested !== 'object' || Array.isArray(requested)) throw new TypeError("worker permissions must be 'inherit', 'none', or an object");

  const child = {
    read: subsetPaths(requested.read ?? requested.filesystem?.read ?? [], parent.filesystemRead ?? [], 'filesystem.read'),
    write: subsetPaths(requested.write ?? requested.filesystem?.write ?? [], parent.filesystemWrite ?? [], 'filesystem.write'),
    environment: subsetExact(requested.environment ?? [], parent.environment ?? [], 'environment'),
    network: subsetNetwork(requested.network ?? [], parent.network ?? []),
    imports: subsetGrant(requested.imports ?? requested.import ?? false, parent.imports, 'import'),
    process: subsetGrant(requested.process ?? false, parent.process, 'process'),
    crypto: attenuateBoolean(requested.crypto ?? false, parent.crypto, 'crypto'),
    timers: attenuateBoolean(requested.timers ?? false, parent.timers, 'timers'),
    workers: attenuateBoolean(requested.workers ?? false, parent.workers, 'workers'),
    deny: mergeDeny(inheritedDeny, requested.deny ?? {}),
    permissionMode,
  };
  return Object.freeze(child);
}

export class CapabilityWorkerPool {
  constructor({ size = 1, workerUrl, workerData = null, capabilities, permissions = 'inherit', taskTimeoutMs = 30_000, maxPending = 1024, resourceLimits = {} } = {}) {
    if (!Number.isInteger(size) || size < 1) throw new TypeError('worker pool size must be a positive integer');
    if (!workerUrl) throw new TypeError('workerUrl is required');
    if (!Number.isInteger(taskTimeoutMs) || taskTimeoutMs < 1) throw new TypeError('taskTimeoutMs must be a positive integer');
    if (!Number.isInteger(maxPending) || maxPending < 1) throw new TypeError('maxPending must be a positive integer');
    this.childPermissions = deriveWorkerCapabilityConfig(capabilities, permissions);
    this.taskTimeoutMs = taskTimeoutMs;
    this.maxPending = maxPending;
    this.pending = new Map();
    this.cursor = 0;
    this.closed = false;
    this.workers = Array.from({ length: size }, (_, index) => {
      const worker = new Worker(workerUrl, {
        workerData: { application: workerData, parallelPermissions: this.childPermissions },
        resourceLimits,
        name: `parallel-worker-${index + 1}`,
      });
      worker.on('message', (message) => this.#onMessage(message));
      worker.on('error', (error) => this.#failWorker(worker, error));
      worker.on('exit', (code) => { if (!this.closed && code !== 0) this.#failWorker(worker, new Error(`Parallel worker exited with code ${code}`)); });
      return worker;
    });
  }

  exec(payload, { signal, timeoutMs = this.taskTimeoutMs } = {}) {
    if (this.closed) return Promise.reject(new Error('Parallel worker pool is closed'));
    if (this.pending.size >= this.maxPending) return Promise.reject(new Error('Parallel worker pool pending-task limit reached'));
    if (!Number.isInteger(timeoutMs) || timeoutMs < 1) return Promise.reject(new TypeError('timeoutMs must be a positive integer'));
    if (signal?.aborted) return Promise.reject(signal.reason ?? new Error('worker task aborted'));
    const id = crypto.randomUUID();
    const worker = this.workers[this.cursor++ % this.workers.length];
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Parallel worker task timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      timer.unref?.();
      const onAbort = () => {
        if (!this.pending.delete(id)) return;
        clearTimeout(timer);
        reject(signal.reason ?? new Error('worker task aborted'));
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      this.pending.set(id, { worker, resolve, reject, timer, signal, onAbort });
      worker.postMessage({ id, payload });
    });
  }

  #onMessage(message) {
    const entry = this.pending.get(message?.id);
    if (!entry) return;
    this.pending.delete(message.id);
    clearTimeout(entry.timer);
    entry.signal?.removeEventListener('abort', entry.onAbort);
    if (message.error) entry.reject(Object.assign(new Error(message.error.message ?? 'worker failure'), message.error));
    else entry.resolve(message.value);
  }

  #failWorker(worker, error) {
    for (const [id, entry] of this.pending) {
      if (entry.worker !== worker) continue;
      this.pending.delete(id);
      clearTimeout(entry.timer);
      entry.signal?.removeEventListener('abort', entry.onAbort);
      entry.reject(error);
    }
  }

  snapshot() { return { size: this.workers.length, pending: this.pending.size, permissions: structuredClone(this.childPermissions), closed: this.closed }; }

  async close() {
    if (this.closed) return;
    this.closed = true;
    for (const [id, entry] of this.pending) {
      this.pending.delete(id);
      clearTimeout(entry.timer);
      entry.signal?.removeEventListener('abort', entry.onAbort);
      entry.reject(new Error('Parallel worker pool closed'));
    }
    await Promise.all(this.workers.map((worker) => worker.terminate()));
  }
}

function subsetPaths(requested, parent, capability) {
  if (!Array.isArray(requested)) throw new TypeError(`${capability} worker permissions must be an array`);
  const roots = parent.map((value) => path.resolve(value));
  return requested.map((value) => {
    const resolved = path.resolve(String(value));
    if (!roots.some((root) => resolved === root || resolved.startsWith(root + path.sep))) throw new ParallelWorkerPolicyError(`worker cannot expand ${capability} authority to ${resolved}`);
    return resolved;
  });
}
function subsetExact(requested, parent, capability) {
  if (!Array.isArray(requested)) throw new TypeError(`${capability} worker permissions must be an array`);
  const allowed = new Set(parent.map(String));
  return requested.map(String).map((value) => { if (!allowed.has(value)) throw new ParallelWorkerPolicyError(`worker cannot expand ${capability} authority to ${value}`); return value; });
}
function subsetNetwork(requested, parent) {
  if (!Array.isArray(requested)) throw new TypeError('network worker permissions must be an array');
  const allowed = new Set(parent.map(String));
  return requested.map(String).map((value) => {
    const host = value.includes(':') ? value.slice(0, value.lastIndexOf(':')) : value;
    if (!allowed.has('*') && !allowed.has(value) && !allowed.has(host)) throw new ParallelWorkerPolicyError(`worker cannot expand network authority to ${value}`);
    return value;
  });
}
function subsetGrant(requested, parent, capability) {
  if (requested === false || requested == null) return false;
  if (requested === true) {
    if (parent !== true) throw new ParallelWorkerPolicyError(`worker cannot expand ${capability} authority to unrestricted`);
    return true;
  }
  if (!Array.isArray(requested)) throw new TypeError(`${capability} worker permission must be boolean or array`);
  if (parent === true) return requested.map(String);
  if (parent === false || parent == null) throw new ParallelWorkerPolicyError(`worker cannot expand ${capability} authority`);
  const allowed = new Set(parent.map(String));
  return requested.map(String).map((value) => { if (!allowed.has('*') && !allowed.has(value)) throw new ParallelWorkerPolicyError(`worker cannot expand ${capability} authority to ${value}`); return value; });
}
function attenuateBoolean(requested, parent, capability) {
  const value = Boolean(requested);
  if (value && !parent) throw new ParallelWorkerPolicyError(`worker cannot expand ${capability} authority`);
  return value;
}
function cloneGrant(value) { return Array.isArray(value) ? [...value] : Boolean(value); }
function mergeDeny(parent = {}, child = {}) {
  const out = structuredClone(parent);
  for (const [key, value] of Object.entries(child)) {
    if (out[key] === true || value === undefined || value === false) continue;
    if (value === true) { out[key] = true; continue; }
    const merged = new Set([...(Array.isArray(out[key]) ? out[key] : []), ...(Array.isArray(value) ? value.map(String) : [])]);
    out[key] = [...merged].sort();
  }
  return out;
}
