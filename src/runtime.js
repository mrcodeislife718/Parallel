import fs from 'node:fs/promises';
import path from 'node:path';
import net from 'node:net';
import http from 'node:http';
import https from 'node:https';
import { Worker } from 'node:worker_threads';
import { spawn } from 'node:child_process';
import { Readable, Writable, Transform, pipeline as nodePipeline } from 'node:stream';
import { promisify } from 'node:util';
import crypto from 'node:crypto';
import { ParallelPermissionError } from './index.js';

const pipeline = promisify(nodePipeline);

export class CapabilitySet {
  constructor(config = {}) {
    this.filesystem = (config.filesystem ?? []).map((entry) => path.resolve(entry));
    this.environment = new Set(config.environment ?? []);
    this.network = new Set(config.network ?? []);
    this.process = Boolean(config.process);
    this.crypto = config.crypto !== false;
    this.timers = config.timers !== false;
    this.workers = config.workers !== false;
  }

  assertFile(target) {
    const resolved = path.resolve(target);
    if (!this.filesystem.some((root) => resolved === root || resolved.startsWith(root + path.sep))) throw new ParallelPermissionError('filesystem', resolved);
    return resolved;
  }

  assertHost(host, port) {
    const key = `${host}:${port}`;
    if (!this.network.has('*') && !this.network.has(host) && !this.network.has(key)) throw new ParallelPermissionError('network', key);
    return key;
  }

  assertEnv(name) { if (!this.environment.has(name)) throw new ParallelPermissionError('environment', name); }
  assertProcess() { if (!this.process) throw new ParallelPermissionError('process'); }
  assertCrypto() { if (!this.crypto) throw new ParallelPermissionError('crypto'); }
  assertTimers() { if (!this.timers) throw new ParallelPermissionError('timers'); }
  assertWorkers() { if (!this.workers) throw new ParallelPermissionError('workers'); }

  snapshot() {
    return { filesystem: [...this.filesystem], environment: [...this.environment], network: [...this.network], process: this.process, crypto: this.crypto, timers: this.timers, workers: this.workers };
  }
}

export class TaskScheduler {
  constructor() { this.queue = []; this.running = false; this.closed = false; this.nextId = 1; }
  schedule(task, { priority = 0, signal } = {}) {
    if (this.closed) throw new Error('scheduler is closed');
    if (typeof task !== 'function') throw new TypeError('scheduled task must be a function');
    return new Promise((resolve, reject) => {
      const entry = { id: this.nextId++, task, priority, signal, resolve, reject };
      this.queue.push(entry);
      this.queue.sort((a, b) => b.priority - a.priority || a.id - b.id);
      queueMicrotask(() => this.#drain());
    });
  }
  async #drain() {
    if (this.running) return;
    this.running = true;
    while (this.queue.length) {
      const entry = this.queue.shift();
      if (entry.signal?.aborted) { entry.reject(entry.signal.reason ?? new Error('task aborted')); continue; }
      try { entry.resolve(await entry.task()); } catch (error) { entry.reject(error); }
    }
    this.running = false;
  }
  close() { this.closed = true; }
}

export class TaskGroup {
  constructor() { this.controller = new AbortController(); this.tasks = new Set(); }
  run(task) {
    const promise = Promise.resolve().then(() => task(this.controller.signal));
    this.tasks.add(promise);
    promise.finally(() => this.tasks.delete(promise));
    return promise;
  }
  cancel(reason = new Error('task group cancelled')) { this.controller.abort(reason); }
  async join() { return Promise.allSettled([...this.tasks]); }
}

export class WorkerPool {
  constructor({ size = 1, workerUrl, workerData, capabilities } = {}) {
    if (!Number.isInteger(size) || size < 1) throw new TypeError('worker pool size must be positive');
    this.capabilities = capabilities;
    this.capabilities?.assertWorkers();
    this.workers = Array.from({ length: size }, () => new Worker(workerUrl, { workerData }));
    this.cursor = 0;
    this.pending = new Map();
    for (const worker of this.workers) worker.on('message', (message) => this.#onMessage(message));
  }
  exec(payload) {
    const id = crypto.randomUUID();
    const worker = this.workers[this.cursor++ % this.workers.length];
    return new Promise((resolve, reject) => { this.pending.set(id, { resolve, reject }); worker.postMessage({ id, payload }); });
  }
  #onMessage(message) { const pending = this.pending.get(message.id); if (!pending) return; this.pending.delete(message.id); if (message.error) pending.reject(Object.assign(new Error(message.error.message), message.error)); else pending.resolve(message.value); }
  async close() { await Promise.all(this.workers.map((worker) => worker.terminate())); }
}

export function createStreams() {
  return Object.freeze({
    readable(source) { return Readable.from(source); },
    writable({ write, close }) { return new Writable({ write(chunk, encoding, callback) { Promise.resolve(write(chunk, encoding)).then(() => callback(), callback); }, final(callback) { Promise.resolve(close?.()).then(() => callback(), callback); } }); },
    transform(transformer) { return new Transform({ transform(chunk, encoding, callback) { Promise.resolve(transformer(chunk, encoding)).then((value) => callback(null, value), callback); } }); },
    pipeline: (...streams) => pipeline(...streams)
  });
}

export function createHttpServer(handler, { tls = null } = {}) {
  if (typeof handler !== 'function') throw new TypeError('HTTP handler must be a function');
  const listener = async (req, res) => {
    try {
      const chunks = []; for await (const chunk of req) chunks.push(chunk);
      const request = { method: req.method, url: `http://${req.headers.host ?? 'localhost'}${req.url}`, headers: req.headers, body: Buffer.concat(chunks) };
      const response = await handler(request);
      res.statusCode = response?.status ?? 200;
      for (const [name, value] of Object.entries(response?.headers ?? {})) res.setHeader(name, value);
      const body = response?.body;
      if (body?.type === 'stream' && body.iterable) { for await (const chunk of body.iterable) res.write(chunk); res.end(); return; }
      if (Buffer.isBuffer(body) || typeof body === 'string') { res.end(body); return; }
      if (body == null) { res.end(); return; }
      if (!res.hasHeader('content-type')) res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(body));
    } catch (error) { res.statusCode = 500; res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ error: 'internal_error', message: error.message })); }
  };
  return tls ? https.createServer(tls, listener) : http.createServer(listener);
}

export function connectTcp(host, port, capabilities) {
  capabilities?.assertHost(host, port);
  return new Promise((resolve, reject) => { const socket = net.createConnection({ host, port }, () => resolve(socket)); socket.once('error', reject); });
}

export function spawnProcess(command, args = [], options = {}, capabilities) {
  capabilities?.assertProcess();
  return spawn(command, args, { stdio: 'pipe', ...options });
}

export function createCrypto(capabilities) {
  return Object.freeze({
    randomBytes(size) { capabilities?.assertCrypto(); return crypto.randomBytes(size); },
    digest(algorithm, data) { capabilities?.assertCrypto(); return crypto.createHash(algorithm).update(data).digest(); },
    hmac(algorithm, key, data) { capabilities?.assertCrypto(); return crypto.createHmac(algorithm, key).update(data).digest(); },
    timingSafeEqual(a, b) { capabilities?.assertCrypto(); return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b)); }
  });
}

export function createRuntimeModuleAbi(version = 1) {
  const modules = new Map();
  return {
    version,
    register(name, factory) { if (modules.has(name)) throw new Error(`runtime module already registered: ${name}`); if (typeof factory !== 'function') throw new TypeError('module factory must be a function'); modules.set(name, factory); },
    load(name, context = {}) { const factory = modules.get(name); if (!factory) throw new Error(`unknown runtime module: ${name}`); return factory({ abiVersion: version, ...context }); },
    list() { return [...modules.keys()].sort(); }
  };
}

export class RuntimeProfiler {
  constructor() { this.samples = []; }
  async measure(name, work) { const start = process.hrtime.bigint(); const before = process.memoryUsage(); try { return await work(); } finally { const end = process.hrtime.bigint(); const after = process.memoryUsage(); this.samples.push({ name, durationNs: Number(end - start), heapDelta: after.heapUsed - before.heapUsed, rssDelta: after.rss - before.rss }); } }
  report() { return this.samples.map(structuredClone); }
}

export async function readFile(file, capabilities) { return fs.readFile(capabilities.assertFile(file)); }
export async function writeFile(file, data, capabilities) { const target = capabilities.assertFile(file); await fs.mkdir(path.dirname(target), { recursive: true }); await fs.writeFile(target, data); }
