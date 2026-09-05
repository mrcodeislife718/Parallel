import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const PARALLEL_MODULE_GRAPH_PROTOCOL = 'parallel-module-graph/1';

export class ParallelModuleGraphError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ParallelModuleGraphError';
  }
}

export class ParallelModuleRuntime {
  constructor({ projectRoot = process.cwd(), capabilities = null, maxModules = 4096, maxModuleBytes = 8 * 1024 * 1024, executionTimeoutMs = 30_000 } = {}) {
    this.projectRoot = path.resolve(projectRoot);
    this.capabilities = capabilities;
    this.maxModules = positiveInteger(maxModules, 'maxModules');
    this.maxModuleBytes = positiveInteger(maxModuleBytes, 'maxModuleBytes');
    this.executionTimeoutMs = positiveInteger(executionTimeoutMs, 'executionTimeoutMs');
    this.cache = new Map();
  }

  async prepare(manifest) {
    validateManifest(manifest, this.maxModules);
    const projectReal = await fs.realpath(this.projectRoot).catch(() => this.projectRoot);
    const byId = new Map(manifest.modules.map((record) => [record.id, record]));
    const closure = reachableModules(manifest.entry, byId);
    const modules = [];
    for (const id of closure) {
      const record = byId.get(id);
      const cached = this.cache.get(record.digest);
      let code;
      if (cached) {
        code = cached.code;
      } else {
        const file = await resolveModuleFile(projectReal, record.file);
        const stat = await fs.stat(file);
        if (!stat.isFile()) throw new ParallelModuleGraphError(`Parallel module is not a file: ${record.file}`);
        if (stat.size > this.maxModuleBytes) throw new ParallelModuleGraphError(`Parallel module exceeds size limit: ${record.id}`);
        code = await fs.readFile(file, 'utf8');
        const digest = sha256(code);
        if (digest !== record.digest) throw new ParallelModuleGraphError(`Parallel module digest mismatch: ${record.id}`);
        this.cache.set(record.digest, { code, bytes: stat.size });
      }
      modules.push({
        id: record.id,
        code,
        dependencies: { ...(record.dependencies ?? {}) },
        dynamicDependencies: this.#authorizedDynamicDependencies(record),
      });
    }
    return Object.freeze({ protocol: manifest.protocol, entry: manifest.entry, modules, graphDigest: graphDigest(manifest) });
  }

  async execute(manifest, { invoke = null } = {}) {
    const prepared = await this.prepare(manifest);
    const runner = fileURLToPath(new URL('./module-runner.js', import.meta.url));
    const payload = JSON.stringify({ ...prepared, invoke });
    const child = spawn(process.execPath, ['--experimental-vm-modules', runner], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: minimalExecutionEnvironment(process.env),
    });
    const stdout = [];
    const stderr = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    const maxOutput = 1024 * 1024;
    child.stdout.on('data', (chunk) => {
      stdoutBytes += chunk.length;
      if (stdoutBytes > maxOutput) child.kill('SIGKILL');
      else stdout.push(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length;
      if (stderrBytes > maxOutput) child.kill('SIGKILL');
      else stderr.push(chunk);
    });
    child.stdin.end(payload);
    const result = await waitForChild(child, this.executionTimeoutMs);
    if (result.timedOut) throw new ParallelModuleGraphError(`Parallel module execution timed out after ${this.executionTimeoutMs}ms`);
    const text = Buffer.concat(stdout).toString('utf8').trim();
    let response;
    try { response = JSON.parse(text); }
    catch {
      const detail = Buffer.concat(stderr).toString('utf8').trim();
      throw new ParallelModuleGraphError(`Parallel module runner returned invalid response${detail ? `: ${detail}` : ''}`);
    }
    if (!response.ok) throw new ParallelModuleGraphError(response.error?.message ?? 'Parallel module execution failed');
    return Object.freeze({ ...response, graphDigest: prepared.graphDigest });
  }

  #authorizedDynamicDependencies(record) {
    const allowed = {};
    for (const [specifier, target] of Object.entries(record.dynamicDependencies ?? {})) {
      try {
        if (!this.capabilities?.assertImport) continue;
        this.capabilities.assertImport(specifier);
        allowed[specifier] = target;
      } catch {
        // Keep denied dynamic imports out of the runtime map. If code actually
        // executes one, the runner rejects it at that point instead of blocking
        // startup for an unused optional path.
      }
    }
    return allowed;
  }
}

export function createModuleGraphManifest({ entry, modules }) {
  const manifest = { protocol: PARALLEL_MODULE_GRAPH_PROTOCOL, entry, modules: modules.map((entry) => ({ ...entry })) };
  validateManifest(manifest, 4096);
  return Object.freeze({ ...manifest, digest: graphDigest(manifest) });
}

function validateManifest(manifest, maxModules) {
  if (!manifest || manifest.protocol !== PARALLEL_MODULE_GRAPH_PROTOCOL) throw new ParallelModuleGraphError(`Parallel requires ${PARALLEL_MODULE_GRAPH_PROTOCOL}`);
  if (typeof manifest.entry !== 'string' || !manifest.entry) throw new ParallelModuleGraphError('Parallel module graph entry is required');
  if (!Array.isArray(manifest.modules) || manifest.modules.length === 0) throw new ParallelModuleGraphError('Parallel module graph requires modules');
  if (manifest.modules.length > maxModules) throw new ParallelModuleGraphError(`Parallel module graph exceeds ${maxModules} modules`);
  const ids = new Set();
  for (const record of manifest.modules) {
    if (!record || typeof record.id !== 'string' || !record.id) throw new ParallelModuleGraphError('Parallel module id is required');
    if (ids.has(record.id)) throw new ParallelModuleGraphError(`Duplicate Parallel module id: ${record.id}`);
    ids.add(record.id);
    if (typeof record.file !== 'string' || !record.file) throw new ParallelModuleGraphError(`Parallel module '${record.id}' requires a file`);
    if (!/^[a-f0-9]{64}$/i.test(record.digest ?? '')) throw new ParallelModuleGraphError(`Parallel module '${record.id}' requires a SHA-256 digest`);
    validateDependencyMap(record.dependencies, record.id, 'static');
    validateDependencyMap(record.dynamicDependencies, record.id, 'dynamic');
  }
  if (!ids.has(manifest.entry)) throw new ParallelModuleGraphError(`Parallel entry module '${manifest.entry}' is missing`);
  for (const record of manifest.modules) {
    for (const target of Object.values(record.dependencies ?? {})) if (!ids.has(target)) throw new ParallelModuleGraphError(`Parallel module '${record.id}' references missing dependency '${target}'`);
    for (const target of Object.values(record.dynamicDependencies ?? {})) if (!ids.has(target)) throw new ParallelModuleGraphError(`Parallel module '${record.id}' references missing dynamic dependency '${target}'`);
  }
}

function validateDependencyMap(map, id, kind) {
  if (map == null) return;
  if (typeof map !== 'object' || Array.isArray(map)) throw new ParallelModuleGraphError(`Parallel module '${id}' ${kind} dependencies must be an object`);
  for (const [specifier, target] of Object.entries(map)) {
    if (!specifier || typeof target !== 'string' || !target) throw new ParallelModuleGraphError(`Parallel module '${id}' has invalid ${kind} dependency`);
  }
}

function reachableModules(entry, byId) {
  const seen = new Set();
  const order = [];
  function visit(id) {
    if (seen.has(id)) return;
    seen.add(id);
    const record = byId.get(id);
    if (!record) throw new ParallelModuleGraphError(`Parallel graph references unknown module '${id}'`);
    for (const target of Object.values(record.dependencies ?? {})) visit(target);
    for (const target of Object.values(record.dynamicDependencies ?? {})) visit(target);
    order.push(id);
  }
  visit(entry);
  return order;
}

async function resolveModuleFile(projectRoot, relative) {
  if (path.isAbsolute(relative)) throw new ParallelModuleGraphError(`Parallel module path must be project-relative: ${relative}`);
  const lexical = path.resolve(projectRoot, relative);
  if (!inside(projectRoot, lexical)) throw new ParallelModuleGraphError(`Parallel module path escapes project root: ${relative}`);
  const physical = await fs.realpath(lexical);
  if (!inside(projectRoot, physical)) throw new ParallelModuleGraphError(`Parallel module symlink escapes project root: ${relative}`);
  return physical;
}

function inside(root, target) { return target === root || target.startsWith(`${root}${path.sep}`); }
function sha256(value) { return crypto.createHash('sha256').update(value).digest('hex'); }
function graphDigest(manifest) { return sha256(JSON.stringify(canonicalize({ protocol: manifest.protocol, entry: manifest.entry, modules: manifest.modules }))); }
function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().filter((key) => key !== 'digest' && value[key] !== undefined).map((key) => [key, canonicalize(value[key])]));
  return value;
}
function minimalExecutionEnvironment(env) {
  const keep = ['PATH', 'SystemRoot', 'WINDIR', 'TMPDIR', 'TMP', 'TEMP', 'HOME', 'USERPROFILE'];
  return Object.fromEntries(keep.filter((key) => env[key] != null).map((key) => [key, env[key]]));
}
function positiveInteger(value, name) { if (!Number.isInteger(value) || value <= 0) throw new TypeError(`${name} must be a positive integer`); return value; }
function waitForChild(child, timeoutMs) {
  return new Promise((resolve, reject) => {
    let timedOut = false;
    const timer = setTimeout(() => { timedOut = true; child.kill('SIGKILL'); }, timeoutMs);
    timer.unref?.();
    child.once('error', (error) => { clearTimeout(timer); reject(error); });
    child.once('close', (code, signal) => { clearTimeout(timer); resolve({ code, signal, timedOut }); });
  });
}
