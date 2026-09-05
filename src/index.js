import fs from 'node:fs/promises';
import path from 'node:path';

export class ParallelPermissionError extends Error {
  constructor(capability, resource) {
    super(`Parallel permission denied: ${capability}${resource ? ` (${resource})` : ''}`);
    this.name = 'ParallelPermissionError';
    this.capability = capability;
    this.resource = resource;
  }
}

export function createRuntime(options = {}) {
  const permissions = {
    filesystem: Array.isArray(options.filesystem) ? options.filesystem.map((entry) => path.resolve(entry)) : [],
    environment: new Set(options.environment ?? []),
    timers: options.timers !== false,
  };

  function assertFileAllowed(target) {
    const resolved = path.resolve(target);
    const allowed = permissions.filesystem.some((root) => resolved === root || resolved.startsWith(`${root}${path.sep}`));
    if (!allowed) throw new ParallelPermissionError('filesystem', resolved);
    return resolved;
  }

  return Object.freeze({
    async sleep(milliseconds) {
      if (!permissions.timers) throw new ParallelPermissionError('timers');
      if (!Number.isFinite(milliseconds) || milliseconds < 0) throw new TypeError('sleep(milliseconds) requires a non-negative finite number');
      await new Promise((resolve) => setTimeout(resolve, milliseconds));
    },
    async readText(file) { return fs.readFile(assertFileAllowed(file), 'utf8'); },
    async writeText(file, content) { const target = assertFileAllowed(file); await fs.mkdir(path.dirname(target), { recursive: true }); await fs.writeFile(target, String(content), 'utf8'); },
    env(name) { if (!permissions.environment.has(name)) throw new ParallelPermissionError('environment', name); return process.env[name] ?? null; },
    permissions() { return { filesystem: [...permissions.filesystem], environment: [...permissions.environment], timers: permissions.timers }; },
  });
}

export * from './runtime.js';
export { CapabilitySet } from './capabilities.js';
export { PermissionPolicy, PermissionAuditLog } from './permission-policy.js';
export { createParallelFetch, ParallelFetchError } from './web-runtime.js';
export { CapabilityWorkerPool, ParallelWorkerPolicyError, deriveWorkerCapabilityConfig } from './worker-runtime.js';
export { createWebStreams, readableFrom, writableFrom, transformFrom, collectBytes, collectText, createFileReadableStream, createFileWritableStream, ParallelStreamLimitError } from './web-streams.js';
export { ParallelModuleRuntime, ParallelModuleGraphError, createModuleGraphManifest, PARALLEL_MODULE_GRAPH_PROTOCOL } from './module-runtime.js';
export { nativeRuntimeManifest, buildNativeKernel, linkNativeProbe } from './native.js';
