import { spawn } from 'node:child_process';

export class ParallelProcessError extends Error {
  constructor(message, code = 'PARALLEL_PROCESS_ERROR', details = {}) {
    super(message);
    this.name = 'ParallelProcessError';
    this.code = code;
    Object.assign(this, details);
  }
}

export async function runProcess(command, args = [], {
  capabilities,
  cwd = undefined,
  env = {},
  inheritEnv = [],
  stdin = undefined,
  timeoutMs = 30_000,
  maxOutputBytes = 8 * 1024 * 1024,
  signal = undefined,
  shell = false,
} = {}) {
  if (!capabilities?.assertProcess) throw new TypeError('Parallel process execution requires runtime capabilities');
  if (typeof command !== 'string' || !command) throw new TypeError('command must be a non-empty string');
  if (!Array.isArray(args)) throw new TypeError('args must be an array');
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) throw new TypeError('timeoutMs must be a positive integer');
  if (!Number.isInteger(maxOutputBytes) || maxOutputBytes < 1) throw new TypeError('maxOutputBytes must be a positive integer');
  if (!Array.isArray(inheritEnv)) throw new TypeError('inheritEnv must be an array');
  capabilities.assertProcess(command);

  const childEnv = {};
  for (const name of inheritEnv) {
    capabilities.assertEnv(name);
    if (process.env[name] !== undefined) childEnv[name] = process.env[name];
  }
  for (const [name, value] of Object.entries(env ?? {})) {
    capabilities.assertEnv(name);
    if (value != null) childEnv[name] = String(value);
  }

  if (signal?.aborted) throw signal.reason ?? new ParallelProcessError('Parallel process aborted before start', 'PARALLEL_PROCESS_ABORTED');

  return new Promise((resolve, reject) => {
    const child = spawn(command, args.map(String), {
      cwd,
      env: childEnv,
      shell,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    const stdout = [];
    const stderr = [];
    let stdoutBytes = 0;
    let stderrBytes = 0;
    let settled = false;

    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener('abort', onAbort);
    };
    const fail = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (!child.killed) child.kill('SIGKILL');
      reject(error);
    };
    const onAbort = () => fail(signal.reason ?? new ParallelProcessError('Parallel process aborted', 'PARALLEL_PROCESS_ABORTED'));
    signal?.addEventListener('abort', onAbort, { once: true });

    const timer = setTimeout(() => fail(new ParallelProcessError(`Parallel process timed out after ${timeoutMs}ms`, 'PARALLEL_PROCESS_TIMEOUT')), timeoutMs);
    timer.unref?.();

    child.once('error', (error) => fail(new ParallelProcessError(error.message, 'PARALLEL_PROCESS_SPAWN', { cause: error })));
    child.stdout.on('data', (chunk) => {
      stdoutBytes += chunk.length;
      if (stdoutBytes > maxOutputBytes) return fail(new ParallelProcessError(`Parallel process stdout exceeds ${maxOutputBytes} bytes`, 'PARALLEL_PROCESS_OUTPUT_LIMIT'));
      stdout.push(chunk);
    });
    child.stderr.on('data', (chunk) => {
      stderrBytes += chunk.length;
      if (stderrBytes > maxOutputBytes) return fail(new ParallelProcessError(`Parallel process stderr exceeds ${maxOutputBytes} bytes`, 'PARALLEL_PROCESS_OUTPUT_LIMIT'));
      stderr.push(chunk);
    });
    child.once('close', (code, closeSignal) => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve(Object.freeze({
        code,
        signal: closeSignal,
        success: code === 0 && closeSignal == null,
        stdout: Buffer.concat(stdout),
        stderr: Buffer.concat(stderr),
      }));
    });

    if (stdin == null) child.stdin.end();
    else {
      child.stdin.on('error', (error) => { if (error.code !== 'EPIPE') fail(error); });
      child.stdin.end(stdin);
    }
  });
}
