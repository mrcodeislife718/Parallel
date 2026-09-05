import fs from 'node:fs/promises';
import { Readable, Writable, Duplex } from 'node:stream';

export class ParallelStreamLimitError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ParallelStreamLimitError';
    this.code = 'PARALLEL_STREAM_LIMIT';
  }
}

export function createWebStreams() {
  return Object.freeze({
    readable: readableFrom,
    writable: writableFrom,
    transform: transformFrom,
    collectBytes,
    collectText,
    nodeReadableToWeb: (stream) => Readable.toWeb(stream),
    webReadableToNode: (stream) => Readable.fromWeb(stream),
    nodeWritableToWeb: (stream) => Writable.toWeb(stream),
    webWritableToNode: (stream) => Writable.fromWeb(stream),
    nodeDuplexToWeb: (stream) => Duplex.toWeb(stream),
    webDuplexToNode: (pair) => Duplex.fromWeb(pair),
  });
}

export function readableFrom(source, strategy = undefined) {
  const iterator = toAsyncIterator(source);
  let finished = false;
  return new ReadableStream({
    async pull(controller) {
      if (finished) return;
      try {
        const { done, value } = await iterator.next();
        if (done) { finished = true; controller.close(); return; }
        controller.enqueue(value);
      } catch (error) { finished = true; controller.error(error); }
    },
    async cancel(reason) {
      finished = true;
      if (typeof iterator.return === 'function') await iterator.return(reason);
    },
  }, strategy);
}

export function writableFrom({ write, close, abort } = {}, strategy = undefined) {
  if (typeof write !== 'function') throw new TypeError('Web writable requires write(chunk)');
  return new WritableStream({
    write(chunk, controller) { return write(chunk, controller); },
    close() { return close?.(); },
    abort(reason) { return abort?.(reason); },
  }, strategy);
}

export function transformFrom({ transform, flush, start } = {}, writableStrategy = undefined, readableStrategy = undefined) {
  if (typeof transform !== 'function') throw new TypeError('Web transform requires transform(chunk, controller)');
  return new TransformStream({ start, transform, flush }, writableStrategy, readableStrategy);
}

export async function collectBytes(stream, { maxBytes = 64 * 1024 * 1024, signal } = {}) {
  if (!(stream instanceof ReadableStream)) throw new TypeError('collectBytes requires a ReadableStream');
  if (!Number.isInteger(maxBytes) || maxBytes < 0) throw new TypeError('maxBytes must be a non-negative integer');
  if (signal?.aborted) throw signal.reason ?? new Error('stream collection aborted');
  const reader = stream.getReader();
  const chunks = [];
  let total = 0;
  const onAbort = () => reader.cancel(signal.reason).catch(() => {});
  signal?.addEventListener('abort', onAbort, { once: true });
  try {
    while (true) {
      if (signal?.aborted) throw signal.reason ?? new Error('stream collection aborted');
      const { done, value } = await reader.read();
      if (done) break;
      const bytes = toBytes(value);
      total += bytes.byteLength;
      if (total > maxBytes) {
        await reader.cancel('Parallel stream size limit exceeded').catch(() => {});
        throw new ParallelStreamLimitError(`Parallel stream exceeds ${maxBytes} bytes`);
      }
      chunks.push(bytes);
    }
  } finally {
    signal?.removeEventListener('abort', onAbort);
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) { out.set(chunk, offset); offset += chunk.byteLength; }
  return out;
}

export async function collectText(stream, options = {}) {
  return new TextDecoder(options.encoding ?? 'utf-8', { fatal: Boolean(options.fatal) }).decode(await collectBytes(stream, options));
}

export async function createFileReadableStream(file, capabilities, { chunkSize = 64 * 1024 } = {}) {
  if (!capabilities?.resolveFile) throw new TypeError('file stream requires Parallel capabilities');
  if (!Number.isInteger(chunkSize) || chunkSize < 1) throw new TypeError('chunkSize must be a positive integer');
  const target = await capabilities.resolveFile(file, 'read');
  const handle = await fs.open(target, 'r');
  let position = 0;
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await handle.close(); } };
  return new ReadableStream({
    async pull(controller) {
      try {
        const buffer = new Uint8Array(chunkSize);
        const { bytesRead } = await handle.read(buffer, 0, buffer.byteLength, position);
        if (bytesRead === 0) { await close(); controller.close(); return; }
        position += bytesRead;
        controller.enqueue(buffer.subarray(0, bytesRead));
      } catch (error) { await close().catch(() => {}); controller.error(error); }
    },
    cancel() { return close(); },
  });
}

export async function createFileWritableStream(file, capabilities, { truncate = true } = {}) {
  if (!capabilities?.resolveFile) throw new TypeError('file stream requires Parallel capabilities');
  const target = await capabilities.resolveFile(file, 'write');
  const handle = await fs.open(target, truncate ? 'w' : 'a');
  let closed = false;
  const close = async () => { if (!closed) { closed = true; await handle.sync(); await handle.close(); } };
  return new WritableStream({
    async write(chunk) {
      if (closed) throw new Error('Parallel file writable is closed');
      const bytes = toBytes(chunk);
      await handle.write(bytes, 0, bytes.byteLength, null);
    },
    close,
    async abort() { if (!closed) { closed = true; await handle.close(); } },
  });
}

function toAsyncIterator(source) {
  if (source?.[Symbol.asyncIterator]) return source[Symbol.asyncIterator]();
  if (source?.[Symbol.iterator]) {
    const iterator = source[Symbol.iterator]();
    return { next: async () => iterator.next(), return: iterator.return ? async (value) => iterator.return(value) : undefined };
  }
  throw new TypeError('readable source must be iterable or async iterable');
}
function toBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  if (typeof value === 'string') return new TextEncoder().encode(value);
  throw new TypeError('Parallel byte stream chunks must be strings, ArrayBuffers, or typed arrays');
}
