import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import { CapabilitySet, ParallelPermissionError, ParallelStreamLimitError, collectBytes, collectText, createFileReadableStream, createFileWritableStream, createWebStreams, readableFrom, transformFrom, writableFrom } from '../src/index.js';

test('Parallel Web readable streams honor pull-based consumption and cancellation', async () => {
  let returned = false;
  const source = {
    [Symbol.asyncIterator]() {
      let i = 0;
      return {
        async next() { i += 1; return i <= 3 ? { done: false, value: String(i) } : { done: true }; },
        async return() { returned = true; return { done: true }; },
      };
    },
  };
  const stream = readableFrom(source);
  const reader = stream.getReader();
  assert.equal((await reader.read()).value, '1');
  await reader.cancel('done');
  assert.equal(returned, true);
});

test('Parallel writable and transform streams use WHATWG backpressure-compatible interfaces', async () => {
  const written = [];
  const writable = writableFrom({ write: (chunk) => written.push(chunk) });
  const writer = writable.getWriter();
  await writer.write('a');
  await writer.write('b');
  await writer.close();
  assert.deepEqual(written, ['a', 'b']);

  const transform = transformFrom({ transform(chunk, controller) { controller.enqueue(String(chunk).toUpperCase()); } });
  const output = collectText(transform.readable);
  const transformWriter = transform.writable.getWriter();
  await transformWriter.write('hello');
  await transformWriter.close();
  assert.equal(await output, 'HELLO');
});

test('bounded stream collection rejects oversized streaming bodies without trusting content length', async () => {
  const stream = readableFrom([new Uint8Array(4), new Uint8Array(4)]);
  await assert.rejects(collectBytes(stream, { maxBytes: 6 }), ParallelStreamLimitError);
});

test('Parallel bridges Node and Web streams in both directions', async () => {
  const api = createWebStreams();
  const web = api.nodeReadableToWeb(Readable.from([Buffer.from('one'), Buffer.from('two')]));
  assert.equal(await collectText(web), 'onetwo');
  const node = api.webReadableToNode(readableFrom(['three']));
  const chunks = [];
  for await (const chunk of node) chunks.push(Buffer.from(chunk));
  assert.equal(Buffer.concat(chunks).toString('utf8'), 'three');
});

test('permission-aware Web file streams read and durably close written files', async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'parallel-web-streams-'));
  try {
    const source = path.join(root, 'source.txt');
    const output = path.join(root, 'output.txt');
    await fs.writeFile(source, 'streamed-data');
    const caps = new CapabilitySet({ read: [root], write: [root] });
    const readable = await createFileReadableStream(source, caps, { chunkSize: 3 });
    assert.equal(await collectText(readable), 'streamed-data');

    const writable = await createFileWritableStream(output, caps);
    const writer = writable.getWriter();
    await writer.write('hello ');
    await writer.write(new TextEncoder().encode('world'));
    await writer.close();
    assert.equal(await fs.readFile(output, 'utf8'), 'hello world');

    const denied = new CapabilitySet({ read: [], write: [] });
    await assert.rejects(createFileReadableStream(source, denied), ParallelPermissionError);
    await assert.rejects(createFileWritableStream(output, denied), ParallelPermissionError);
  } finally {
    await fs.rm(root, { recursive: true, force: true });
  }
});
