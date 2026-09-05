import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { once } from 'node:events';
import { CapabilitySet, createParallelFetch, ParallelFetchError, ParallelPermissionError } from '../src/index.js';

async function listen(handler) {
  const server = http.createServer(handler);
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const { port } = server.address();
  return { server, port, url: `http://127.0.0.1:${port}` };
}

async function close(server) { server.close(); await once(server, 'close'); }

test('Parallel fetch performs Web-standard requests only to authorized network targets', async () => {
  const target = await listen((req, res) => { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ method: req.method, ok: true })); });
  try {
    const capabilities = new CapabilitySet({ network: [`127.0.0.1:${target.port}`] });
    const fetch = createParallelFetch({ capabilities });
    const response = await fetch(`${target.url}/health`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { method: 'GET', ok: true });
    await assert.rejects(() => createParallelFetch({ capabilities: new CapabilitySet({ network: [] }) })(`${target.url}/health`), ParallelPermissionError);
  } finally { await close(target.server); }
});

test('Parallel re-authorizes every redirect hop', async () => {
  const denied = await listen((_req, res) => res.end('secret'));
  const allowed = await listen((_req, res) => { res.statusCode = 302; res.setHeader('location', `${denied.url}/secret`); res.end(); });
  try {
    const fetch = createParallelFetch({ capabilities: new CapabilitySet({ network: [`127.0.0.1:${allowed.port}`] }) });
    await assert.rejects(() => fetch(`${allowed.url}/redirect`), ParallelPermissionError);
  } finally { await close(allowed.server); await close(denied.server); }
});

test('Parallel preserves POST bodies across 307 redirects', async () => {
  const target = await listen(async (req, res) => {
    let body = '';
    for await (const chunk of req) body += chunk;
    res.end(`${req.method}:${body}`);
  });
  const redirect = await listen((_req, res) => { res.statusCode = 307; res.setHeader('location', `${target.url}/target`); res.end(); });
  try {
    const capabilities = new CapabilitySet({ network: [`127.0.0.1:${redirect.port}`, `127.0.0.1:${target.port}`] });
    const fetch = createParallelFetch({ capabilities });
    const response = await fetch(`${redirect.url}/start`, { method: 'POST', body: 'payload' });
    assert.equal(await response.text(), 'POST:payload');
  } finally { await close(redirect.server); await close(target.server); }
});

test('Parallel bounds streamed response bytes even without Content-Length', async () => {
  const target = await listen((_req, res) => { res.write('12345'); res.end('67890'); });
  try {
    const fetch = createParallelFetch({ capabilities: new CapabilitySet({ network: [`127.0.0.1:${target.port}`] }), maxResponseBytes: 6 });
    const response = await fetch(target.url);
    await assert.rejects(() => response.text(), (error) => error instanceof ParallelFetchError && error.code === 'PARALLEL_FETCH_RESPONSE_LIMIT');
  } finally { await close(target.server); }
});

test('Parallel enforces fetch header timeout and redirect ceilings', async () => {
  const slow = await listen(async (_req, res) => { await new Promise((resolve) => setTimeout(resolve, 80)); res.end('late'); });
  const loop = await listen((req, res) => { res.statusCode = 302; res.setHeader('location', req.url === '/a' ? '/b' : '/a'); res.end(); });
  try {
    const capabilities = new CapabilitySet({ network: [`127.0.0.1:${slow.port}`, `127.0.0.1:${loop.port}`] });
    const fetch = createParallelFetch({ capabilities, timeoutMs: 20, maxRedirects: 2 });
    await assert.rejects(() => fetch(slow.url), (error) => error instanceof ParallelFetchError && error.code === 'PARALLEL_FETCH_TIMEOUT');
    await assert.rejects(() => fetch(`${loop.url}/a`), (error) => error instanceof ParallelFetchError && error.code === 'PARALLEL_FETCH_REDIRECT_LIMIT');
  } finally { await close(slow.server); await close(loop.server); }
});
