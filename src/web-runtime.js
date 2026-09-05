import { ParallelPermissionError } from './index.js';

export class ParallelFetchError extends Error {
  constructor(message, code = 'PARALLEL_FETCH_ERROR', cause = undefined) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'ParallelFetchError';
    this.code = code;
  }
}

export function createParallelFetch({
  capabilities,
  fetchImpl = globalThis.fetch,
  maxRedirects = 8,
  maxResponseBytes = 64 * 1024 * 1024,
  timeoutMs = 30_000,
} = {}) {
  if (!capabilities?.assertHost) throw new TypeError('Parallel fetch requires runtime capabilities');
  if (typeof fetchImpl !== 'function') throw new TypeError('Parallel fetch requires a fetch implementation');
  positiveInteger(maxRedirects, 'maxRedirects', { allowZero: true });
  positiveInteger(maxResponseBytes, 'maxResponseBytes');
  positiveInteger(timeoutMs, 'timeoutMs');

  return async function parallelFetch(input, init = {}) {
    let request = new Request(input, init);
    let redirects = 0;
    const externalSignal = request.signal;

    while (true) {
      const url = validateHttpUrl(request.url);
      assertUrlAllowed(url, capabilities);
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(new ParallelFetchError(`Parallel fetch timed out after ${timeoutMs}ms`, 'PARALLEL_FETCH_TIMEOUT')), timeoutMs);
      timer.unref?.();
      const signal = combineSignals(externalSignal, controller.signal);
      let response;
      try {
        response = await fetchImpl(request, { redirect: 'manual', signal });
      } catch (error) {
        if (controller.signal.aborted && !externalSignal?.aborted) throw controller.signal.reason;
        throw error;
      } finally {
        clearTimeout(timer);
      }

      if (!isRedirect(response.status)) return boundResponse(response, maxResponseBytes);
      const location = response.headers.get('location');
      if (!location) return boundResponse(response, maxResponseBytes);
      if (redirects >= maxRedirects) throw new ParallelFetchError(`Parallel fetch exceeded ${maxRedirects} redirects`, 'PARALLEL_FETCH_REDIRECT_LIMIT');

      const nextUrl = new URL(location, request.url);
      validateHttpUrl(nextUrl.href);
      assertUrlAllowed(nextUrl, capabilities);
      redirects += 1;
      request = redirectRequest(request, nextUrl, response.status);
    }
  };
}

function assertUrlAllowed(url, capabilities) {
  const port = url.port ? Number(url.port) : url.protocol === 'https:' ? 443 : 80;
  try { capabilities.assertHost(url.hostname, port); }
  catch (error) {
    if (error instanceof ParallelPermissionError || error?.name === 'ParallelPermissionError') throw error;
    throw new ParallelFetchError(`Parallel could not evaluate network permission for ${url.host}`, 'PARALLEL_FETCH_PERMISSION', error);
  }
}

function validateHttpUrl(value) {
  let url;
  try { url = value instanceof URL ? value : new URL(value); }
  catch (error) { throw new ParallelFetchError(`Invalid fetch URL: ${value}`, 'PARALLEL_FETCH_URL', error); }
  if (!['http:', 'https:'].includes(url.protocol)) throw new ParallelFetchError(`Unsupported fetch protocol: ${url.protocol}`, 'PARALLEL_FETCH_PROTOCOL');
  if (url.username || url.password) throw new ParallelFetchError('Credentials in fetch URLs are not allowed', 'PARALLEL_FETCH_URL_CREDENTIALS');
  return url;
}

function redirectRequest(previous, nextUrl, status) {
  let method = previous.method;
  let body = previous.body;
  const headers = new Headers(previous.headers);
  if (status === 303 || ((status === 301 || status === 302) && method === 'POST')) {
    method = 'GET';
    body = undefined;
    headers.delete('content-length');
    headers.delete('content-type');
  }
  if (new URL(previous.url).origin !== nextUrl.origin) {
    headers.delete('authorization');
    headers.delete('proxy-authorization');
    headers.delete('cookie');
  }
  const init = { method, headers, redirect: 'manual', signal: previous.signal };
  if (body != null && method !== 'GET' && method !== 'HEAD') {
    init.body = body;
    init.duplex = 'half';
  }
  return new Request(nextUrl, init);
}

function boundResponse(response, maxBytes) {
  const declared = response.headers.get('content-length');
  if (declared != null) {
    const length = Number(declared);
    if (Number.isFinite(length) && length > maxBytes) throw new ParallelFetchError(`Parallel fetch response exceeds ${maxBytes} bytes`, 'PARALLEL_FETCH_RESPONSE_LIMIT');
  }
  if (!response.body) return response;
  const reader = response.body.getReader();
  let total = 0;
  const body = new ReadableStream({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) { controller.close(); return; }
        total += value.byteLength;
        if (total > maxBytes) {
          await reader.cancel('Parallel response size limit exceeded').catch(() => {});
          controller.error(new ParallelFetchError(`Parallel fetch response exceeds ${maxBytes} bytes`, 'PARALLEL_FETCH_RESPONSE_LIMIT'));
          return;
        }
        controller.enqueue(value);
      } catch (error) { controller.error(error); }
    },
    cancel(reason) { return reader.cancel(reason); },
  });
  return new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
}

function combineSignals(a, b) {
  if (!a) return b;
  if (typeof AbortSignal.any === 'function') return AbortSignal.any([a, b]);
  const controller = new AbortController();
  const forward = (signal) => { if (!controller.signal.aborted) controller.abort(signal.reason); };
  if (a.aborted) forward(a); else a.addEventListener('abort', () => forward(a), { once: true });
  if (b.aborted) forward(b); else b.addEventListener('abort', () => forward(b), { once: true });
  return controller.signal;
}

function isRedirect(status) { return [301, 302, 303, 307, 308].includes(status); }
function positiveInteger(value, name, { allowZero = false } = {}) {
  if (!Number.isInteger(value) || value < (allowZero ? 0 : 1)) throw new TypeError(`${name} must be ${allowZero ? 'a non-negative' : 'a positive'} integer`);
  return value;
}
