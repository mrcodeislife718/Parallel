import tls from 'node:tls';

export class ParallelTlsError extends Error {
  constructor(message, code = 'PARALLEL_TLS_ERROR', cause = undefined) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = 'ParallelTlsError';
    this.code = code;
  }
}

export async function connectTls({
  host,
  port = 443,
  capabilities,
  servername = host,
  ca = undefined,
  cert = undefined,
  key = undefined,
  ALPNProtocols = ['h2', 'http/1.1'],
  minVersion = 'TLSv1.2',
  rejectUnauthorized = true,
  timeoutMs = 30_000,
} = {}) {
  if (!capabilities?.assertHost) throw new TypeError('Parallel TLS requires runtime capabilities');
  if (typeof host !== 'string' || !host) throw new TypeError('host must be a non-empty string');
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new TypeError('port must be between 1 and 65535');
  if (typeof servername !== 'string' || !servername) throw new TypeError('servername must be a non-empty string');
  if (!Number.isInteger(timeoutMs) || timeoutMs < 1) throw new TypeError('timeoutMs must be a positive integer');
  capabilities.assertHost(host, port);

  return new Promise((resolve, reject) => {
    let settled = false;
    const socket = tls.connect({ host, port, servername, ca, cert, key, ALPNProtocols, minVersion, rejectUnauthorized });
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(new ParallelTlsError(`Parallel TLS connection timed out after ${timeoutMs}ms`, 'PARALLEL_TLS_TIMEOUT'));
    }, timeoutMs);
    timer.unref?.();

    const finishError = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.destroy();
      reject(error instanceof ParallelTlsError ? error : new ParallelTlsError(error.message, 'PARALLEL_TLS_CONNECT', error));
    };

    socket.once('error', finishError);
    socket.once('secureConnect', () => {
      if (settled) return;
      if (rejectUnauthorized && !socket.authorized) {
        finishError(new ParallelTlsError(socket.authorizationError || 'TLS peer authorization failed', 'PARALLEL_TLS_UNAUTHORIZED'));
        return;
      }
      settled = true;
      clearTimeout(timer);
      socket.removeListener('error', finishError);
      resolve(Object.freeze({
        socket,
        authorized: socket.authorized,
        authorizationError: socket.authorizationError ?? null,
        protocol: socket.getProtocol(),
        alpnProtocol: socket.alpnProtocol || null,
        cipher: socket.getCipher(),
        peerCertificate: socket.getPeerCertificate(true),
      }));
    });
  });
}
