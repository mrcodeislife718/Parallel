import { parentPort, workerData } from 'node:worker_threads';

parentPort.on('message', async ({ id, payload }) => {
  try {
    if (payload?.exitCode != null) process.exit(payload.exitCode);
    if (payload?.delayMs) await new Promise((resolve) => setTimeout(resolve, payload.delayMs));
    if (payload?.fail) {
      parentPort.postMessage({ id, error: { name: 'Error', message: 'worker failure' } });
      return;
    }
    if (payload?.permissions === true) {
      parentPort.postMessage({ id, value: workerData?.parallelPermissions ?? null });
      return;
    }
    parentPort.postMessage({ id, value: payload?.value ?? payload });
  } catch (error) {
    parentPort.postMessage({ id, error: { name: error.name, message: error.message, stack: error.stack } });
  }
});
