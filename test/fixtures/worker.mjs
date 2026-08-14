import { parentPort } from 'node:worker_threads';
parentPort.on('message', ({ id, payload }) => {
  if (payload?.fail) parentPort.postMessage({ id, error:{ name:'Error', message:'worker failure' } });
  else parentPort.postMessage({ id, value: payload?.value ?? payload });
});
