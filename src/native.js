import path from 'node:path';
import fs from 'node:fs/promises';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const nativeRoot = path.resolve(here, '..', 'native');

export function nativeRuntimeManifest() {
  return {
    protocol: 'parallel-native/1',
    abiVersion: 4,
    implementation: 'c11-posix-reactor',
    capabilities: ['timers','filesystem','network','process','crypto','workers'],
    eventLoop: [
      'fifo-tasks',
      'one-shot-timers',
      'timer-cancellation',
      'descriptor-readiness',
      'poll-reactor',
      'native-tcp-connect',
      'native-tcp-listen',
      'native-tcp-read-write',
      'bounded-run-once',
      'continuous-run',
      'stop-close-lifecycle'
    ],
    boundary: 'native-kernel',
    status: 'active-native-transition'
  };
}

export async function buildNativeKernel({ cc = process.env.CC || 'cc', outDir, extraFlags = [] } = {}) {
  if (!outDir) throw new Error('buildNativeKernel requires outDir');
  const root = path.resolve(outDir);
  await fs.mkdir(root, { recursive: true });
  const runtimeSource = path.join(nativeRoot, 'parallel_runtime.c');
  const tcpSource = path.join(nativeRoot, 'parallel_tcp.c');
  const runtimeObject = path.join(root, 'parallel_runtime.o');
  const tcpObject = path.join(root, 'parallel_tcp.o');
  const runtimeResult = await run(cc, ['-std=c11','-O2','-Wall','-Wextra','-Werror','-I',nativeRoot,'-c',runtimeSource,'-o',runtimeObject,...extraFlags]);
  if (!runtimeResult.ok) return { ok:false, result:runtimeResult, objects:[], manifest:nativeRuntimeManifest() };
  const tcpResult = await run(cc, ['-std=c11','-O2','-Wall','-Wextra','-Werror','-I',nativeRoot,'-c',tcpSource,'-o',tcpObject,...extraFlags]);
  if (!tcpResult.ok) return { ok:false, result:tcpResult, objects:[runtimeObject], manifest:nativeRuntimeManifest() };
  const runtimeBytes = await fs.readFile(runtimeObject);
  const tcpBytes = await fs.readFile(tcpObject);
  const digest = crypto.createHash('sha256').update(runtimeBytes).update(tcpBytes).digest('hex');
  return {
    ok: true,
    object: runtimeObject,
    objects: [runtimeObject, tcpObject],
    digest,
    bytes: runtimeBytes.length + tcpBytes.length,
    result: { runtime: runtimeResult, tcp: tcpResult },
    manifest: nativeRuntimeManifest()
  };
}

export async function linkNativeProbe(sourceText, { cc = process.env.CC || 'cc', outDir } = {}) {
  if (!outDir) throw new Error('linkNativeProbe requires outDir');
  const root = path.resolve(outDir);
  await fs.mkdir(root, { recursive: true });
  const probe = path.join(root, 'probe.c');
  const binary = path.join(root, process.platform === 'win32' ? 'parallel-probe.exe' : 'parallel-probe');
  await fs.writeFile(probe, sourceText, 'utf8');
  const compile = await run(cc, [
    '-std=c11','-O2','-Wall','-Wextra','-Werror','-I',nativeRoot,
    probe,
    path.join(nativeRoot,'parallel_runtime.c'),
    path.join(nativeRoot,'parallel_tcp.c'),
    '-o',binary
  ]);
  if (!compile.ok) return { ok:false, stage:'compile', compile };
  const execute = await run(binary, []);
  return { ...execute, stage:'execute', compile, binary };
}

function run(bin, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(bin, args, { stdio:'pipe', shell:false });
    let stdout='', stderr='';
    child.stdout.setEncoding('utf8'); child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk) => stdout += chunk);
    child.stderr.on('data', (chunk) => stderr += chunk);
    child.once('error', reject);
    child.once('close', (code, signal) => resolve({ ok:code===0, code, signal, stdout, stderr, command:{bin,args} }));
  });
}
