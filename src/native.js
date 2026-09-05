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
    abiVersion: 2,
    implementation: 'c11',
    capabilities: ['timers','filesystem','network','process','crypto','workers'],
    eventLoop: ['fifo-tasks','one-shot-timers','timer-cancellation','bounded-run-once','continuous-run','stop-close-lifecycle'],
    boundary: 'native-kernel',
    status: 'active-native-transition'
  };
}

export async function buildNativeKernel({ cc = process.env.CC || 'cc', outDir, extraFlags = [] } = {}) {
  if (!outDir) throw new Error('buildNativeKernel requires outDir');
  const root = path.resolve(outDir);
  await fs.mkdir(root, { recursive: true });
  const source = path.join(nativeRoot, 'parallel_runtime.c');
  const object = path.join(root, 'parallel_runtime.o');
  const result = await run(cc, ['-std=c11','-O2','-Wall','-Wextra','-Werror','-I',nativeRoot,'-c',source,'-o',object,...extraFlags]);
  if (!result.ok) return { ok:false, result, object:null, manifest:nativeRuntimeManifest() };
  const bytes = await fs.readFile(object);
  return {
    ok: true,
    object,
    digest: crypto.createHash('sha256').update(bytes).digest('hex'),
    bytes: bytes.length,
    result,
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
  const compile = await run(cc, ['-std=c11','-O2','-Wall','-Wextra','-Werror','-I',nativeRoot,probe,path.join(nativeRoot,'parallel_runtime.c'),'-o',binary]);
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
