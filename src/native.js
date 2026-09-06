import path from 'node:path';
import fs from 'node:fs/promises';
import crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const nativeRoot = path.resolve(here, '..', 'native');

export function nativeRuntimeManifest() {
  return {
    protocol: 'parallel-native/1', abiVersion: 6, implementation: 'c11-posix-reactor',
    capabilities: ['timers','filesystem','network','process','crypto','workers'],
    eventLoop: [
      'fifo-tasks','thread-safe-task-posting','reactor-wakeup-pipe','one-shot-timers','timer-cancellation','descriptor-readiness','poll-reactor',
      'native-tcp-connect','native-tcp-listen','native-tcp-read-write','native-process-spawn','native-process-pipes','native-process-signals','native-process-exit-polling',
      'native-worker-pool','bounded-worker-queue','reactor-thread-worker-completion','worker-join-shutdown',
      'rooted-native-filesystem','nofollow-path-walk','native-file-read-write','native-file-stat','native-directory-read','worker-backed-filesystem','reactor-thread-filesystem-completion',
      'bounded-run-once','continuous-run','stop-close-lifecycle'
    ],
    boundary: 'native-kernel', status: 'active-native-transition'
  };
}

export async function buildNativeKernel({ cc = process.env.CC || 'cc', outDir, extraFlags = [] } = {}) {
  if (!outDir) throw new Error('buildNativeKernel requires outDir');
  const root = path.resolve(outDir); await fs.mkdir(root,{recursive:true});
  const sources = [
    ['runtime',path.join(nativeRoot,'parallel_runtime.c')],['tcp',path.join(nativeRoot,'parallel_tcp.c')],['process',path.join(nativeRoot,'parallel_process.c')],
    ['workers',path.join(nativeRoot,'parallel_workers.c')],['filesystem',path.join(nativeRoot,'parallel_filesystem.c')],['filesystem_async',path.join(nativeRoot,'parallel_filesystem_async.c')]
  ];
  const objects=[]; const results={};
  for (const [name,source] of sources) {
    const object=path.join(root,`parallel_${name}.o`);
    const result=await run(cc,['-std=c11','-O2','-Wall','-Wextra','-Werror','-pthread','-I',nativeRoot,'-c',source,'-o',object,...extraFlags]);
    results[name]=result; if(!result.ok)return{ok:false,result,objects,manifest:nativeRuntimeManifest()}; objects.push(object);
  }
  const hash=crypto.createHash('sha256'); let bytes=0;
  for(const object of objects){const objectBytes=await fs.readFile(object);hash.update(objectBytes);bytes+=objectBytes.length;}
  return{ok:true,object:objects[0],objects,digest:hash.digest('hex'),bytes,result:results,manifest:nativeRuntimeManifest()};
}

export async function linkNativeProbe(sourceText,{cc=process.env.CC||'cc',outDir}={}) {
  if(!outDir)throw new Error('linkNativeProbe requires outDir');
  const root=path.resolve(outDir); await fs.mkdir(root,{recursive:true});
  const probe=path.join(root,'probe.c'); const binary=path.join(root,process.platform==='win32'?'parallel-probe.exe':'parallel-probe'); await fs.writeFile(probe,sourceText,'utf8');
  const compile=await run(cc,['-std=c11','-O2','-Wall','-Wextra','-Werror','-pthread','-I',nativeRoot,probe,
    path.join(nativeRoot,'parallel_runtime.c'),path.join(nativeRoot,'parallel_tcp.c'),path.join(nativeRoot,'parallel_process.c'),path.join(nativeRoot,'parallel_workers.c'),path.join(nativeRoot,'parallel_filesystem.c'),path.join(nativeRoot,'parallel_filesystem_async.c'),'-o',binary]);
  if(!compile.ok)return{ok:false,stage:'compile',compile}; const execute=await run(binary,[]); return{...execute,stage:'execute',compile,binary};
}

function run(bin,args){return new Promise((resolve,reject)=>{const child=spawn(bin,args,{stdio:'pipe',shell:false});let stdout='',stderr='';child.stdout.setEncoding('utf8');child.stderr.setEncoding('utf8');child.stdout.on('data',c=>stdout+=c);child.stderr.on('data',c=>stderr+=c);child.once('error',reject);child.once('close',(code,signal)=>resolve({ok:code===0,code,signal,stdout,stderr,command:{bin,args}}));});}
