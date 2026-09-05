import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { linkNativeProbe, nativeRuntimeManifest } from '../src/index.js';

test('native Parallel wakes a blocked reactor when a pthread posts work', async (t) => {
  if (process.platform === 'win32') t.skip('native pthread wake proof currently targets POSIX C11 runners');
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'parallel-native-wakeup-'));
  t.after(() => fs.rm(root, { recursive:true, force:true }));

  assert.equal(nativeRuntimeManifest().abiVersion, 6);
  assert.ok(nativeRuntimeManifest().eventLoop.includes('thread-safe-task-posting'));
  assert.ok(nativeRuntimeManifest().eventLoop.includes('reactor-wakeup-pipe'));

  const probe = await linkNativeProbe(`#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include "parallel_runtime.h"
typedef struct payload { parallel_runtime *runtime; int *value; int post_result; } payload;
static int add_seven(void *ctx){ int *value=(int*)ctx; *value += 7; return 0; }
static void *producer(void *ctx){
  payload *p=(payload*)ctx;
  struct timespec delay={.tv_sec=0,.tv_nsec=50000000L};
  nanosleep(&delay,NULL);
  p->post_result=parallel_runtime_post(p->runtime,add_seven,p->value);
  return NULL;
}
int main(void){
  parallel_runtime runtime;
  int value=35;
  payload p={.runtime=&runtime,.value=&value,.post_result=-999};
  if(parallel_runtime_init(&runtime,PARALLEL_CAP_WORKERS)!=0)return 1;
  if(runtime.abi_version!=6)return 2;
  pthread_t thread;
  if(pthread_create(&thread,NULL,producer,&p)!=0)return 3;
  uint64_t start=parallel_monotonic_ns();
  int ran=parallel_runtime_run_once(&runtime,1000);
  uint64_t elapsed_ms=(parallel_monotonic_ns()-start)/1000000ull;
  if(pthread_join(thread,NULL)!=0)return 4;
  if(ran!=1)return 5;
  if(p.post_result!=0)return 6;
  if(value!=42)return 7;
  if(runtime.tasks_executed!=1)return 8;
  if(elapsed_ms<20 || elapsed_ms>500)return 9;
  if(parallel_runtime_close(&runtime)!=0)return 10;
  printf("%s:%d:%llu\n",parallel_runtime_version(),value,(unsigned long long)elapsed_ms);
  return 0;
}`, { outDir:root });

  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.match(probe.stdout.trim(), /^parallel-native\/0\.6-abi6:42:[2-4][0-9]{1,2}$/);
});
