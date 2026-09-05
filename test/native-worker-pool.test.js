import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { linkNativeProbe, nativeRuntimeManifest } from '../src/index.js';

test('native worker pool bounds work, executes on pthreads, and completes on the Parallel reactor', async (t) => {
  if (process.platform === 'win32') t.skip('native worker proof currently targets POSIX pthread runners');
  const root = await fs.mkdtemp(path.join(os.tmpdir(),'parallel-native-workers-'));
  t.after(() => fs.rm(root, { recursive:true, force:true }));

  for (const feature of ['native-worker-pool','bounded-worker-queue','reactor-thread-worker-completion','worker-join-shutdown']) {
    assert.ok(nativeRuntimeManifest().eventLoop.includes(feature));
  }

  const probe = await linkNativeProbe(`#include <stdio.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include "parallel_runtime.h"
typedef struct gate { pthread_mutex_t mutex; pthread_cond_t cond; int open; } gate;
typedef struct work_ctx { gate *g; int value; int wait; } work_ctx;
typedef struct result_ctx { int count; int sum; } result_ctx;
static int work(void *ctx){
  work_ctx *job=(work_ctx*)ctx;
  if(job->wait){
    pthread_mutex_lock(&job->g->mutex);
    while(!job->g->open) pthread_cond_wait(&job->g->cond,&job->g->mutex);
    pthread_mutex_unlock(&job->g->mutex);
  }
  return job->value;
}
static void completed(int result,void *ctx){ result_ctx *r=(result_ctx*)ctx; r->count += 1; r->sum += result; }
int main(void){
  parallel_runtime denied;
  if(parallel_runtime_init(&denied,0)!=0)return 1;
  parallel_worker_pool *missing=NULL;
  if(parallel_worker_pool_create(&denied,1,1,&missing)!=-3)return 2;
  if(parallel_runtime_close(&denied)!=0)return 3;

  parallel_runtime runtime;
  if(parallel_runtime_init(&runtime,PARALLEL_CAP_WORKERS)!=0)return 4;
  parallel_worker_pool *pool=NULL;
  if(parallel_worker_pool_create(&runtime,1,1,&pool)!=0)return 5;
  gate g; pthread_mutex_init(&g.mutex,NULL); pthread_cond_init(&g.cond,NULL); g.open=0;
  result_ctx results={0};
  work_ctx first={.g=&g,.value=20,.wait=1};
  work_ctx second={.g=&g,.value=22,.wait=0};
  work_ctx overflow={.g=&g,.value=99,.wait=0};
  if(parallel_worker_pool_submit(pool,work,&first,completed,&results)!=0)return 6;

  parallel_worker_pool_stats stats;
  int active=0;
  for(int i=0;i<100;i++){
    if(parallel_worker_pool_snapshot(pool,&stats)!=0)return 7;
    if(stats.active==1){active=1;break;}
    struct timespec delay={.tv_sec=0,.tv_nsec=1000000L}; nanosleep(&delay,NULL);
  }
  if(!active)return 8;
  if(parallel_worker_pool_submit(pool,work,&second,completed,&results)!=0)return 9;
  if(parallel_worker_pool_submit(pool,work,&overflow,completed,&results)!=-11)return 10;
  if(parallel_worker_pool_snapshot(pool,&stats)!=0)return 11;
  if(stats.queued!=1||stats.active!=1||stats.submitted!=2)return 12;

  pthread_mutex_lock(&g.mutex); g.open=1; pthread_cond_broadcast(&g.cond); pthread_mutex_unlock(&g.mutex);
  if(parallel_worker_pool_close(pool)!=0)return 13;
  for(int i=0;i<20&&results.count<2;i++){
    int ran=parallel_runtime_run_once(&runtime,50);
    if(ran<0)return 14;
  }
  if(results.count!=2||results.sum!=42)return 15;
  if(parallel_worker_pool_snapshot(pool,&stats)!=0)return 16;
  if(stats.completed!=2||stats.post_failures!=0||!stats.closed)return 17;
  if(parallel_worker_pool_submit(pool,work,&overflow,completed,&results)!=-2)return 18;
  if(parallel_worker_pool_destroy(pool)!=0)return 19;
  pthread_cond_destroy(&g.cond); pthread_mutex_destroy(&g.mutex);
  if(parallel_runtime_close(&runtime)!=0)return 20;
  printf("workers:%d:%d:%llu:%llu\n",results.count,results.sum,(unsigned long long)stats.submitted,(unsigned long long)stats.completed);
  return 0;
}`, { outDir:root });

  assert.equal(probe.ok, true, probe.stderr ?? probe.compile?.stderr);
  assert.equal(probe.stdout.trim(), 'workers:2:42:2:2');
});
