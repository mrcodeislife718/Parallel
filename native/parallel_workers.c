#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct parallel_worker_job {
  parallel_worker_fn work;
  void *work_context;
  parallel_worker_completion_fn completion;
  void *completion_context;
  struct parallel_worker_job *next;
} parallel_worker_job;

typedef struct parallel_worker_completion {
  parallel_worker_completion_fn completion;
  void *context;
  int result;
} parallel_worker_completion;

struct parallel_worker_pool {
  parallel_runtime *runtime;
  pthread_t *threads;
  size_t thread_count;
  size_t queue_capacity;
  parallel_worker_job *head;
  parallel_worker_job *tail;
  size_t queued;
  size_t active;
  uint64_t submitted;
  uint64_t completed;
  uint64_t post_failures;
  int closing;
  int closed;
  pthread_mutex_t mutex;
  pthread_cond_t available;
};

static int deliver_completion(void *context) {
  parallel_worker_completion *item = (parallel_worker_completion *) context;
  if (item == NULL) return -1;
  if (item->completion != NULL) item->completion(item->result, item->context);
  free(item);
  return 0;
}

static int post_completion(parallel_worker_pool *pool, parallel_worker_job *job, int result) {
  if (job->completion == NULL) return 0;
  parallel_worker_completion *item = (parallel_worker_completion *) calloc(1u, sizeof(*item));
  if (item == NULL) return -5;
  item->completion = job->completion;
  item->context = job->completion_context;
  item->result = result;
  int posted = parallel_runtime_post(pool->runtime, deliver_completion, item);
  if (posted != 0) {
    free(item);
    return posted;
  }
  return 0;
}

static void *worker_main(void *context) {
  parallel_worker_pool *pool = (parallel_worker_pool *) context;
  for (;;) {
    if (pthread_mutex_lock(&pool->mutex) != 0) return NULL;
    while (pool->head == NULL && !pool->closing) pthread_cond_wait(&pool->available, &pool->mutex);
    if (pool->head == NULL && pool->closing) {
      pthread_mutex_unlock(&pool->mutex);
      return NULL;
    }

    parallel_worker_job *job = pool->head;
    pool->head = job->next;
    if (pool->head == NULL) pool->tail = NULL;
    pool->queued -= 1u;
    pool->active += 1u;
    pthread_mutex_unlock(&pool->mutex);

    int result = job->work(job->work_context);
    int completion_status = post_completion(pool, job, result);
    free(job);

    if (pthread_mutex_lock(&pool->mutex) != 0) return NULL;
    pool->active -= 1u;
    pool->completed += 1u;
    if (completion_status != 0) pool->post_failures += 1u;
    pthread_mutex_unlock(&pool->mutex);
  }
}

int parallel_worker_pool_create(
  parallel_runtime *runtime,
  size_t thread_count,
  size_t queue_capacity,
  parallel_worker_pool **pool_out
) {
  if (runtime == NULL || pool_out == NULL || thread_count == 0 || queue_capacity == 0) return -1;
  if (runtime->closed) return -2;
  if (!parallel_runtime_has_capability(runtime, PARALLEL_CAP_WORKERS)) return -3;

  parallel_worker_pool *pool = (parallel_worker_pool *) calloc(1u, sizeof(*pool));
  if (pool == NULL) return -5;
  pool->runtime = runtime;
  pool->thread_count = thread_count;
  pool->queue_capacity = queue_capacity;
  pool->threads = (pthread_t *) calloc(thread_count, sizeof(pthread_t));
  if (pool->threads == NULL) { free(pool); return -5; }
  if (pthread_mutex_init(&pool->mutex, NULL) != 0) { free(pool->threads); free(pool); return -8; }
  if (pthread_cond_init(&pool->available, NULL) != 0) {
    pthread_mutex_destroy(&pool->mutex);
    free(pool->threads);
    free(pool);
    return -8;
  }

  size_t created = 0;
  for (; created < thread_count; created += 1u) {
    int result = pthread_create(&pool->threads[created], NULL, worker_main, pool);
    if (result != 0) {
      if (pthread_mutex_lock(&pool->mutex) == 0) {
        pool->closing = 1;
        pthread_cond_broadcast(&pool->available);
        pthread_mutex_unlock(&pool->mutex);
      }
      for (size_t index = 0; index < created; index += 1u) pthread_join(pool->threads[index], NULL);
      pthread_cond_destroy(&pool->available);
      pthread_mutex_destroy(&pool->mutex);
      free(pool->threads);
      free(pool);
      return -result;
    }
  }

  *pool_out = pool;
  return 0;
}

int parallel_worker_pool_submit(
  parallel_worker_pool *pool,
  parallel_worker_fn work,
  void *work_context,
  parallel_worker_completion_fn completion,
  void *completion_context
) {
  if (pool == NULL || work == NULL) return -1;
  parallel_worker_job *job = (parallel_worker_job *) calloc(1u, sizeof(*job));
  if (job == NULL) return -5;
  job->work = work;
  job->work_context = work_context;
  job->completion = completion;
  job->completion_context = completion_context;

  if (pthread_mutex_lock(&pool->mutex) != 0) { free(job); return -8; }
  if (pool->closing || pool->closed) {
    pthread_mutex_unlock(&pool->mutex);
    free(job);
    return -2;
  }
  if (pool->queued >= pool->queue_capacity) {
    pthread_mutex_unlock(&pool->mutex);
    free(job);
    return -11;
  }
  if (pool->tail == NULL) pool->head = job;
  else pool->tail->next = job;
  pool->tail = job;
  pool->queued += 1u;
  pool->submitted += 1u;
  pthread_cond_signal(&pool->available);
  pthread_mutex_unlock(&pool->mutex);
  return 0;
}

int parallel_worker_pool_snapshot(parallel_worker_pool *pool, parallel_worker_pool_stats *stats) {
  if (pool == NULL || stats == NULL) return -1;
  if (pthread_mutex_lock(&pool->mutex) != 0) return -8;
  stats->thread_count = pool->thread_count;
  stats->queue_capacity = pool->queue_capacity;
  stats->queued = pool->queued;
  stats->active = pool->active;
  stats->submitted = pool->submitted;
  stats->completed = pool->completed;
  stats->post_failures = pool->post_failures;
  stats->closing = pool->closing;
  stats->closed = pool->closed;
  pthread_mutex_unlock(&pool->mutex);
  return 0;
}

int parallel_worker_pool_close(parallel_worker_pool *pool) {
  if (pool == NULL) return -1;
  if (pthread_mutex_lock(&pool->mutex) != 0) return -8;
  if (pool->closed) { pthread_mutex_unlock(&pool->mutex); return -2; }
  pool->closing = 1;
  pthread_cond_broadcast(&pool->available);
  pthread_mutex_unlock(&pool->mutex);

  for (size_t index = 0; index < pool->thread_count; index += 1u) {
    int result = pthread_join(pool->threads[index], NULL);
    if (result != 0) return -result;
  }

  if (pthread_mutex_lock(&pool->mutex) != 0) return -8;
  pool->closed = 1;
  pthread_mutex_unlock(&pool->mutex);
  return 0;
}

int parallel_worker_pool_destroy(parallel_worker_pool *pool) {
  if (pool == NULL) return -1;
  if (!pool->closed) return -16;
  parallel_worker_job *job = pool->head;
  while (job != NULL) {
    parallel_worker_job *next = job->next;
    free(job);
    job = next;
  }
  pthread_cond_destroy(&pool->available);
  pthread_mutex_destroy(&pool->mutex);
  free(pool->threads);
  free(pool);
  return 0;
}
