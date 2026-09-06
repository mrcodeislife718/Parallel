#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct parallel_fs_async_job {
  parallel_worker_pool *pool;
  parallel_fs_scope *scope;
  char *path;
  void *buffer;
  size_t length;
  uint32_t mode;
  int write_mode;
  int create;
  int truncate;
  int append;
  int64_t io_result;
  parallel_fs_completion_fn completion;
  void *completion_context;
} parallel_fs_async_job;

static int fs_async_work(void *context) {
  parallel_fs_async_job *job = (parallel_fs_async_job *) context;
  int fd = -1;
  int opened = job->write_mode
    ? parallel_fs_open_write(job->scope, job->path, job->create, job->truncate, job->append, job->mode, &fd)
    : parallel_fs_open_read(job->scope, job->path, &fd);
  if (opened != 0) { job->io_result = opened; return opened; }
  job->io_result = job->write_mode
    ? parallel_fs_write(fd, job->buffer, job->length)
    : parallel_fs_read(fd, job->buffer, job->length);
  if (job->write_mode && job->io_result >= 0) {
    int synced = parallel_fs_sync(fd);
    if (synced != 0) job->io_result = synced;
  }
  int closed = parallel_fs_close(fd);
  if (job->io_result >= 0 && closed != 0) job->io_result = closed;
  return job->io_result < 0 ? (int)job->io_result : 0;
}

static void fs_async_complete(int worker_result, void *context) {
  (void) worker_result;
  parallel_fs_async_job *job = (parallel_fs_async_job *) context;
  if (job == NULL) return;
  if (job->completion != NULL) job->completion(job->io_result, job->completion_context);
  free(job->path);
  if (job->write_mode) free(job->buffer);
  free(job);
}

static int submit_job(
  parallel_worker_pool *pool,
  parallel_fs_scope *scope,
  const char *path,
  void *buffer,
  size_t length,
  int write_mode,
  int create,
  int truncate,
  int append,
  uint32_t mode,
  parallel_fs_completion_fn completion,
  void *completion_context
) {
  if (pool == NULL || scope == NULL || path == NULL || buffer == NULL || completion == NULL) return -1;
  parallel_fs_async_job *job = (parallel_fs_async_job *) calloc(1u, sizeof(*job));
  if (job == NULL) return -5;
  size_t path_len = strlen(path);
  job->path = (char *) malloc(path_len + 1u);
  if (job->path == NULL) { free(job); return -5; }
  memcpy(job->path, path, path_len + 1u);
  job->pool = pool;
  job->scope = scope;
  job->length = length;
  job->write_mode = write_mode;
  job->create = create;
  job->truncate = truncate;
  job->append = append;
  job->mode = mode;
  job->completion = completion;
  job->completion_context = completion_context;

  if (write_mode) {
    job->buffer = malloc(length == 0 ? 1u : length);
    if (job->buffer == NULL) { free(job->path); free(job); return -5; }
    if (length != 0) memcpy(job->buffer, buffer, length);
  } else {
    job->buffer = buffer;
  }

  int submitted = parallel_worker_pool_submit(pool, fs_async_work, job, fs_async_complete, job);
  if (submitted != 0) {
    if (write_mode) free(job->buffer);
    free(job->path);
    free(job);
    return submitted;
  }
  return 0;
}

int parallel_fs_read_async(
  parallel_worker_pool *pool,
  parallel_fs_scope *scope,
  const char *path,
  void *buffer,
  size_t capacity,
  parallel_fs_completion_fn completion,
  void *completion_context
) {
  return submit_job(pool, scope, path, buffer, capacity, 0, 0, 0, 0, 0u, completion, completion_context);
}

int parallel_fs_write_async(
  parallel_worker_pool *pool,
  parallel_fs_scope *scope,
  const char *path,
  const void *buffer,
  size_t length,
  int create,
  int truncate,
  int append,
  uint32_t mode,
  parallel_fs_completion_fn completion,
  void *completion_context
) {
  return submit_job(pool, scope, path, (void *)buffer, length, 1, create, truncate, append, mode, completion, completion_context);
}
