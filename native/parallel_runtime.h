#ifndef PARALLEL_RUNTIME_H
#define PARALLEL_RUNTIME_H

#include <stdint.h>
#include <stddef.h>

#define PARALLEL_NATIVE_ABI_VERSION 2u

typedef enum parallel_capability {
  PARALLEL_CAP_TIMERS = 1u << 0,
  PARALLEL_CAP_FILESYSTEM = 1u << 1,
  PARALLEL_CAP_NETWORK = 1u << 2,
  PARALLEL_CAP_PROCESS = 1u << 3,
  PARALLEL_CAP_CRYPTO = 1u << 4,
  PARALLEL_CAP_WORKERS = 1u << 5
} parallel_capability;

typedef uint64_t parallel_timer_id;
typedef int (*parallel_task_fn)(void *context);

typedef struct parallel_runtime {
  uint32_t abi_version;
  uint32_t capabilities;
  uint64_t tasks_executed;
  uint64_t timers_executed;
  uint64_t next_timer_id;
  int stop_requested;
  int closed;
  void *internal;
} parallel_runtime;

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities);
int parallel_runtime_has_capability(const parallel_runtime *runtime, parallel_capability capability);
uint64_t parallel_monotonic_ns(void);

/* Execute a callback immediately on the current thread. */
int parallel_runtime_execute(parallel_runtime *runtime, parallel_task_fn task, void *context);

/* Queue a callback for FIFO execution by the event loop. */
int parallel_runtime_post(parallel_runtime *runtime, parallel_task_fn task, void *context);

/* Schedule/cancel a one-shot timer. Requires PARALLEL_CAP_TIMERS. */
int parallel_runtime_set_timeout(
  parallel_runtime *runtime,
  uint64_t delay_ms,
  parallel_task_fn task,
  void *context,
  parallel_timer_id *timer_id
);
int parallel_runtime_cancel_timer(parallel_runtime *runtime, parallel_timer_id timer_id);

/*
 * Run at most one ready task/timer. max_wait_ms bounds how long run_once may
 * sleep waiting for the next timer. Returns 1 when work ran, 0 when no work
 * was available before the deadline, and a negative value on error.
 */
int parallel_runtime_run_once(parallel_runtime *runtime, uint64_t max_wait_ms);

/* Run until no work remains or parallel_runtime_stop() is requested. */
int parallel_runtime_run(parallel_runtime *runtime);
int parallel_runtime_stop(parallel_runtime *runtime);

/* Close is idempotence-sensitive: first close succeeds, repeated close errors. */
int parallel_runtime_close(parallel_runtime *runtime);
const char *parallel_runtime_version(void);

#endif
