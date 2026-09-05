#ifndef PARALLEL_RUNTIME_H
#define PARALLEL_RUNTIME_H

#include <stdint.h>
#include <stddef.h>

#define PARALLEL_NATIVE_ABI_VERSION 3u

typedef enum parallel_capability {
  PARALLEL_CAP_TIMERS = 1u << 0,
  PARALLEL_CAP_FILESYSTEM = 1u << 1,
  PARALLEL_CAP_NETWORK = 1u << 2,
  PARALLEL_CAP_PROCESS = 1u << 3,
  PARALLEL_CAP_CRYPTO = 1u << 4,
  PARALLEL_CAP_WORKERS = 1u << 5
} parallel_capability;

typedef enum parallel_io_event {
  PARALLEL_IO_READABLE = 1u << 0,
  PARALLEL_IO_WRITABLE = 1u << 1,
  PARALLEL_IO_ERROR = 1u << 2,
  PARALLEL_IO_HANGUP = 1u << 3
} parallel_io_event;

typedef uint64_t parallel_timer_id;
typedef uint64_t parallel_watch_id;
typedef int (*parallel_task_fn)(void *context);
typedef int (*parallel_io_fn)(int descriptor, uint32_t events, void *context);

typedef struct parallel_runtime {
  uint32_t abi_version;
  uint32_t capabilities;
  uint64_t tasks_executed;
  uint64_t timers_executed;
  uint64_t io_events_executed;
  uint64_t next_timer_id;
  uint64_t next_watch_id;
  int stop_requested;
  int closed;
  void *internal;
} parallel_runtime;

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities);
int parallel_runtime_has_capability(const parallel_runtime *runtime, parallel_capability capability);
uint64_t parallel_monotonic_ns(void);

int parallel_runtime_execute(parallel_runtime *runtime, parallel_task_fn task, void *context);
int parallel_runtime_post(parallel_runtime *runtime, parallel_task_fn task, void *context);

int parallel_runtime_set_timeout(
  parallel_runtime *runtime,
  uint64_t delay_ms,
  parallel_task_fn task,
  void *context,
  parallel_timer_id *timer_id
);
int parallel_runtime_cancel_timer(parallel_runtime *runtime, parallel_timer_id timer_id);

/*
 * Register a POSIX descriptor with Parallel's native reactor. The descriptor
 * remains owned by the caller; removing a watch never closes it. READABLE and
 * WRITABLE may be requested. ERROR/HANGUP are always reported when observed.
 */
int parallel_runtime_watch_descriptor(
  parallel_runtime *runtime,
  int descriptor,
  uint32_t events,
  parallel_io_fn callback,
  void *context,
  parallel_watch_id *watch_id
);
int parallel_runtime_unwatch_descriptor(parallel_runtime *runtime, parallel_watch_id watch_id);

/* Run at most one ready task, timer, or descriptor callback. */
int parallel_runtime_run_once(parallel_runtime *runtime, uint64_t max_wait_ms);
int parallel_runtime_run(parallel_runtime *runtime);
int parallel_runtime_stop(parallel_runtime *runtime);
int parallel_runtime_close(parallel_runtime *runtime);
const char *parallel_runtime_version(void);

#endif
