#ifndef PARALLEL_RUNTIME_H
#define PARALLEL_RUNTIME_H

#include <stdint.h>
#include <stddef.h>

#define PARALLEL_NATIVE_ABI_VERSION 6u

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
typedef int (*parallel_worker_fn)(void *context);
typedef void (*parallel_worker_completion_fn)(int result, void *context);

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

typedef struct parallel_process {
  int64_t pid;
  int stdin_descriptor;
  int stdout_descriptor;
  int stderr_descriptor;
  int exited;
  int exit_code;
  int term_signal;
} parallel_process;

typedef struct parallel_worker_pool parallel_worker_pool;
typedef struct parallel_worker_pool_stats {
  size_t thread_count;
  size_t queue_capacity;
  size_t queued;
  size_t active;
  uint64_t submitted;
  uint64_t completed;
  uint64_t post_failures;
  int closing;
  int closed;
} parallel_worker_pool_stats;

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities);
int parallel_runtime_has_capability(const parallel_runtime *runtime, parallel_capability capability);
uint64_t parallel_monotonic_ns(void);

int parallel_runtime_execute(parallel_runtime *runtime, parallel_task_fn task, void *context);
/* Thread-safe while the runtime is open. External producer threads must be joined before close. */
int parallel_runtime_post(parallel_runtime *runtime, parallel_task_fn task, void *context);

int parallel_runtime_set_timeout(
  parallel_runtime *runtime,
  uint64_t delay_ms,
  parallel_task_fn task,
  void *context,
  parallel_timer_id *timer_id
);
int parallel_runtime_cancel_timer(parallel_runtime *runtime, parallel_timer_id timer_id);

int parallel_runtime_watch_descriptor(
  parallel_runtime *runtime,
  int descriptor,
  uint32_t events,
  parallel_io_fn callback,
  void *context,
  parallel_watch_id *watch_id
);
int parallel_runtime_unwatch_descriptor(parallel_runtime *runtime, parallel_watch_id watch_id);

int parallel_tcp_connect(parallel_runtime *runtime, const char *host, uint16_t port, int *descriptor);
int parallel_tcp_finish_connect(int descriptor);
int parallel_tcp_listen(parallel_runtime *runtime, const char *host, uint16_t port, int backlog, int *descriptor);
int parallel_tcp_accept(parallel_runtime *runtime, int listener_descriptor, int *client_descriptor);
int64_t parallel_tcp_read(int descriptor, void *buffer, size_t capacity);
int64_t parallel_tcp_write(int descriptor, const void *buffer, size_t length);
int parallel_tcp_set_nodelay(int descriptor, int enabled);
int parallel_tcp_close(int descriptor);

int parallel_process_spawn(
  parallel_runtime *runtime,
  const char *executable,
  char *const argv[],
  char *const envp[],
  const char *cwd,
  parallel_process *process
);
int64_t parallel_process_write_stdin(parallel_process *process, const void *buffer, size_t length);
int parallel_process_close_stdin(parallel_process *process);
int64_t parallel_process_read_stdout(parallel_process *process, void *buffer, size_t capacity);
int64_t parallel_process_read_stderr(parallel_process *process, void *buffer, size_t capacity);
int parallel_process_poll_exit(parallel_process *process);
int parallel_process_signal(parallel_process *process, int signal_number);
int parallel_process_close_pipes(parallel_process *process);
int parallel_process_dispose(parallel_process *process, int terminate_signal);

/*
 * Native bounded worker pool. Work executes on pthreads; completion callbacks
 * are marshalled back onto the owning Parallel reactor through
 * parallel_runtime_post(). Close drains accepted work and joins every thread.
 * The pool must be closed and destroyed before the owning runtime is closed.
 */
int parallel_worker_pool_create(
  parallel_runtime *runtime,
  size_t thread_count,
  size_t queue_capacity,
  parallel_worker_pool **pool_out
);
int parallel_worker_pool_submit(
  parallel_worker_pool *pool,
  parallel_worker_fn work,
  void *work_context,
  parallel_worker_completion_fn completion,
  void *completion_context
);
int parallel_worker_pool_snapshot(parallel_worker_pool *pool, parallel_worker_pool_stats *stats);
int parallel_worker_pool_close(parallel_worker_pool *pool);
int parallel_worker_pool_destroy(parallel_worker_pool *pool);

int parallel_runtime_run_once(parallel_runtime *runtime, uint64_t max_wait_ms);
int parallel_runtime_run(parallel_runtime *runtime);
int parallel_runtime_stop(parallel_runtime *runtime);
int parallel_runtime_close(parallel_runtime *runtime);
const char *parallel_runtime_version(void);

#endif
