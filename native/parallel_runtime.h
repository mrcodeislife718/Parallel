#ifndef PARALLEL_RUNTIME_H
#define PARALLEL_RUNTIME_H

#include <stdint.h>
#include <stddef.h>

#define PARALLEL_NATIVE_ABI_VERSION 6u

typedef enum parallel_capability { PARALLEL_CAP_TIMERS=1u<<0, PARALLEL_CAP_FILESYSTEM=1u<<1, PARALLEL_CAP_NETWORK=1u<<2, PARALLEL_CAP_PROCESS=1u<<3, PARALLEL_CAP_CRYPTO=1u<<4, PARALLEL_CAP_WORKERS=1u<<5 } parallel_capability;
typedef enum parallel_io_event { PARALLEL_IO_READABLE=1u<<0, PARALLEL_IO_WRITABLE=1u<<1, PARALLEL_IO_ERROR=1u<<2, PARALLEL_IO_HANGUP=1u<<3 } parallel_io_event;
typedef uint64_t parallel_timer_id; typedef uint64_t parallel_watch_id;
typedef int (*parallel_task_fn)(void *context); typedef int (*parallel_io_fn)(int descriptor,uint32_t events,void *context);
typedef int (*parallel_worker_fn)(void *context); typedef void (*parallel_worker_completion_fn)(int result,void *context); typedef void (*parallel_fs_completion_fn)(int64_t result,void *context);

typedef struct parallel_runtime { uint32_t abi_version; uint32_t capabilities; uint64_t tasks_executed; uint64_t timers_executed; uint64_t io_events_executed; uint64_t next_timer_id; uint64_t next_watch_id; int stop_requested; int closed; void *internal; } parallel_runtime;
typedef struct parallel_process { int64_t pid; int stdin_descriptor; int stdout_descriptor; int stderr_descriptor; int exited; int exit_code; int term_signal; } parallel_process;
typedef struct parallel_worker_pool parallel_worker_pool;
typedef struct parallel_worker_pool_stats { size_t thread_count; size_t queue_capacity; size_t queued; size_t active; uint64_t submitted; uint64_t completed; uint64_t post_failures; int closing; int closed; } parallel_worker_pool_stats;
typedef struct parallel_fs_scope parallel_fs_scope;
typedef struct parallel_fs_stat_info { uint64_t size; uint32_t mode; int is_file; int is_directory; int is_symlink; int64_t modified_ns; } parallel_fs_stat_info;
typedef struct parallel_fs_dir_entry { char name[256]; uint8_t type; } parallel_fs_dir_entry;

int parallel_runtime_init(parallel_runtime*,uint32_t); int parallel_runtime_has_capability(const parallel_runtime*,parallel_capability); uint64_t parallel_monotonic_ns(void);
int parallel_runtime_execute(parallel_runtime*,parallel_task_fn,void*); int parallel_runtime_post(parallel_runtime*,parallel_task_fn,void*);
int parallel_runtime_set_timeout(parallel_runtime*,uint64_t,parallel_task_fn,void*,parallel_timer_id*); int parallel_runtime_cancel_timer(parallel_runtime*,parallel_timer_id);
int parallel_runtime_watch_descriptor(parallel_runtime*,int,uint32_t,parallel_io_fn,void*,parallel_watch_id*); int parallel_runtime_unwatch_descriptor(parallel_runtime*,parallel_watch_id);
int parallel_tcp_connect(parallel_runtime*,const char*,uint16_t,int*); int parallel_tcp_finish_connect(int); int parallel_tcp_listen(parallel_runtime*,const char*,uint16_t,int,int*); int parallel_tcp_accept(parallel_runtime*,int,int*); int64_t parallel_tcp_read(int,void*,size_t); int64_t parallel_tcp_write(int,const void*,size_t); int parallel_tcp_set_nodelay(int,int); int parallel_tcp_close(int);
int parallel_process_spawn(parallel_runtime*,const char*,char *const[],char *const[],const char*,parallel_process*); int64_t parallel_process_write_stdin(parallel_process*,const void*,size_t); int parallel_process_close_stdin(parallel_process*); int64_t parallel_process_read_stdout(parallel_process*,void*,size_t); int64_t parallel_process_read_stderr(parallel_process*,void*,size_t); int parallel_process_poll_exit(parallel_process*); int parallel_process_signal(parallel_process*,int); int parallel_process_close_pipes(parallel_process*); int parallel_process_dispose(parallel_process*,int);
int parallel_worker_pool_create(parallel_runtime*,size_t,size_t,parallel_worker_pool**); int parallel_worker_pool_submit(parallel_worker_pool*,parallel_worker_fn,void*,parallel_worker_completion_fn,void*); int parallel_worker_pool_snapshot(parallel_worker_pool*,parallel_worker_pool_stats*); int parallel_worker_pool_close(parallel_worker_pool*); int parallel_worker_pool_destroy(parallel_worker_pool*);

/* Additive ABI 6 filesystem surface: rooted descriptors and no-follow component walks prevent traversal/symlink escape. */
int parallel_fs_scope_create(parallel_runtime*,const char*,const char*,parallel_fs_scope**); int parallel_fs_scope_close(parallel_fs_scope*);
int parallel_fs_open_read(parallel_fs_scope*,const char*,int*); int parallel_fs_open_write(parallel_fs_scope*,const char*,int,int,int,uint32_t,int*);
int64_t parallel_fs_read(int,void*,size_t); int64_t parallel_fs_write(int,const void*,size_t); int parallel_fs_sync(int); int parallel_fs_close(int);
int parallel_fs_stat(parallel_fs_scope*,const char*,parallel_fs_stat_info*); int parallel_fs_read_dir(parallel_fs_scope*,const char*,parallel_fs_dir_entry*,size_t,size_t*);
/* Read buffer/scope remain valid until callback. Write input is copied before submission. Completion runs on reactor thread. */
int parallel_fs_read_async(parallel_worker_pool*,parallel_fs_scope*,const char*,void*,size_t,parallel_fs_completion_fn,void*);
int parallel_fs_write_async(parallel_worker_pool*,parallel_fs_scope*,const char*,const void*,size_t,int,int,int,uint32_t,parallel_fs_completion_fn,void*);

int parallel_runtime_run_once(parallel_runtime*,uint64_t); int parallel_runtime_run(parallel_runtime*); int parallel_runtime_stop(parallel_runtime*); int parallel_runtime_close(parallel_runtime*); const char *parallel_runtime_version(void);
#endif
