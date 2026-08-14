#ifndef PARALLEL_RUNTIME_H
#define PARALLEL_RUNTIME_H

#include <stdint.h>
#include <stddef.h>

#define PARALLEL_NATIVE_ABI_VERSION 1u

typedef enum parallel_capability {
  PARALLEL_CAP_TIMERS = 1u << 0,
  PARALLEL_CAP_FILESYSTEM = 1u << 1,
  PARALLEL_CAP_NETWORK = 1u << 2,
  PARALLEL_CAP_PROCESS = 1u << 3,
  PARALLEL_CAP_CRYPTO = 1u << 4,
  PARALLEL_CAP_WORKERS = 1u << 5
} parallel_capability;

typedef struct parallel_runtime {
  uint32_t abi_version;
  uint32_t capabilities;
  uint64_t tasks_executed;
  int closed;
} parallel_runtime;

typedef int (*parallel_task_fn)(void *context);

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities);
int parallel_runtime_has_capability(const parallel_runtime *runtime, parallel_capability capability);
uint64_t parallel_monotonic_ns(void);
int parallel_runtime_execute(parallel_runtime *runtime, parallel_task_fn task, void *context);
int parallel_runtime_close(parallel_runtime *runtime);
const char *parallel_runtime_version(void);

#endif
