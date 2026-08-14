#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"
#include <time.h>

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities) {
  if (runtime == NULL) return -1;
  runtime->abi_version = PARALLEL_NATIVE_ABI_VERSION;
  runtime->capabilities = capabilities;
  runtime->tasks_executed = 0;
  runtime->closed = 0;
  return 0;
}

int parallel_runtime_has_capability(const parallel_runtime *runtime, parallel_capability capability) {
  if (runtime == NULL || runtime->closed) return 0;
  return (runtime->capabilities & (uint32_t) capability) != 0;
}

uint64_t parallel_monotonic_ns(void) {
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
  return ((uint64_t) now.tv_sec * 1000000000ull) + (uint64_t) now.tv_nsec;
}

int parallel_runtime_execute(parallel_runtime *runtime, parallel_task_fn task, void *context) {
  if (runtime == NULL || task == NULL) return -1;
  if (runtime->closed) return -2;
  int result = task(context);
  runtime->tasks_executed += 1;
  return result;
}

int parallel_runtime_close(parallel_runtime *runtime) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  runtime->closed = 1;
  return 0;
}

const char *parallel_runtime_version(void) {
  return "parallel-native/0.1-abi1";
}
