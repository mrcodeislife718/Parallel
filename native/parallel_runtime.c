#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <stdlib.h>
#include <time.h>
#include <limits.h>

typedef struct parallel_task_node {
  parallel_task_fn task;
  void *context;
  struct parallel_task_node *next;
} parallel_task_node;

typedef struct parallel_timer_node {
  parallel_timer_id id;
  uint64_t due_ns;
  parallel_task_fn task;
  void *context;
  struct parallel_timer_node *next;
} parallel_timer_node;

typedef struct parallel_runtime_state {
  parallel_task_node *task_head;
  parallel_task_node *task_tail;
  parallel_timer_node *timer_head;
} parallel_runtime_state;

static parallel_runtime_state *state_of(const parallel_runtime *runtime) {
  if (runtime == NULL) return NULL;
  return (parallel_runtime_state *) runtime->internal;
}

static void free_tasks(parallel_task_node *node) {
  while (node != NULL) {
    parallel_task_node *next = node->next;
    free(node);
    node = next;
  }
}

static void free_timers(parallel_timer_node *node) {
  while (node != NULL) {
    parallel_timer_node *next = node->next;
    free(node);
    node = next;
  }
}

static int sleep_ns(uint64_t nanoseconds) {
  struct timespec requested;
  requested.tv_sec = (time_t) (nanoseconds / 1000000000ull);
  requested.tv_nsec = (long) (nanoseconds % 1000000000ull);
  while (nanosleep(&requested, &requested) != 0) {
    /* EINTR is represented by a remaining interval in requested. Retry it. */
    if (requested.tv_sec == 0 && requested.tv_nsec == 0) break;
  }
  return 0;
}

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities) {
  if (runtime == NULL) return -1;
  parallel_runtime_state *state = (parallel_runtime_state *) calloc(1u, sizeof(parallel_runtime_state));
  if (state == NULL) return -5;
  runtime->abi_version = PARALLEL_NATIVE_ABI_VERSION;
  runtime->capabilities = capabilities;
  runtime->tasks_executed = 0;
  runtime->timers_executed = 0;
  runtime->next_timer_id = 1;
  runtime->stop_requested = 0;
  runtime->closed = 0;
  runtime->internal = state;
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
  if (runtime->closed || runtime->internal == NULL) return -2;
  int result = task(context);
  runtime->tasks_executed += 1;
  return result;
}

int parallel_runtime_post(parallel_runtime *runtime, parallel_task_fn task, void *context) {
  if (runtime == NULL || task == NULL) return -1;
  if (runtime->closed) return -2;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;

  parallel_task_node *node = (parallel_task_node *) calloc(1u, sizeof(parallel_task_node));
  if (node == NULL) return -5;
  node->task = task;
  node->context = context;

  if (state->task_tail == NULL) state->task_head = node;
  else state->task_tail->next = node;
  state->task_tail = node;
  return 0;
}

int parallel_runtime_set_timeout(
  parallel_runtime *runtime,
  uint64_t delay_ms,
  parallel_task_fn task,
  void *context,
  parallel_timer_id *timer_id
) {
  if (runtime == NULL || task == NULL || timer_id == NULL) return -1;
  if (runtime->closed) return -2;
  if (!parallel_runtime_has_capability(runtime, PARALLEL_CAP_TIMERS)) return -3;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;

  const uint64_t now = parallel_monotonic_ns();
  if (now == 0) return -6;
  if (delay_ms > (UINT64_MAX - now) / 1000000ull) return -4;

  parallel_timer_node *node = (parallel_timer_node *) calloc(1u, sizeof(parallel_timer_node));
  if (node == NULL) return -5;
  node->id = runtime->next_timer_id++;
  if (node->id == 0) node->id = runtime->next_timer_id++;
  node->due_ns = now + (delay_ms * 1000000ull);
  node->task = task;
  node->context = context;

  parallel_timer_node **cursor = &state->timer_head;
  while (*cursor != NULL && ((*cursor)->due_ns < node->due_ns || ((*cursor)->due_ns == node->due_ns && (*cursor)->id < node->id))) {
    cursor = &(*cursor)->next;
  }
  node->next = *cursor;
  *cursor = node;
  *timer_id = node->id;
  return 0;
}

int parallel_runtime_cancel_timer(parallel_runtime *runtime, parallel_timer_id timer_id) {
  if (runtime == NULL || timer_id == 0) return -1;
  if (runtime->closed) return -2;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;

  parallel_timer_node **cursor = &state->timer_head;
  while (*cursor != NULL) {
    if ((*cursor)->id == timer_id) {
      parallel_timer_node *removed = *cursor;
      *cursor = removed->next;
      free(removed);
      return 1;
    }
    cursor = &(*cursor)->next;
  }
  return 0;
}

static int run_task(parallel_runtime *runtime, parallel_runtime_state *state) {
  parallel_task_node *node = state->task_head;
  if (node == NULL) return 0;
  state->task_head = node->next;
  if (state->task_head == NULL) state->task_tail = NULL;
  parallel_task_fn task = node->task;
  void *context = node->context;
  free(node);
  (void) task(context);
  runtime->tasks_executed += 1;
  return 1;
}

static int run_due_timer(parallel_runtime *runtime, parallel_runtime_state *state, uint64_t now) {
  parallel_timer_node *node = state->timer_head;
  if (node == NULL || node->due_ns > now) return 0;
  state->timer_head = node->next;
  parallel_task_fn task = node->task;
  void *context = node->context;
  free(node);
  (void) task(context);
  runtime->tasks_executed += 1;
  runtime->timers_executed += 1;
  return 1;
}

int parallel_runtime_run_once(parallel_runtime *runtime, uint64_t max_wait_ms) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;

  if (run_task(runtime, state)) return 1;

  uint64_t now = parallel_monotonic_ns();
  if (now == 0) return -6;
  if (run_due_timer(runtime, state, now)) return 1;

  if (state->timer_head == NULL || max_wait_ms == 0) return 0;

  uint64_t wait_ns = state->timer_head->due_ns > now ? state->timer_head->due_ns - now : 0;
  const uint64_t max_wait_ns = max_wait_ms > UINT64_MAX / 1000000ull ? UINT64_MAX : max_wait_ms * 1000000ull;
  if (wait_ns > max_wait_ns) wait_ns = max_wait_ns;
  if (wait_ns > 0) sleep_ns(wait_ns);

  now = parallel_monotonic_ns();
  if (now == 0) return -6;
  return run_due_timer(runtime, state, now);
}

int parallel_runtime_run(parallel_runtime *runtime) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;

  runtime->stop_requested = 0;
  while (!runtime->stop_requested) {
    if (state->task_head == NULL && state->timer_head == NULL) return 0;
    int result = parallel_runtime_run_once(runtime, 1000u);
    if (result < 0) return result;
  }
  return 0;
}

int parallel_runtime_stop(parallel_runtime *runtime) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  runtime->stop_requested = 1;
  return 0;
}

int parallel_runtime_close(parallel_runtime *runtime) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  parallel_runtime_state *state = state_of(runtime);
  if (state != NULL) {
    free_tasks(state->task_head);
    free_timers(state->timer_head);
    free(state);
  }
  runtime->internal = NULL;
  runtime->closed = 1;
  return 0;
}

const char *parallel_runtime_version(void) {
  return "parallel-native/0.2-abi2";
}
