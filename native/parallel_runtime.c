#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <stdlib.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <poll.h>

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

typedef struct parallel_watch_node {
  parallel_watch_id id;
  int descriptor;
  uint32_t events;
  parallel_io_fn callback;
  void *context;
  struct parallel_watch_node *next;
} parallel_watch_node;

typedef struct parallel_runtime_state {
  parallel_task_node *task_head;
  parallel_task_node *task_tail;
  parallel_timer_node *timer_head;
  parallel_watch_node *watch_head;
  size_t watch_count;
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

static void free_watches(parallel_watch_node *node) {
  while (node != NULL) {
    parallel_watch_node *next = node->next;
    free(node);
    node = next;
  }
}

int parallel_runtime_init(parallel_runtime *runtime, uint32_t capabilities) {
  if (runtime == NULL) return -1;
  parallel_runtime_state *state = (parallel_runtime_state *) calloc(1u, sizeof(parallel_runtime_state));
  if (state == NULL) return -5;
  runtime->abi_version = PARALLEL_NATIVE_ABI_VERSION;
  runtime->capabilities = capabilities;
  runtime->tasks_executed = 0;
  runtime->timers_executed = 0;
  runtime->io_events_executed = 0;
  runtime->next_timer_id = 1;
  runtime->next_watch_id = 1;
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

int parallel_runtime_watch_descriptor(
  parallel_runtime *runtime,
  int descriptor,
  uint32_t events,
  parallel_io_fn callback,
  void *context,
  parallel_watch_id *watch_id
) {
  if (runtime == NULL || callback == NULL || watch_id == NULL || descriptor < 0) return -1;
  if (runtime->closed) return -2;
  const uint32_t requested = events & (PARALLEL_IO_READABLE | PARALLEL_IO_WRITABLE);
  if (requested == 0) return -4;
  if (!parallel_runtime_has_capability(runtime, PARALLEL_CAP_FILESYSTEM) &&
      !parallel_runtime_has_capability(runtime, PARALLEL_CAP_NETWORK) &&
      !parallel_runtime_has_capability(runtime, PARALLEL_CAP_PROCESS)) return -3;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;

  parallel_watch_node *node = (parallel_watch_node *) calloc(1u, sizeof(parallel_watch_node));
  if (node == NULL) return -5;
  node->id = runtime->next_watch_id++;
  if (node->id == 0) node->id = runtime->next_watch_id++;
  node->descriptor = descriptor;
  node->events = requested;
  node->callback = callback;
  node->context = context;
  node->next = state->watch_head;
  state->watch_head = node;
  state->watch_count += 1u;
  *watch_id = node->id;
  return 0;
}

int parallel_runtime_unwatch_descriptor(parallel_runtime *runtime, parallel_watch_id watch_id) {
  if (runtime == NULL || watch_id == 0) return -1;
  if (runtime->closed) return -2;
  parallel_runtime_state *state = state_of(runtime);
  if (state == NULL) return -2;
  parallel_watch_node **cursor = &state->watch_head;
  while (*cursor != NULL) {
    if ((*cursor)->id == watch_id) {
      parallel_watch_node *removed = *cursor;
      *cursor = removed->next;
      free(removed);
      state->watch_count -= 1u;
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

static int timeout_for_poll(const parallel_runtime_state *state, uint64_t now, uint64_t max_wait_ms) {
  uint64_t wait_ms = max_wait_ms;
  if (state->timer_head != NULL) {
    uint64_t timer_wait_ms = 0;
    if (state->timer_head->due_ns > now) {
      const uint64_t delta_ns = state->timer_head->due_ns - now;
      timer_wait_ms = (delta_ns + 999999ull) / 1000000ull;
    }
    if (timer_wait_ms < wait_ms) wait_ms = timer_wait_ms;
  }
  if (wait_ms > (uint64_t) INT_MAX) return INT_MAX;
  return (int) wait_ms;
}

static int poll_one_ready_descriptor(parallel_runtime *runtime, parallel_runtime_state *state, int timeout_ms) {
  if (state->watch_count == 0) {
    if (timeout_ms <= 0) return 0;
    int result;
    do { result = poll(NULL, 0, timeout_ms); } while (result < 0 && errno == EINTR);
    return result < 0 ? -7 : 0;
  }

  struct pollfd *pollfds = (struct pollfd *) calloc(state->watch_count, sizeof(struct pollfd));
  parallel_watch_node **nodes = (parallel_watch_node **) calloc(state->watch_count, sizeof(parallel_watch_node *));
  if (pollfds == NULL || nodes == NULL) {
    free(pollfds);
    free(nodes);
    return -5;
  }

  size_t index = 0;
  for (parallel_watch_node *node = state->watch_head; node != NULL; node = node->next) {
    nodes[index] = node;
    pollfds[index].fd = node->descriptor;
    if (node->events & PARALLEL_IO_READABLE) pollfds[index].events |= POLLIN;
    if (node->events & PARALLEL_IO_WRITABLE) pollfds[index].events |= POLLOUT;
    index += 1u;
  }

  int ready;
  do { ready = poll(pollfds, (nfds_t) state->watch_count, timeout_ms); } while (ready < 0 && errno == EINTR);
  if (ready <= 0) {
    free(nodes);
    free(pollfds);
    return ready < 0 ? -7 : 0;
  }

  int executed = 0;
  for (index = 0; index < state->watch_count; index += 1u) {
    if (pollfds[index].revents == 0) continue;
    uint32_t events = 0;
    if (pollfds[index].revents & POLLIN) events |= PARALLEL_IO_READABLE;
    if (pollfds[index].revents & POLLOUT) events |= PARALLEL_IO_WRITABLE;
    if (pollfds[index].revents & POLLERR) events |= PARALLEL_IO_ERROR;
    if (pollfds[index].revents & (POLLHUP | POLLNVAL)) events |= PARALLEL_IO_HANGUP;
    parallel_watch_node *node = nodes[index];
    (void) node->callback(node->descriptor, events, node->context);
    runtime->tasks_executed += 1;
    runtime->io_events_executed += 1;
    executed = 1;
    break;
  }

  free(nodes);
  free(pollfds);
  return executed;
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

  if (state->watch_count == 0 && state->timer_head == NULL) return 0;
  const int timeout_ms = timeout_for_poll(state, now, max_wait_ms);
  const int io_result = poll_one_ready_descriptor(runtime, state, timeout_ms);
  if (io_result != 0) return io_result;

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
    if (state->task_head == NULL && state->timer_head == NULL && state->watch_count == 0) return 0;
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
    free_watches(state->watch_head);
    free(state);
  }
  runtime->internal = NULL;
  runtime->closed = 1;
  return 0;
}

const char *parallel_runtime_version(void) {
  return "parallel-native/0.3-abi3";
}
