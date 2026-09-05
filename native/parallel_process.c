#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static int require_process(const parallel_runtime *runtime) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  if (!parallel_runtime_has_capability(runtime, PARALLEL_CAP_PROCESS)) return -3;
  return 0;
}

static void close_if_open(int *descriptor) {
  if (descriptor == NULL || *descriptor < 0) return;
  (void) close(*descriptor);
  *descriptor = -1;
}

static int set_nonblocking(int descriptor) {
  int flags = fcntl(descriptor, F_GETFL, 0);
  if (flags < 0) return -errno;
  if (fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0) return -errno;
  return 0;
}

static int set_cloexec(int descriptor) {
  int flags = fcntl(descriptor, F_GETFD, 0);
  if (flags < 0) return -errno;
  if (fcntl(descriptor, F_SETFD, flags | FD_CLOEXEC) != 0) return -errno;
  return 0;
}

static int make_pipe(int descriptors[2]) {
  if (pipe(descriptors) != 0) return -errno;
  int left = set_cloexec(descriptors[0]);
  int right = set_cloexec(descriptors[1]);
  if (left != 0 || right != 0) {
    close(descriptors[0]);
    close(descriptors[1]);
    return left != 0 ? left : right;
  }
  return 0;
}

static void initialize_process(parallel_process *process) {
  memset(process, 0, sizeof(*process));
  process->pid = -1;
  process->stdin_descriptor = -1;
  process->stdout_descriptor = -1;
  process->stderr_descriptor = -1;
  process->exit_code = -1;
  process->term_signal = 0;
}

int parallel_process_spawn(
  parallel_runtime *runtime,
  const char *executable,
  char *const argv[],
  char *const envp[],
  const char *cwd,
  parallel_process *process
) {
  int permission = require_process(runtime);
  if (permission != 0) return permission;
  if (executable == NULL || executable[0] == '\0' || argv == NULL || argv[0] == NULL || process == NULL) return -1;

  initialize_process(process);
  int stdin_pipe[2] = { -1, -1 };
  int stdout_pipe[2] = { -1, -1 };
  int stderr_pipe[2] = { -1, -1 };
  int status = make_pipe(stdin_pipe);
  if (status != 0) return status;
  status = make_pipe(stdout_pipe);
  if (status != 0) { close(stdin_pipe[0]); close(stdin_pipe[1]); return status; }
  status = make_pipe(stderr_pipe);
  if (status != 0) {
    close(stdin_pipe[0]); close(stdin_pipe[1]);
    close(stdout_pipe[0]); close(stdout_pipe[1]);
    return status;
  }

  pid_t pid = fork();
  if (pid < 0) {
    int error = -errno;
    close(stdin_pipe[0]); close(stdin_pipe[1]);
    close(stdout_pipe[0]); close(stdout_pipe[1]);
    close(stderr_pipe[0]); close(stderr_pipe[1]);
    return error;
  }

  if (pid == 0) {
    if (dup2(stdin_pipe[0], STDIN_FILENO) < 0 ||
        dup2(stdout_pipe[1], STDOUT_FILENO) < 0 ||
        dup2(stderr_pipe[1], STDERR_FILENO) < 0) _exit(126);

    close(stdin_pipe[0]); close(stdin_pipe[1]);
    close(stdout_pipe[0]); close(stdout_pipe[1]);
    close(stderr_pipe[0]); close(stderr_pipe[1]);

    if (cwd != NULL && cwd[0] != '\0' && chdir(cwd) != 0) _exit(126);
    char *const empty_environment[] = { NULL };
    execve(executable, argv, envp == NULL ? empty_environment : envp);
    _exit(errno == ENOENT ? 127 : 126);
  }

  close(stdin_pipe[0]);
  close(stdout_pipe[1]);
  close(stderr_pipe[1]);

  process->pid = (int64_t) pid;
  process->stdin_descriptor = stdin_pipe[1];
  process->stdout_descriptor = stdout_pipe[0];
  process->stderr_descriptor = stderr_pipe[0];

  int stdin_nonblocking = set_nonblocking(process->stdin_descriptor);
  int stdout_nonblocking = set_nonblocking(process->stdout_descriptor);
  int stderr_nonblocking = set_nonblocking(process->stderr_descriptor);
  if (stdin_nonblocking != 0 || stdout_nonblocking != 0 || stderr_nonblocking != 0) {
    (void) kill(pid, SIGKILL);
    while (waitpid(pid, NULL, 0) < 0 && errno == EINTR) {}
    close_if_open(&process->stdin_descriptor);
    close_if_open(&process->stdout_descriptor);
    close_if_open(&process->stderr_descriptor);
    process->pid = -1;
    return stdin_nonblocking != 0 ? stdin_nonblocking : (stdout_nonblocking != 0 ? stdout_nonblocking : stderr_nonblocking);
  }
  return 0;
}

int64_t parallel_process_write_stdin(parallel_process *process, const void *buffer, size_t length) {
  if (process == NULL || process->stdin_descriptor < 0 || buffer == NULL || length == 0) return -1;
  ssize_t count = write(process->stdin_descriptor, buffer, length);
  if (count >= 0) return (int64_t) count;
  if (errno == EAGAIN || errno == EWOULDBLOCK) return -11;
  if (errno == EPIPE) return -32;
  return -(int64_t) errno;
}

int parallel_process_close_stdin(parallel_process *process) {
  if (process == NULL) return -1;
  if (process->stdin_descriptor < 0) return 0;
  int descriptor = process->stdin_descriptor;
  process->stdin_descriptor = -1;
  if (close(descriptor) != 0) return -errno;
  return 0;
}

static int64_t read_pipe(int descriptor, void *buffer, size_t capacity) {
  if (descriptor < 0 || buffer == NULL || capacity == 0) return -1;
  ssize_t count = read(descriptor, buffer, capacity);
  if (count >= 0) return (int64_t) count;
  if (errno == EAGAIN || errno == EWOULDBLOCK) return -11;
  return -(int64_t) errno;
}

int64_t parallel_process_read_stdout(parallel_process *process, void *buffer, size_t capacity) {
  if (process == NULL) return -1;
  return read_pipe(process->stdout_descriptor, buffer, capacity);
}

int64_t parallel_process_read_stderr(parallel_process *process, void *buffer, size_t capacity) {
  if (process == NULL) return -1;
  return read_pipe(process->stderr_descriptor, buffer, capacity);
}

int parallel_process_poll_exit(parallel_process *process) {
  if (process == NULL || process->pid <= 0) return -1;
  if (process->exited) return 1;
  int status = 0;
  pid_t result;
  do { result = waitpid((pid_t) process->pid, &status, WNOHANG); } while (result < 0 && errno == EINTR);
  if (result == 0) return 0;
  if (result < 0) return -errno;

  process->exited = 1;
  if (WIFEXITED(status)) {
    process->exit_code = WEXITSTATUS(status);
    process->term_signal = 0;
  } else if (WIFSIGNALED(status)) {
    process->exit_code = -1;
    process->term_signal = WTERMSIG(status);
  }
  return 1;
}

int parallel_process_signal(parallel_process *process, int signal_number) {
  if (process == NULL || process->pid <= 0 || signal_number <= 0) return -1;
  int exited = parallel_process_poll_exit(process);
  if (exited < 0) return exited;
  if (exited == 1) return 0;
  if (kill((pid_t) process->pid, signal_number) != 0) return -errno;
  return 1;
}

int parallel_process_close_pipes(parallel_process *process) {
  if (process == NULL) return -1;
  close_if_open(&process->stdin_descriptor);
  close_if_open(&process->stdout_descriptor);
  close_if_open(&process->stderr_descriptor);
  return 0;
}

int parallel_process_dispose(parallel_process *process, int terminate_signal) {
  if (process == NULL) return -1;
  if (process->pid > 0 && !process->exited) {
    int exited = parallel_process_poll_exit(process);
    if (exited < 0) return exited;
    if (exited == 0) {
      if (terminate_signal <= 0) return -16;
      if (kill((pid_t) process->pid, terminate_signal) != 0 && errno != ESRCH) return -errno;
      int status = 0;
      pid_t result;
      do { result = waitpid((pid_t) process->pid, &status, 0); } while (result < 0 && errno == EINTR);
      if (result < 0 && errno != ECHILD) return -errno;
      process->exited = 1;
      if (result > 0 && WIFEXITED(status)) { process->exit_code = WEXITSTATUS(status); process->term_signal = 0; }
      else if (result > 0 && WIFSIGNALED(status)) { process->exit_code = -1; process->term_signal = WTERMSIG(status); }
    }
  }
  (void) parallel_process_close_pipes(process);
  process->pid = -1;
  return 0;
}
