#define _POSIX_C_SOURCE 200809L
#include "parallel_runtime.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static int require_network(const parallel_runtime *runtime) {
  if (runtime == NULL) return -1;
  if (runtime->closed) return -2;
  if (!parallel_runtime_has_capability(runtime, PARALLEL_CAP_NETWORK)) return -3;
  return 0;
}

static int set_nonblocking(int descriptor) {
  int flags = fcntl(descriptor, F_GETFL, 0);
  if (flags < 0) return -errno;
  if (fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0) return -errno;
  return 0;
}

static int resolve_host(const char *host, uint16_t port, int passive, struct addrinfo **result) {
  if (host == NULL || result == NULL) return -1;
  char service[6];
  int written = snprintf(service, sizeof(service), "%u", (unsigned int) port);
  if (written <= 0 || (size_t) written >= sizeof(service)) return -4;

  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  if (passive) hints.ai_flags = AI_PASSIVE;

  int status = getaddrinfo(host[0] == '\0' ? NULL : host, service, &hints, result);
  if (status != 0) return -20 - status;
  return 0;
}

int parallel_tcp_connect(parallel_runtime *runtime, const char *host, uint16_t port, int *descriptor) {
  int permission = require_network(runtime);
  if (permission != 0) return permission;
  if (host == NULL || host[0] == '\0' || descriptor == NULL || port == 0) return -1;

  struct addrinfo *addresses = NULL;
  int resolved = resolve_host(host, port, 0, &addresses);
  if (resolved != 0) return resolved;

  int last_error = -8;
  for (struct addrinfo *address = addresses; address != NULL; address = address->ai_next) {
    int socket_fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (socket_fd < 0) { last_error = -errno; continue; }
    int nonblocking = set_nonblocking(socket_fd);
    if (nonblocking != 0) { last_error = nonblocking; close(socket_fd); continue; }

    int status = connect(socket_fd, address->ai_addr, address->ai_addrlen);
    if (status == 0) {
      *descriptor = socket_fd;
      freeaddrinfo(addresses);
      return 1;
    }
    if (errno == EINPROGRESS || errno == EALREADY || errno == EWOULDBLOCK) {
      *descriptor = socket_fd;
      freeaddrinfo(addresses);
      return 0;
    }
    last_error = -errno;
    close(socket_fd);
  }

  freeaddrinfo(addresses);
  return last_error;
}

int parallel_tcp_finish_connect(int descriptor) {
  if (descriptor < 0) return -1;
  int error = 0;
  socklen_t length = (socklen_t) sizeof(error);
  if (getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &error, &length) != 0) return -errno;
  if (error == 0) return 1;
  if (error == EINPROGRESS || error == EALREADY) return 0;
  return -error;
}

int parallel_tcp_listen(parallel_runtime *runtime, const char *host, uint16_t port, int backlog, int *descriptor) {
  int permission = require_network(runtime);
  if (permission != 0) return permission;
  if (host == NULL || descriptor == NULL || backlog < 1) return -1;

  struct addrinfo *addresses = NULL;
  int resolved = resolve_host(host, port, 1, &addresses);
  if (resolved != 0) return resolved;

  int last_error = -8;
  for (struct addrinfo *address = addresses; address != NULL; address = address->ai_next) {
    int socket_fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
    if (socket_fd < 0) { last_error = -errno; continue; }
    int reuse = 1;
    (void) setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    int nonblocking = set_nonblocking(socket_fd);
    if (nonblocking != 0) { last_error = nonblocking; close(socket_fd); continue; }
    if (bind(socket_fd, address->ai_addr, address->ai_addrlen) != 0) {
      last_error = -errno;
      close(socket_fd);
      continue;
    }
    if (listen(socket_fd, backlog) != 0) {
      last_error = -errno;
      close(socket_fd);
      continue;
    }
    *descriptor = socket_fd;
    freeaddrinfo(addresses);
    return 0;
  }

  freeaddrinfo(addresses);
  return last_error;
}

int parallel_tcp_accept(parallel_runtime *runtime, int listener_descriptor, int *client_descriptor) {
  int permission = require_network(runtime);
  if (permission != 0) return permission;
  if (listener_descriptor < 0 || client_descriptor == NULL) return -1;

  int client = accept(listener_descriptor, NULL, NULL);
  if (client < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
    return -errno;
  }
  int nonblocking = set_nonblocking(client);
  if (nonblocking != 0) {
    close(client);
    return nonblocking;
  }
  *client_descriptor = client;
  return 1;
}

int64_t parallel_tcp_read(int descriptor, void *buffer, size_t capacity) {
  if (descriptor < 0 || buffer == NULL || capacity == 0) return -1;
  ssize_t count = recv(descriptor, buffer, capacity, 0);
  if (count >= 0) return (int64_t) count;
  if (errno == EAGAIN || errno == EWOULDBLOCK) return -11;
  return -(int64_t) errno;
}

int64_t parallel_tcp_write(int descriptor, const void *buffer, size_t length) {
  if (descriptor < 0 || buffer == NULL || length == 0) return -1;
#ifdef MSG_NOSIGNAL
  ssize_t count = send(descriptor, buffer, length, MSG_NOSIGNAL);
#else
  ssize_t count = send(descriptor, buffer, length, 0);
#endif
  if (count >= 0) return (int64_t) count;
  if (errno == EAGAIN || errno == EWOULDBLOCK) return -11;
  return -(int64_t) errno;
}

int parallel_tcp_set_nodelay(int descriptor, int enabled) {
  if (descriptor < 0) return -1;
  int value = enabled ? 1 : 0;
  if (setsockopt(descriptor, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0) return -errno;
  return 0;
}

int parallel_tcp_close(int descriptor) {
  if (descriptor < 0) return -1;
  if (close(descriptor) != 0) return -errno;
  return 0;
}
