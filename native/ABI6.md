# Parallel native ABI 6

ABI 6 makes `parallel_runtime_post()` safe for external pthread producers and adds an internal nonblocking wakeup pipe to the POSIX reactor. A producer enqueues under the runtime task mutex, signals the wake descriptor, and a blocked `poll()` resumes immediately to execute queued work. Timers, descriptor registration changes, and `parallel_runtime_stop()` also signal the wake channel so reactor deadlines/state changes are observed promptly.
