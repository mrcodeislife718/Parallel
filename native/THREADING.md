# Parallel native threading contract

ABI 6 introduces a reactor-owned wakeup channel and thread-safe task posting. External pthreads may call `parallel_runtime_post()` while the runtime is open; posting is serialized through the runtime task mutex and signals the internal wake descriptor so a blocked `poll()` returns immediately. The wake descriptor is internal kernel state and is never exposed as application I/O.

The runtime must not be closed while producer threads can still post. Shutdown must first stop/join producers, then close the runtime. This avoids use-after-close races while preserving deterministic native ownership.
