# Parallel native threading contract

ABI 6 introduces a reactor-owned wakeup channel and thread-safe task posting. External pthreads may call `parallel_runtime_post()` while the runtime is open; posting is serialized through the runtime task mutex and signals the internal wake descriptor so a blocked `poll()` returns immediately. The wake descriptor is internal kernel state and is never exposed as application I/O.

`parallel_runtime_post()` is the cross-thread ingress guaranteed by ABI 6. Timer scheduling/cancellation, descriptor watch registration/removal, and other mutable reactor structures remain reactor-thread-owned unless a later ABI explicitly promotes them to concurrent operations. Worker threads return results by posting completion tasks rather than mutating those structures directly.

The runtime must not be closed while producer threads or worker pools can still post. Shutdown order is: stop accepting work, close/join worker pools and other producers, drain reactor completions as required, then close the runtime. This avoids use-after-close races while preserving deterministic native ownership.
