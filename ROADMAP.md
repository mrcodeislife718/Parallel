# Parallel Roadmap

Parallel is the Cannon runtime.

## Product contract

Parallel owns execution, async I/O, timers, workers, concurrency, filesystem, networking, HTTP/TLS, streams, processes, crypto, permissions, and runtime diagnostics. The long-term goal is for `cannon app.cannon` to execute through Parallel without depending on Node as Cannon's identity.

## Design sources

Parallel takes Node's ecosystem practicality and stability, Bun's integrated-tooling speed, and Deno's secure-by-default permissions while avoiding module-system fragmentation and unnecessary external tooling.

## Implementation order

1. Hosted bootstrap runtime with real timers, filesystem, process, and networking primitives.
2. Capability/permission model.
3. Event loop and task scheduler abstraction.
4. Workers and structured concurrency.
5. Streams and HTTP/TLS.
6. Runtime module ABI for Plasma.
7. Native runtime replacement path and performance profiling.

## Proof gates

Every runtime API requires end-to-end execution tests, resource cleanup tests, failure-path tests, and permission tests. Performance claims require reproducible benchmarks.

## Commercial boundary

Parallel core remains open adoption infrastructure. Revenue can come from managed runtime hosting, enterprise support, hardened/safety profiles, observability, and Chronos-hosted execution.
