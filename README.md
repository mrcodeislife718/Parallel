# Parallel

Parallel is the Cannon runtime.

It is the execution layer for Cannon/Cannon+ programs and is responsible for the category of work handled by Node/Deno/Bun-class runtimes: async I/O, timers, workers, concurrency, filesystem access, networking, HTTP/TLS, streams, processes, crypto, permissions, and runtime diagnostics.

## Long-term runtime goal

The architectural destination is for:

```text
cannon app.cannon
```

to execute through Parallel without Node being Cannon's runtime identity. Hosted/bootstrap runtimes may be used during implementation, but Cannon should ultimately have its own runtime path.

## Role in the ecosystem

```text
Cannon / Cannon+
       │
       ▼
      Nova
       │
       ▼
    Parallel
       │
   ┌───┼───────────┐
   ▼   ▼           ▼
Cadence Plasma   Velocity
   │               │
   └──────► Chronos
```

- **Nova** produces and coordinates compiled artifacts.
- **Cadence** builds Cannon-native HTTP/backend behavior on Parallel.
- **Plasma** supplies runtime-module and foreign/native interoperability.
- **Velocity** orchestrates local application development and targets.
- **Cortex** inspects/debugs runtime behavior.
- **Chronos** can host and deploy reproducible Parallel-based execution.

## Design direction

Parallel takes Node's ecosystem practicality, Bun's integrated-tooling speed, and Deno's secure-by-default permission model as reference points while avoiding unnecessary module fragmentation and dependence on an external runtime identity.

## Proof standard

Every runtime API requires end-to-end execution tests, cleanup tests, failure-path tests, and permission tests. Performance claims require reproducible benchmarks.

## Commercial boundary

Parallel core is adoption infrastructure. Revenue can come from managed runtime hosting, enterprise support, hardened/safety profiles, observability, and Chronos-hosted execution.

See [ECOSYSTEM.md](./ECOSYSTEM.md) and [ROADMAP.md](./ROADMAP.md).
