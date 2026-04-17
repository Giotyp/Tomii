# tomii-core

Low-latency streaming task-graph runtime for MIMO consumer-producer pipelines.

## What it is

`tomii-core` executes computational graphs (defined in JSON) where nodes are
dynamically-loaded plugin functions and edges are typed data dependencies. The
runtime is designed for Multiple-Input Multiple-Output streaming workloads:
multiple streams can be in flight simultaneously across a fixed number of
concurrent _slots_, each one a fully isolated processing lane. Execution is
driven by a Rayon-backed worker pool, with no `async` machinery and no
dynamic allocation on the task hot path.

## Quick start

```rust
// 1. Parse and compile the graph
let spec = tomii_core::graph_gen::from_json_str(graph_json, workers)?;
let scheduler = create_scheduler(SchedulerConfig { workers: 4, core_offset: 1, .. });
let compiled = spec.compile(&scheduler);

// 2. Build the runtime (no threads spawned yet)
let mut rt = TomiiRtBuilder::new(compiled, scheduler)
    .slots(4)
    .max_streams(100)
    .max_runtime(Some(60))
    .build()?;

// 3. Run to completion (blocks until max_streams done or max_runtime exceeded)
rt.run()?;
```

`TomiiRtBuilder::build` is cheap: it validates config and wires up shared
state. Threads are created only inside `TomiiRt::run`.

## Feature flags

| Flag | Default | Description |
|---|---|---|
| `network` | enabled | UDP/TCP packet reception and dedicated receiver threads |
| `recording` | enabled | `AsyncRecorder` + per-thread timing CSV output |
| `cli` | enabled | Command-line runner binary (`clap` + `gag`) |
| `plugin-scheduler` | disabled | External scheduler via `Arc<dyn TaskScheduler>`; adds one `Box` allocation per task spawn |
| `test-utils` | disabled | No-op function fallback when `get_func()` returns `None`; allows integration tests to run without a plugin dylib |
| `rdtsc` | disabled | x86 RDTSC-based high-resolution timing (x86_64 only) |

Disabling `network` removes all socket infrastructure. Disabling `recording`
removes `AsyncRecorder` and the timing subsystem. Both can be combined for
a minimal embedded deployment.

## Runtime internals

For a deep dive into the execution model — thread roles, the four-phase batch
protocol, slot lifecycle, lock-ordering rules, and memory ordering rationale —
see [`src/runtime/ARCHITECTURE.md`](src/runtime/ARCHITECTURE.md)