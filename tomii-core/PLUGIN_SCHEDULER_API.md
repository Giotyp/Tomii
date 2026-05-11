# Tomii Scheduler Plugin API

## Overview

The `TaskScheduler` trait (`tomii_core::scheduler::TaskScheduler`) is the stable
extension point for replacing Tomii's built-in schedulers with a custom
implementation. Use it when you need:

- A non-work-stealing dispatch policy (priority queues, deadline scheduling, etc.)
- Custom CPU-affinity logic beyond what `SchedulerImpl::Custom` provides.
- Integration with an external thread pool (e.g. tokio, async-std, a vendor SDK).

A working reference implementation is in `examples/scheduler-plugin/`. It builds
against the public API only and can be adapted as a starting point.

**When not to use this path.** `SchedulerImpl::Custom` (selected via
`SchedulerType::Custom` / `--custom`) is the highest-performance built-in option: it
uses per-group lock-free queues with static dispatch and CPU pinning. Prefer it for
latency-sensitive MIMO pipelines. Use the plugin path only when you genuinely need
a policy the built-in schedulers cannot express.

---

## Stability Contract

The following types, which appear in every method signature, are **stable across
minor releases**:

| Type | Crate |
|---|---|
| `SchedulerPriority` | `tomii-types` |
| `SchedulerWorkerRange` | `tomii-types` |
| `CoreSpec` | `tomii-types` |

Breaking changes to any of these types or to the trait method signatures will bump
the **major** version of `tomii-core`.

The following types are **internal** and not part of the plugin ABI. They may
change at any time without a major bump:

`Priority`, `WorkerRangeSpec`, `CoreId`, `TaskMeta`, `AsyncRecorder`,
`SchedulerBase`, `RayonScheduler`, `CustomScheduler`, `SchedulerImpl`.

Do not depend on these types from an external scheduler crate.

---

## Integration

```rust
use std::sync::Arc;
use tomii_core::runtime::TomiiRtBuilder;
use tomii_core::scheduler::TaskScheduler;

// 1. Build your scheduler.
let my_sched: Arc<dyn TaskScheduler> = MyScheduler::new(/* ... */);

// 2. Compile the graph with a placeholder scheduler for core-count metadata,
//    then replace it with the plugin.
let compiled = spec.compile(&placeholder_rayon_sched);
let rt = TomiiRtBuilder::new_with_plugin(compiled, my_sched)
    .slots(4)
    .max_streams(1000)
    .build();

rt.run();
```

`TomiiRtBuilder::new_with_plugin` wraps the `Arc<dyn TaskScheduler>` in
`SchedulerImpl::Plugin`. No unsafe code is required.

---

## Trait Methods Reference

### `spawn_task_with_priority` — **required**

Called by the resolution thread once per ready graph node. The task is already
boxed; execute it on any worker thread. Priority is a hint: `High` tasks are
on the critical path; `Normal` is the default; `Low` is background work. The
scheduler may ignore priority if the policy does not support it.

### `spawn_to_group` — *default: delegates to `spawn_task_with_priority`*

Called when a node has a `use_workers` affinity spec. `group_id` is the value
returned by `get_affinity_group` for that spec. Override if your scheduler
maintains per-group queues; the default is safe to keep if you do not.

### `get_affinity_group` — *default: returns `0`*

Maps a `SchedulerWorkerRange` to an opaque group identifier used in subsequent
`spawn_to_group` calls. Return `0` (global pool) if your scheduler does not
support worker groups.

### `workers` — **required**

Total worker thread count. Tomii uses this to size internal arrays (slot
counters, dependency tables). Must match the actual number of threads your
scheduler manages.

### `core_offset` — **required**

The CPU core index where your worker threads start. Tomii pins its resolution
threads starting at this offset. Return `0` if you do not use CPU affinity.

### `system_threads` — **required**

Number of resolution threads Tomii should spawn. Typically `1`. This value
controls how many threads run the dependency-propagation loop; higher values
increase parallelism but require the graph to have sufficient parallelism to
benefit.

### `receiver_core_offset` — **required**

CPU core offset for network receiver threads. Return `0` if network reception
is not used.

### `receiver_threads` — **required**

Number of dedicated network receiver threads. Return `0` if not used.

### `write_record` — *default: no-op*

Called after `run()` completes if `--timing` is set. Override to flush any
internal timing data to a CSV file at `path`. The default no-op is correct for
schedulers that do not record timing.

### `main_core` — *default: `None`*

If `Some(CoreSpec)`, the runtime pins the main thread to that core before
entering the resolution loop. Return `None` to skip main-thread pinning.

---

## Thread Safety

The trait bounds `Send + Sync + 'static` are enforced by the compiler. Key
implications:

- **No `!Send` state.** Mutexes, atomics, and `Arc`-wrapped inner state are
  all acceptable. `Cell`, `Rc`, or raw pointers without synchronization are not.
- **Concurrent callers.** `spawn_task_with_priority` is called from the
  resolution thread while worker threads are concurrently executing previously
  spawned tasks. All internal state must tolerate this.
- **Shutdown ordering.** The runtime does not call any `TaskScheduler` method
  after `TomiiRt::run` returns. You are responsible for draining your work queue
  in `Drop`.

---

## Performance Notes

Every call to `spawn_task_with_priority` receives a `Box<dyn FnOnce() + Send>`.
This allocation is unavoidable with the trait-object dispatch model. The built-in
`RayonScheduler` avoids it via static dispatch through `SchedulerBase::spawn_task_common`.

If allocation pressure from boxing is measurable:
1. Switch to `SchedulerImpl::Custom` — static dispatch, per-group lock-free queues.
2. Or batch tasks at the graph level to amortize the per-task cost.

The `SchedulerImpl::Plugin` path adds one virtual dispatch per `spawn` call on
top of the box allocation. All other hot-path operations (dependency counting,
slot management, result storage) are unaffected by the scheduler choice.

---

## Version Field Convention

Embed an ABI version constant in your implementing crate so that forward
compatibility can be checked at runtime:

```rust
pub const SCHEDULER_ABI_VERSION: u32 = 1;
```

---

## Semver

`tomii-core` is currently `0.x`. The `TaskScheduler` ABI stabilises at `1.0`.

Pin your plugin to the same minor series:

```toml
tomii-core = "0.1"
tomii-types = "0.1"
```
