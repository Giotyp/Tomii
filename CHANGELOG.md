# Changelog

## v1.0.0 — 2026-05-11

### Breaking changes for plugin authors

**`TaskScheduler` trait signature changed.** Internal types no longer appear on the public trait surface. If you implemented a custom `TaskScheduler`, update the following:

| Old type | New type | Location |
|---|---|---|
| `crate::Priority` (or `scheduler::Priority`) | `tomii_types::SchedulerPriority` | `spawn_task_with_priority` first arg |
| `crate::WorkerRangeSpec` | `tomii_types::SchedulerWorkerRange` | `spawn_to_group` second arg |
| `core_affinity::CoreId` | `tomii_types::CoreSpec` (use `.to_raw()` / `CoreSpec::from_raw()`) | `main_core()` return |
| `Arc<AsyncRecorder>` parameter | removed | `get_async_recorder()` method dropped |
| `crate::TaskMeta` parameter | removed | callers pass priority/affinity directly |

The `#[cfg(feature = "plugin-scheduler")]` gate is removed — the trait is always compiled.

**`ResolutionState` renamed to `DependencyCounter`.** A backward-compat re-export shim is provided at `tomii_core::resolution_state` (`#[doc(hidden)]`), but migrate to `tomii_core::DependencyCounter`. The `MultiThreadedState` alias is similarly re-exported as `MultiThreadedCounter`.

### New features

**`ResolutionStrategy` trait** (`tomii-core/src/runtime/resolution_strategy.rs`).  
Decouples "how dependencies are resolved" from "which thread executes a task". The only v1
implementation is `MultiSlotBatchStrategy` (existing behaviour). The trait is stored as
`Arc<dyn ResolutionStrategy>` in `ExecCtx` and is accessible to custom integrations.

**`--resolution-strategy <name>` CLI flag**.  
Currently only accepts `multi-slot-batch` (the default). The flag documents the architectural
seam; future strategies register here.

**Stable scheduler types in `tomii-types`.**  
`SchedulerPriority`, `SchedulerWorkerRange`, and `CoreSpec` are now stable, versioned types in
`tomii-types`. Plugin authors depending only on `tomii-types` can implement `TaskScheduler`
without pulling in `tomii-core` internals.

**`examples/scheduler-plugin/`** — minimal FIFO scheduler example demonstrating the stable API.
Build and load via `--scheduler-plugin path/to/libscheduler_plugin.so`.

**`examples/agent-tuning/`** — 4-arm optimisation loop (random search, Bayesian/Optuna, grid
search, Claude-driven) over the stream-analytics knob space. Verifier-gated; each arm runs
50 iterations against the same perf threshold. See `examples/agent-tuning/README.md`.

**`bench/`** directory on `develop`.  
`bench/mimo-bench/`, `bench/pipeline-bench/`, and `bench/anti-diag-bench/` are now present on
the `develop` branch. Flagship numbers are reproducible from a clean clone.

**`bench/pipeline-bench/scripts/memory_measure.sh`** — measures peak RSS for Tomii vs Taskflow
at S=8, W=4 to confirm (or update) the 2.8× memory headline.

**`tomii-core/PLUGIN_SCHEDULER_API.md`** — stability contract, integration snippet, per-method
reference, thread-safety requirements, version field convention, semver expectations.

### Bug fixes / internal changes

- Loom test extended to cover `DependencyCounter` concurrent slot completion interleaving.
- Pre-existing clippy lints in `bin/main.rs` fixed (`redundant use`, `last()` → `next_back()`).
- `SchedCtx` borrow bundle lifted to `pub` visibility for strategy implementors.
- `process_batch_resolution` lifted to `pub(crate)` for use from `resolution_strategy.rs`.
- `worker_resolve_successors` lifted to `pub(super)` for delegation from the strategy trait.
