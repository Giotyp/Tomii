# Changelog

## v1.1.0 — 2026-07-06

### Breaking changes for plugin authors

**Function registry contract grew three entries.** The runtime now queries the registry for
unchecked wrapper twins and their variant metadata. Converter-generated registries (the
`FUNC_PATH` build flow) get these automatically; **hand-maintained registries supplied via
`REG_PATH` must add them** or the build fails to compile. Minimal stubs:

```rust
/// # Safety
/// Always returns `None`; no contract to uphold when no unchecked twins exist.
pub unsafe fn get_unchecked_func(_name: &str) -> Option<CmPtr> { None }
pub fn get_func_argspec(_name: &str) -> Option<&'static [&'static str]> { None }
pub fn get_func_ret_variant(_name: &str) -> Option<&'static str> { None }
```

**`SchedulerConfig` gained a `worker_hook` field**
(`Option<Arc<dyn tomii_core::WorkerHook>>`). Struct-literal construction in custom-scheduler
embedders needs the new field; pass `worker_hook: None` to keep the old behaviour.

**`tomii_converter::ExportedFn` gained a `ret_variant_hint` field** — only affects code that
constructs `ExportedFn` values directly.

### New features

**Runtime state dump (`--dump-state FILE`, SIGUSR1).** `StateDumper`
(`tomii-core/src/runtime/dump.rs`) writes a JSON snapshot of per-slot state, counters,
scheduler totals, and parked frames at shutdown; sending `SIGUSR1` mid-run writes numbered
live snapshots (`FILE.1`, `FILE.2`, …).

**Graph topology dump.** `python -m tomii --dump graph.json [--out FILE]` renders any graph
JSON to GraphViz DOT (solid/dashed/bold edges for `$res`/`$dep`/`$barrier`).

**Unified knob ontology.** `python -m tomii --knob-space [graph.json] [--workload NAME]`
generates a versioned tuning space (schema v2) from the runtime knob catalog
(`--list-knobs-json`, roles perf/measurement/io/env) plus per-graph knobs (shared factor
variables, literal node factors, `group_by` widths). Any Tomii graph is now autotunable by
any optimizer without a hand-written spec; the example-local `knob_space.json` is removed.

**`WorkerHook` per-worker lifecycle callbacks.** `SchedulerConfig::worker_hook` fires
`on_worker_start` / `on_worker_exit` on both Rayon and Custom scheduler worker threads.

**`TomiiRt::run_until(predicate)`.** Runs until a `FnMut(&RunProgress) -> bool` predicate
returns true, polled at a 10 ms tick (plain `run()` behaviour is unchanged).

**Workload-pluggable agent-tuning harness.** `examples/agent-tuning/` now drives all four
search arms (random / Bayesian / grid / agent) over any registered workload —
stream-analytics, pipeline (knob-aware verify pass), and MIMO (keep-up gate with
2×-compute-floor sender pacing) — purely from the generated knob space:
`run_all.sh [iterations] [workload]`.

### Performance

- **`SuccessorArena`**: successor edges flattened into contiguous per-node slices, replacing
  the 3–4 dependent pointer chases through N×N `Arc` tables in the successor hot loop (both
  the batch path and the worker-resolvable fast path). Batch entry points now route through
  `ResolutionStrategy::drive_batch`, making the v1.0 strategy seam live.
- **Typed zero-alloc spawn path**: the Custom scheduler queues a POD `NodeTaskDesc` instead
  of a `Box<dyn FnOnce>` + `Arc` clones per task.
- **Persistent per-node argument templates** for stable-arity nodes; disable with
  `TOMII_DISABLE_ARG_TEMPLATES` for same-binary A/B attribution.
- **Unchecked wrapper twins**: the converter emits `_cm_wrap_unchecked` variants (unchecked
  slice access, no per-arg variant matches); at init the runtime selects them only for nodes
  whose argument variants are provable from the graph (static discriminants, `$ref` types,
  predecessor return-variant hints). Disable with `TOMII_DISABLE_UNCHECKED_WRAPPERS`.

### Distribution

- **Free-threaded-first Python**: pyo3 0.26 with `gil_used = false` — wheels for CPython
  3.9–3.14 now include **cp313t and cp314t** (Python kernels no longer serialize on the GIL
  at W>1).
- **macOS / aarch64 targets** enabled by a portable fallback clock for the x86-only
  `utils_rdtsc` paths.

### Bug fixes / internal changes

- **Network packet admission no longer wedges** (#6): out-of-window and no-slot-yet packets
  park in a bounded `pending_frames` buffer and re-inject when the window advances; on
  overflow the furthest-out frame is dropped whole (stream counters advance — degrade,
  never hang). Previously such packets could be silently dropped mid-frame, hanging the
  stream's barrier.
- MIMO bench verifier hardened: sender killed hard (`kill` + `wait`), inter-frame pacing,
  steady-state assertion.
- Python package type-checks clean (mypy 133 → 0); serializer calls pydantic alias kwargs
  correctly; `$network` args raise `ValueError` when `index_function` is missing (the
  runtime requires it).
- Benches prefer the workspace-built binary over the bundled wheel binary.
- `uv.lock` refreshed for the `agent-tuning` extra (optuna).

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

### Distribution

First public release on both registries:

- **PyPI** — `tomii-rt` (import name `tomii`): Linux x86_64 wheels for CPython
  3.9–3.13, plus sdist. macOS / Windows / ARM and free-threaded (3.13t) builds are
  deferred (x86-only `utils_rdtsc` and `pyo3 0.22` respectively).
- **crates.io** — `tomii-core`, `tomii-types`, `tomii-macro`, `tomii-converter`.

### Bug fixes / internal changes

- `WorkerMetrics` is now sized to the actual worker-pool thread count (not the
  requested `--workers`) and the per-task hooks bounds-check the worker index,
  fixing an intermittent `index out of bounds` panic in the scheduler when the
  core allocator grew the pool beyond the requested width.
- Loom test extended to cover `DependencyCounter` concurrent slot completion interleaving.
- Pre-existing clippy lints in `bin/main.rs` fixed (`redundant use`, `last()` → `next_back()`).
- `SchedCtx` borrow bundle lifted to `pub` visibility for strategy implementors.
- `process_batch_resolution` lifted to `pub(crate)` for use from `resolution_strategy.rs`.
- `worker_resolve_successors` lifted to `pub(super)` for delegation from the strategy trait.
