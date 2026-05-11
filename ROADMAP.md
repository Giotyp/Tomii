# Roadmap

## v1.0 (current release)

Core runtime, ergonomics, and flagship benchmarks — see [CHANGELOG.md](CHANGELOG.md) for details.

**Shipped:**
- Four-phase batch-resolution protocol with generational slot reuse
- Pluggable `TaskScheduler` trait (stable API in `tomii-types`)
- `ResolutionStrategy` trait (`MultiSlotBatchStrategy` is the only v1 implementation)
- `DependencyCounter` trait (renamed from `ResolutionState`; backward-compat shim retained)
- Python graph API with `--list-knobs-json` / `--schema` / `--report` structured interfaces
- Agent-tuning example: 4-arm search over stream-analytics (random, Bayesian, grid, Claude)
- Polyglot plugin showcase: Rust, C, Python kernels on one DAG
- `bench/mimo-bench/` — public 4×4 MIMO uplink, Tomii vs Taskflow, 1.26–1.39× Tomii win
- `bench/pipeline-bench/` — S-scaling pipeline, gap closes from 2.45× at S=1 to 1.33× at S=16

---

## v1.1 — Planned

### M1: Frozen-graph specialisation

When the graph topology is statically known before the first slot, pre-flatten the successor
table into a compact `Vec<[u32; MAX_SUCC]>` structure and eliminate the `Arc<Graph>` indirection
on every dependency decrement.

**Prerequisite:** `ResolutionStrategy` trait (landed in v1.0).  
**Expected gain:** ~5–8% on wavefront workloads; larger on anti-diagonal (`~2.4×` gap vs TBB is
primarily dispatch overhead, not successor-table layout, but any reduction helps).  
**Files:** `tomii-core/src/runtime/node_cache.rs`, `successor.rs`, new `frozen_graph.rs`.

### A2: Successor table flattening

Compress the per-node `Vec<SuccessorEntry>` into a single arena allocation with index-range
slices. Reduces cache misses during Phase 3 successor collection at high factor counts.

**Prerequisite:** M1 (same data-layout change).

### K-way SeqCst → AcqRel proof attempt

`remaining_deps[g]` at `buffers/node_dep.rs` uses `SeqCst` for streaming-correct barrier
semantics across concurrent slots. Bugs #14/#18–20 (see `notes/antidiag-overhead.md`) were
caused by premature relaxation of this ordering. Attempt reduction to `AcqRel` only with a
formal memory-model argument (loom model + hand-proof).

**Risk:** High correctness risk. Do not relax without loom confirmation.

### Agent-tuning expansion

Extend `examples/agent-tuning/` to MIMO and pipeline workloads with the same 4-arm
methodology, same threshold rules, and verifier-gated perf recording. Publish the
combined results table.

### Phase 0 benchmark matrix completion

Rows 4 (Timely-Rust iterative dataflow) and rows 1/3/7 (TBB ports) from the §2.1 matrix in
`tomii-focus.md`. Rows 1, 2, 5, 6 are already covered by `bench/`.

### Additional runtime knobs

- `WorkerHook` — per-worker init/shutdown callbacks (useful for NUMA-local allocation setup)
- `run_until(predicate)` — early-termination variant of the main run loop
- `tomii dump` CLI — serialise the current runtime state (slot occupancy, pending counts,
  dependency graph snapshot) for offline debugging

---

## Out of scope (architectural limits, not bugs)

These are intrinsic costs of tripartite decoupling. They are documented honestly rather than
planned away:

- **`parallel_for` reduction**: Tomii cannot express data-dependent fan-in without a static
  topology. TBB wins here by design.
- **Sub-µs micro-task DAGs**: the resolution-thread state machine adds ~5 ms/stream fixed cost
  at S=1. Taskflow is faster for single-stream fine-grained work.
- **Dynamic-topology DAGs** (e.g. Timely-style incremental dataflow): Tomii's JSON topology
  is fixed at launch. Re-entering the graph mid-stream requires a slot reset.
- **Folding `node_results.set` + `AsyncRecorder` into `ResolutionStrategy`**: would enlarge
  the trait surface significantly. Left as a v1.2 consideration.
