# Τομί (Tomii)

**Task-graph framework for streaming pipelines, MIMO workloads, and agent-tuneable applications.**

Tomii is *not* a general-purpose Taskflow or TBB replacement. For pure single-stream
micro-task DAGs where dispatch overhead dominates, Taskflow is faster (see the benchmark
matrix below). Tomii's advantage is in workloads that benefit from concurrent streams,
generational slot reuse, and structured graph surfaces — particularly MIMO-class
packet-driven pipelines and agent-driven automated optimisation loops.

---

## Performance highlights

| Workload | Tomii vs comparator | Notes |
|---|---|---|
| Public 4×4 MIMO uplink (S=4, W=4) | **1.26× faster than Taskflow** | Packet-overlap advantage; `bench/mimo-bench/` |
| Multi-stream pipeline throughput (S=16, W=4) | **1.33× slower than Taskflow** | Gap closes from 2.45× at S=1; `bench/pipeline-bench/` |
| Slot reuse, N=16,384 | **151× faster than Taskflow eager** | Generational reset vs full re-instantiation |
| Per-slot RSS growth rate (S=1→64, W=4) | **1.6× lower than Taskflow** | Tomii +83 kB/slot vs Taskflow +131 kB/slot; measured via `bench/pipeline-bench/scripts/memory_measure.sh` |
| Anti-diagonal wavefront (single stream) | **~2.4× slower than TBB/Taskflow** | Intrinsic cost of tripartite decoupling |

All numbers are verifier-gated and reproducible from this repository. See
`bench/*/README.md` for methodology and hardware details.

---

## Flagship examples

### Perf #1 — Public 4×4 MIMO uplink benchmark

```bash
cd bench/mimo-bench
# requires Intel MKL, Agora sender (see README)
python tomii/run_bench.py --workers 4 --slots 4 --streams 200
python taskflow/run_bench.py --workers 4 --slots 4 --streams 200
```

Tomii dispatches FFT tasks as each UDP packet arrives; Taskflow must collect all packets
before submitting the full DAG. At ~17.9 µs/packet spacing over a ~1 ms frame, this
recovers 280–360 µs that Taskflow cannot — a structural advantage, not a tuning accident.

### Perf #2 — Multi-stream pipeline S-scaling

```bash
cd bench/pipeline-bench
python tomii/run_bench.py --workers 4 --slots 16 --streams 200
python taskflow/run_bench.py --workers 4 --slots 16 --streams 200
```

Gap closes from 2.45× (S=1) to 1.33× (S=16) as multi-slot amortisation takes effect.
Per-slot RSS growth is 1.6× lower than Taskflow (+83 vs +131 kB/slot). Full S×W sweep in
`bench/pipeline-bench/results/pipeline_sweep_post_u7c.csv`.

Tomii runs use `--custom --coalesce-barriers --inline-continuation` (hardcoded in `run_bench.py`);
these are the recommended flags for streaming workloads. Taskflow uses default `tf::Executor`.

### Ergonomics #1 — Agent-native graph tuning

```bash
cd examples/agent-tuning
bash run_all.sh 50   # runs all 4 arms (random, Bayesian, grid, Claude)
```

Four optimisation arms compete over the stream-analytics knob space with the same budget
(50 iterations) and verifier. Measured result: all arms 50/50 verifier-passing; agent mean
latency 0.33 ms vs random mean 14.5 ms — the agent converges efficiently without being
given source code or documentation. An edit that drops a barrier or removes a `$dep` edge
fails the verifier and is rejected. See `examples/agent-tuning/README.md`.

### Ergonomics #2 — Polyglot plugin showcase

The same DAG (FFT + matrix compute) runs with Rust, C, and Python kernels:

```bash
python examples/matrix-compute/run_bench.py --workers 4
python examples/matrix-compute-C/run_bench.py --workers 4
python examples/matrix-compute-python/run_bench.py --workers 4
```

No source changes to the runtime — the plugin boundary is a C ABI, language-agnostic by
design. See `examples/README.md` for the full capability matrix.

---

## Python API (recommended)

```python
import tomii as tm

app = tm.Graph()

buf_size    = app.var("buf_size", 100)
fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])

gen_vec     = app.node("gen_vec",     func="generate_vector", factor=200, args=[buf_size])
compute_fft = app.node("compute_fft", func="compute_fft",     factor=200,
                       args=[fft_planner, gen_vec.out()])
vec_mat     = app.node("vec_mat",     func="vec_to_mat",      factor=200,
                       args=[gen_vec.out(), compute_fft.wait()])

app.build(func_path="plugin/src/lib.rs", plugin_manifest="plugin/Cargo.toml")
app.run(workers=4, slots=2, timing="timing.csv")
```

### Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

### Graph API

| Method | Description |
|---|---|
| `app.var(name, value)` | Constant initialization |
| `app.var(name, func=..., args=[...])` | Computed initialization |
| `app.node(name, func=..., args=[...], factor=...)` | Computation node; `factor` = parallel instances |
| `app.post_node(...)` | Post-computation cleanup node |
| `node.out(i)` | Data dependency on instance `i` (`$res`) |
| `node.wait(i)` | Barrier — wait for instance `i` (`$barrier`) |
| `('dep', node)` | Ordering-only dependency, no output (`$dep`) |
| `app.network(**cfg)` | Configure UDP/TCP packet-driven dispatch |
| `app.to_json()` | Export graph to JSON |

### Type system

```python
tm.i32(-5) / tm.f32(3.14) / tm.String("hello") / tm.bool_(True)
tm.Complex64(1.0, -0.5) / tm.Vec("f32", [1.0, 2.0, 3.0])
```

Python `int` → `usize`, `float` → `f64` by default.

### Loops and conditions

```python
loop_node = app.node("proc", func="process", factor=200,
                     loop=Loop("iter", factor=loop_factor))

cond_node = app.node("filter", func="filter_fn", factor=200,
                     condition=Condition(operation="Eq", value=1, value_type="usize",
                                        func="check_fn", args=[some_var]))
```

### Build options

```python
app.build(func_path="plugin/src/lib.rs",      # Rust source — auto-generates wrappers
          # or func_path="include/plugin.h",  # C header with // @tomii_export annotations
          plugin_manifest="plugin/Cargo.toml",
          release=True)
```

### Run options

```python
app.run(workers=8, system_threads=2, slots=4, max_streams=100,
        timing="timing.csv", report="report.json",
        inline_continuation=True, coalesce_barriers=True)
```

---

## Agent-native interfaces

Tomii exposes structured discovery and diagnostic interfaces designed for LLM agents.

### Discovery

```bash
python -m tomii --list-knobs           # all graph.run() options
python -m tomii --list-knobs-json      # machine-readable JSON with search hints
python -m tomii --schema               # JSON schema for the graph construction API
```

### Structured performance report

Pass `report="report.json"` to `app.run()` for a JSON performance report after each run:

| Key | Description |
|---|---|
| `summary.avg_latency_us` / `p50` / `p99` | Stream latency statistics |
| `summary.throughput_streams_per_sec` | End-to-end throughput |
| `summary.scheduling_overhead_diagnostic` | `overhead_pct`, `overhead_us`, interpretation |
| `per_node` | Per-node avg/p99 exec time, `on_critical_path` flag |
| `optimization_suggestions` | Prioritised list: category, action, knob, estimated speedup |

### Agent Skills

[`SKILLS/`](SKILLS/) contains structured workflow skills covering the full optimisation
lifecycle — from project discovery to graph coarsening:

| Skill | Purpose |
|---|---|
| `project-discover` | Orient in an unknown project: topology, knob inventory, baseline |
| `graph-build` | Translate a computation description into a graph + plugin stubs |
| `run-validate` | Build, verify correctness, establish baseline |
| `diagnose` | Classify bottleneck (scheduling / compute / imbalance) |
| `knob-search` | 5-iteration search over scheduler knobs using per-knob hints |
| `graph-coarsen` | Reduce task count when `overhead_pct > 60%` |
| `plugin-author` | Write correct `#[tomii_export]` Rust/C plugin functions |

```bash
./SKILLS/install-skills.sh            # installs to .claude/skills/ in CWD
```

---

## Graph visualisation

```bash
python -m tomii --visualize examples/stream-analytics/graph.json       # browser view
python -m tomii --visualize examples/stream-analytics/graph.json --edit # browser edit
python -m tomii --visualize graph.json --ascii                          # terminal ASCII
```

The web UI renders a colour-coded DAG (Dagre layout): green for compute nodes,
orange-bordered for conditional, gray for post-nodes. Edges are styled by type
(`$res` solid blue, `$dep` dashed, `$barrier` thick orange). **Export Python** downloads a
ready-to-run script from the current graph.

---

## JSON + CLI workflow

```
cargo run -p tomii-core --bin main -- \
  --json graph.json --dylib plugin.so \
  --workers 4 --slots 2 --max-streams 100
```

Key flags: `--workers`, `--slots`, `--system-threads`, `--timing`, `--report`,
`--fifo`, `--custom`, `--inline-continuation`, `--coalesce-barriers`,
`--resolution-strategy multi-slot-batch`.

Full flag list: `cargo run -p tomii-core --bin main -- --help`

---

## Pluggable scheduler

Implement the `TaskScheduler` trait (defined entirely in `tomii-types` — no `tomii-core`
dependency required) and load it at runtime:

```bash
cargo build --release -p scheduler-plugin
cargo run -p tomii-core --bin main -- \
  --scheduler-plugin target/release/libscheduler_plugin.so \
  --json graph.json --dylib plugin.so
```

See `examples/scheduler-plugin/` for a minimal FIFO example and
`tomii-core/PLUGIN_SCHEDULER_API.md` for the stability contract.

---

## Architecture

**Workspace crates:**
- `tomii-core` — runtime, scheduler, graph engine, network receiver
- `tomii-types` — `CmTypes` enum + stable scheduler API types (`SchedulerPriority`, `SchedulerWorkerRange`, `CoreSpec`)
- `tomii-converter` — code-generation: wraps Rust/C plugin headers into `wrappers.rs`/`reg.rs`
- `tomii-macro` — procedural macros for plugin wrapping (WIP)
- `tomii/` — Python API package

**Key runtime modules (`tomii-core/src/runtime/`):**
- `resolution_loop.rs` — main resolution loop, batch draining
- `batch_resolution.rs` — four-phase batch processing (Phases 1–3)
- `resolution_strategy.rs` — `ResolutionStrategy` trait + `MultiSlotBatchStrategy`
- `task_execution.rs` — worker task execution, inline-continuation fast path
- `successor.rs` — successor collection and dependency propagation
- `shared_data.rs` — `SharedData`, `SlotData`, borrow bundles
- `slot_lifecycle.rs` — slot completion detection
- `ARCHITECTURE.md` — detailed threading model, memory ordering, invariants

**Threading model:**
- Worker threads (Rayon or custom pool) with CPU affinity — kernel execution
- System thread(s) — dependency propagation and slot lifecycle
- Network receiver threads — low-latency UDP/TCP packet ingestion (feature `network`)
- Async recorder thread — non-blocking timing output

See `tomii-core/src/runtime/ARCHITECTURE.md` for the performance envelope (intrinsic costs,
target workload classes, and workloads Tomii explicitly does not target).

---

## Build

```bash
cargo build --release
make schema   # regenerate Python bindings after changing json_structs.rs
```

> **Note for `bench/mimo-bench/` builds:** the MIMO bench links Intel MKL and Agora libs.
> Run `source examples/mimolib/scripts/export.sh` before building to set the required
> library paths. See `bench/mimo-bench/README.md` for the full dependency list.

## Environment variables

- `FUNC_PATH` — path to plugin header or Rust source (required for tomii-converter)
- `WRAP_PATH` / `REG_PATH` — wrapper/registry files (optional, auto-generated)

---

## Roadmap

See [ROADMAP.md](ROADMAP.md). v1.1 planned items include frozen-graph specialisation (M1),
successor table flattening (A2), agent-tuning expansion to MIMO and pipeline workloads, and
a formal attempt to reduce `remaining_deps` SeqCst → AcqRel.

## License

[Apache License 2.0](LICENSE)
