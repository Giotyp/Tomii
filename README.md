# SynStream

### Task-Graph Framework for Streaming Applications

SynStream automates the process of describing a computational graph and executing it in a specified environment. It focuses on streaming applications, which require low-latency and data-reuse between computation stages in a consumer-producer MIMO pattern.

## How to Use

There are two ways to define and run a SynStream application: the **Python API** (recommended) and the **JSON + CLI** workflow.

---

## Python API (Recommended)

The `synstream` Python package lets you define graphs in code, build the plugin library, and launch the runtime — all from a single Python script.

### Install

```bash
# From the workspace root
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Quick Start

```python
import synstream as ss

app = ss.Graph()

# Initializations (pre-computed objects shared across the graph)
buf_size    = app.var("buf_size", 100)
num_nodes   = app.var("num_nodes", 200)
fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])

# Computation nodes: factor= creates parallel instances
gen_vec     = app.node("gen_vec",     func="generate_vector", factor=num_nodes,
                       args=[buf_size])
compute_fft = app.node("compute_fft", func="compute_fft",     factor=num_nodes,
                       args=[fft_planner, gen_vec.out()])   # $res dependency
vec_mat     = app.node("vec_mat",     func="vec_to_mat",      factor=num_nodes,
                       args=[gen_vec.out(), compute_fft.wait()])  # $barrier sync

# Build plugin + runtime, then execute
app.build(wrap_path="wrappers.rs", reg_path="reg.rs",
          plugin_manifest="plugin/Cargo.toml")
app.run(workers=4, slots=2, timing="timing.txt")
```

Or combine into one call:

```python
app.build_and_run(wrap_path="wrappers.rs", reg_path="reg.rs",
                  plugin_manifest="plugin/Cargo.toml",
                  workers=4, slots=2, timing="timing.txt")
```

### Graph API

| Method | Description |
|---|---|
| `app.var(name, value)` | Constant initialization (e.g. `buf_size = app.var("buf_size", 100)`) |
| `app.var(name, func=..., args=[...])` | Computed initialization (calls a plugin function at startup) |
| `app.node(name, func=..., args=[...], factor=...)` | Computation node; `factor` creates parallel instances |
| `app.post_node(...)` | Post-computation node (runs after the main graph completes) |
| `node.out(i)` | Data dependency on instance `i` of a predecessor (`$res`) |
| `node.wait(i)` | Barrier — wait for instance `i` to complete before proceeding (`$barrier`) |
| `('dep', node)` | Ordering-only dependency — wait for `node` to complete without consuming its output (`$dep`) |
| `app.network(**cfg)` | Configure UDP/TCP packet injection for network-driven graphs |
| `app.to_json()` / `app.save_json(path)` | Export graph to JSON without building |

### Type System

Python literals are auto-inferred (`int` → `usize`, `float` → `f64`). Use explicit wrappers for other types:

```python
import synstream as ss

ss.i32(-5)
ss.f32(3.14)
ss.String("hello")
ss.bool_(True)
ss.Complex64(1.0, -0.5)
ss.Vec("f32", [1.0, 2.0, 3.0])
```

### Loops and Conditions

```python
from synstream import Loop, Condition

# Loop node: iterates `loop_factor` times
loop_node = app.node("proc", func="process", factor=num_nodes,
                     loop=Loop("iter", factor=loop_factor))

# Conditional node: skips execution based on a plugin predicate
cond_node = app.node("filter", func="filter_fn", factor=num_nodes,
                     condition=Condition(
                         operation="Eq", value=1, value_type="usize",
                         func="check_fn", args=[some_var]
                     ))
```

### Build Options

```python
app.build(
    func_path="plugin/src/lib.rs",     # Auto-generate wrappers from Rust source
    # -- OR --
    func_path="include/plugin.h",      # Auto-generate wrappers from annotated C header
    # -- OR --
    wrap_path="wrappers.rs",           # Use pre-generated wrapper files
    reg_path="reg.rs",
    plugin_manifest="plugin/Cargo.toml",
    release=True,                       # Release build (default)
    clean=False,                        # Skip cargo clean (faster rebuilds)
)
```

### Run Options

All CLI flags are available as keyword arguments (underscores replace hyphens):

```python
app.run(
    workers=8,
    system_threads=2,
    slots=4,
    max_streams=100,
    max_runtime=60,
    timing="timing.csv",
    report="report.json",
    slot_priority=True,
    exclude_streams=5,
    debug=False,
)
```

### Real-World Examples

`examples/matrix-compute/run_bench.py` — Rust plugin (FFT + matrix multiply):

```bash
python examples/matrix-compute/run_bench.py --workers 4 --no-clean
```

`examples/matrix-compute-C/run_bench.py` — Same DAG backed by a C library (FFTW + OpenBLAS). Point `func_path` at the annotated C header and the converter generates `libloading`-based wrappers automatically:

```bash
python examples/matrix-compute-C/run_bench.py --workers 4 --no-clean
```

C functions are exported by annotating declarations in the header with `// @synstream_export`:

```c
// @synstream_export
void* fft_planner(size_t buf_size);

// @synstream_export(out_len=n, free=free_matrix)
complex_f32* generate_vector(size_t n);
```

---

## Agent-Native

SynStream exposes structured discovery and diagnostic interfaces designed for LLM agents and automated optimization loops.

### Discovery commands

```bash
python -m synstream --list-knobs          # human-readable list of all graph.run() options
python -m synstream --list-knobs-json     # machine-readable JSON with search hints per knob
python -m synstream --schema              # JSON schema for the full graph construction API
```

`list_knobs()` and `list_knobs_json()` are also available as Python functions:

```python
import synstream as ss
print(ss.list_knobs())
```

### Structured report

Pass `report="report.json"` to `graph.run()` (or `--report` on the CLI) to emit a JSON performance report after each run:

```python
app.run(workers=8, slots=1, report="report.json")
```

Top-level keys:

| Key | Description |
|-----|-------------|
| `summary.avg_latency_us` / `p50` / `p99` / `std_dev` | Stream latency statistics |
| `summary.throughput_streams_per_sec` | End-to-end throughput |
| `summary.total_tasks_per_stream` | Total scheduled tasks per stream |
| `summary.scheduling_overhead_diagnostic` | `overhead_pct`, `overhead_us`, `critical_path_exec_us`, `interpretation` |
| `per_node` | Per-node avg/p99 exec time, invocation count, `on_critical_path` flag |
| `critical_path` | Estimated serial latency, node count, `max_node_factor` |
| `resource_utilization.worker_busy_pct` | Per-worker utilization % |
| `bottleneck_hints` | Free-text hints for the top bottleneck nodes |
| `optimization_suggestions` | Structured list of prioritized suggestions (see below) |

### Optimization suggestions

`optimization_suggestions` is an array of objects, each with:

```json
{
  "priority": 1,
  "category": "graph_topology",
  "description": "...",
  "action": "...",
  "knob": "tile_size",
  "suggested_value": 32,
  "estimated_speedup": "4–8x",
  "confidence": "high"
}
```

Categories: `graph_topology`, `runtime_flags`, `parallelism`. The suggestions fire based on `overhead_pct` thresholds and guide agents toward graph coarsening, `coalesce_barriers`, `batching_size`, or parallelism adjustments.

### Agent Skills

The [`SKILLS/`](SKILLS/) folder contains structured workflow skills for AI agents — covering
the full optimization lifecycle from project discovery to graph coarsening:

| Skill | Purpose |
|-------|---------|
| `project-discover` | Orient in an unknown project: topology, plugins, knob inventory, baseline |
| `graph-build` | Translate a computation description into a Python graph + plugin stubs |
| `run-validate` | Build, verify correctness (single-worker first), establish baseline |
| `diagnose` | Classify bottleneck from `report.json` (scheduling / compute / imbalance) |
| `knob-search` | 5-iteration search over scheduler knobs using per-knob search hints |
| `graph-coarsen` | Reduce task count via tile_size or group_size when overhead_pct > 60% |
| `plugin-author` | Write correct `#[synstream_export]` Rust/C plugin functions |

Each skill is a self-contained markdown file with Claude Code SKILL.md frontmatter. To
install them as Claude Code slash commands in your project:

```bash
./SKILLS/install-skills.sh            # installs to .claude/skills/ in CWD
./SKILLS/install-skills.sh /your/dir  # installs to a specific project directory
```

---

## JSON + CLI Workflow

For environments without Python, or when exporting a graph for external tools:

1. Describe the application using a JSON file (or export one via `app.save_json()`).

2. Obtain (or create) a plugin library compatible with SynStream (dynamic `.so` file and header file or Rust source file).

3. Set the **FUNC_PATH** environment variable to the header or Rust source path.

4. Execute SynStream:

```
Usage: main [OPTIONS] --json <FILE> --dylib <FILE>

Options:
      --json <FILE>                         Task graph definition (required)
      --dylib <FILE>                        Plugin library (.so) (required)
      --workers <CORES>                     Worker thread count [default: 1]
      --core-offset <CORE_OFFSET>           CPU affinity start index [default: 1]
      --system-threads <SYSTEM_THREADS>     Resolution threads [default: 1]
      --receiver-threads <RECEIVER_THREADS> Network receiver threads [default: 1]
      --slots <SLOTS>                       Concurrent stream slots [default: 1]
      --max-streams <MAX_STREAMS>           Streams to process [default: 1]
      --max-runtime <MAX_RUNTIME>           Timeout in seconds, 0 = unlimited [default: 0]
      --batching-size <BATCHING_SIZE>       Tasks per batch [default: 1]
      --batching-limit <BATCHING_LIMIT>     Max batch wait time in microseconds [default: 10]
      --timing <FILE>                       CSV timing output file
      --record                              Enable scheduler event recording
      --record-stream <STREAM_ID>           Record only a specific stream
      --exclude-streams <EXCLUDE_STREAMS>   Exclude N initial streams from timing statistics [default: 0]
      --slot-priority                       Sequential slot processing with round-robin for cache locality
      --fifo                                Enable FIFO scheduler
      --custom                              Enable custom lock-free priority scheduler
      --use-rdtsc                           Use hardware RDTSC for timing
      --inits                               Print initializations to stdout
      --debug                               Enable debug printing
      --output <FILE>                       Redirect stdout [default: stdout]
  -h, --help                                Print help
  -V, --version                             Print version
```

## Architecture

**Workspace crates:**
- `synstream-core` — Runtime, scheduler, graph engine, and network receiver infrastructure
- `synstream-types` — `CmTypes` enum for type-erased value passing across plugin boundaries
- `synstream-converter` — Code-generation library; produces `wrappers.rs`/`reg.rs` from annotated Rust source or C headers (`.h`/`.hpp`)
- `synstream-macro` — Procedural macros for plugin wrapping (WIP)
- `synstream/` — Python API package
- `examples/matrix-compute` — FFT and matrix computation benchmark (Rust plugin)
- `examples/matrix-compute-C` — Same DAG using a C plugin (FFTW + OpenBLAS via annotated C header)
- `examples/mimolib` — MIMO streaming benchmark

**Core modules in `synstream-core/src`:**
- `runtime/` — Main execution orchestration split into focused submodules: `mod.rs`, `init.rs`, `threading.rs`, `scheduling.rs`, `task_execution.rs`, `batch_resolution.rs`, `packet_processing.rs`, `slot_lifecycle.rs`, `slot_management.rs`, `successor.rs`, `arg_resolution.rs`, `node_cache.rs`, `shared_data.rs`, `thread_locals.rs`, `resolution_loop.rs`, `reporting.rs`, `network_init.rs`
- `scheduler.rs` — Unified `RayonScheduler` (work-stealing + FIFO modes); `custom_scheduler/` for the lock-free priority scheduler
- `graph.rs` / `graph_gen.rs` — DAG representation, JSON parsing, dependency resolution
- `network.rs` / `network_funcs.rs` — Dedicated receiver threads for UDP/TCP packet injection (feature-gated: `--features network`)
- `buffers/` — Per-slot, per-node result storage: `node_dep.rs` (atomic threshold spawning), `node_info.rs`, `result_map.rs` (lock-free result storage)
- `resolution_state.rs` — Multi-threaded atomic dependency tracking
- `time_buffer/` — Telemetry collection and JSON report generation
- `worker_range.rs` — Worker CPU affinity configuration
- `async_recorder.rs` — Lock-free timing and event recording

**Threading model:**
- Worker threads (Rayon or custom pool) with CPU affinity for kernel execution
- System (resolution) threads for dependency checking and task scheduling
- Dedicated network receiver threads for low-latency packet ingestion (requires `network` feature)
- Async recorder thread for non-blocking timing output

## JSON Graph Format

Graphs define `initializations` (pre-computed objects) and `nodes` (tasks). Key argument types:
- `$ref` — Reference to an initialized object
- `$res` — Result from a predecessor node (data dependency)
- `$dep` — Ordering-only dependency (wait for completion, no output consumed)
- `$barrier` — Wait for all predecessor instances
- `$network` — Network packet injection marker

The `factor` field creates parallel node instances. Network nodes are handled by receiver threads and excluded from the task scheduler's dependency counters.

## Environment Variables

- `FUNC_PATH` — Path to plugin header or Rust source (required)
- `WRAP_PATH` — Wrapper functions file (optional, auto-generated)
- `REG_PATH` — Function registry file (optional, auto-generated)

## Build

```bash
# Setup environment (required before building)
source examples/mimolib/scripts/export.sh

# Build entire workspace
cargo build

# Optimized build
cargo build --release

# Quick compilation check
cargo check --lib

# Regenerate Python bindings after changing json_structs.rs
make schema
```

## License

[Apache License 2.0](LICENSE)