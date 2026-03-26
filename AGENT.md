# SynStream Agent Quick-Reference

## Plugin functions

Annotate pure Rust functions with `#[synstream_export]` — the build system generates all FFI
bridging automatically (no `wrappers.rs` or `reg.rs` required).

```rust
use synstream_macro::synstream_export;

#[synstream_export]
pub fn init_data(n: usize) -> Vec<f64> { ... }

#[synstream_export]
pub fn process_item(data: &Vec<f64>, idx: usize) -> f64 { ... }
```

**When NOT to use `#[synstream_export]`**: functions that mutate shared state via raw pointers
(e.g. `with_any` → `*mut T`). Write those as `#[no_mangle] pub fn foo_cm(args: &[CmTypes]) -> CmTypes`
and extract arguments manually. See `synstream-types/` for `CmTypes` definition.

## Cargo.toml template

```toml
[package]
name = "my-plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["dylib", "rlib"]

[dependencies]
synstream-types = { path = "../../synstream-types" }
synstream-macro = { path = "../../synstream-macro" }
```

## Python script skeleton

```python
import synstream as ss
from synstream._builder import find_workspace_root
from pathlib import Path

HERE = Path(__file__).resolve().parent
_TARGET_DIR = str(find_workspace_root() / "target")

graph = ss.Graph()
# ... build graph with graph.var() and graph.node() ...

build_result = graph.build(
    func_path=str(HERE / "src" / "lib.rs"),
    plugin_manifest=str(HERE / "Cargo.toml"),
    env={"CARGO_TARGET_DIR": _TARGET_DIR},
    release=True,
    clean=False,
)

graph.run(
    dylib=build_result.dylib,
    workers=4,
    max_streams=10,
)
```

## Key `graph.run()` flags

| Flag | What it does |
|------|-------------|
| `workers=N` | Rayon worker threads |
| `core_offset=1` | Pin workers starting at CPU 1 |
| `slots=N` | Concurrent in-flight streams |
| `max_streams=N` | Total streams to process |
| `timing="timing.csv"` | Write per-node timing CSV |
| `report="report.json"` | Write JSON performance report (per-node stats, critical path, bottleneck hints) |

Run `python -m synstream --list-knobs-json` for all `graph.run()` options with search hints (machine-readable JSON).
Run `python -m synstream --schema` for graph construction parameters (node options, arg types).

## Performance Model

### Reading `report.json`

The two most useful fields for directing optimization effort:

```
summary.scheduling_overhead_diagnostic.overhead_pct   # % of latency that is scheduling, not compute
summary.scheduling_overhead_diagnostic.interpretation  # plain-English diagnosis
optimization_suggestions                               # ranked list: what to change and why
critical_path.max_node_factor                          # highest factor on critical path
summary.total_tasks_per_stream                         # total tasks spawned per stream
```

**Decision rule**: if `overhead_pct > 60%`, fix graph topology first (tile_size / group_size).
If `overhead_pct < 20%`, fix the kernel. In between, try scheduling knobs first
(`coalesce_barriers`, `batching_size`, `inline_continuation`).

> **Warning:** `coalesce_barriers=True` groups tasks into bulk batches and suppresses
> `total_tasks_per_stream` in subsequent `report.json` (field will be `null`).
> Apply it only after graph structure is confirmed correct — i.e. `optimization_suggestions`
> is empty or contains only low-priority entries.

### Graph Coarsening Recipe

When `overhead_pct` is high, the graph has too many fine-grained tasks. Reduce task count
by grouping work units into tiles — replace one task per unit with one task per tile:

```python
# Before: factor = full width (many small tasks, high scheduling overhead)
for step in range(num_steps):
    width = compute_width(step)          # e.g. cells in this diagonal / row / chunk
    node = graph.node(f"step_{step}", func="your_unit_fn",
                      factor=width, args=[data, step])

# After: factor = ceil(width / tile_size)  (fewer, larger tasks, lower overhead)
tile_size = 64                           # start here; tune based on report feedback
for step in range(num_steps):
    width = compute_width(step)
    n_tiles = (width + tile_size - 1) // tile_size
    node = graph.node(f"step_{step}", func="your_tile_fn",
                      factor=n_tiles, args=[data, step, tile_size])
```

The Rust kernel for `your_tile_fn` receives the tile index and `tile_size` and loops over
`[index * tile_size, min((index+1) * tile_size, width))` internally.

**Choosing tile_size**: use `critical_path.max_node_factor` from `report.json` as the
current per-node factor. Target `tile_size = max_node_factor / 8` as a starting point,
then sweep 16 → 32 → 64 → 128 and pick the knee of the latency curve.

### Applying `optimization_suggestions`

Each entry has `priority`, `knob`, `action`, and `estimated_speedup`. Apply priority-1 first,
rebuild, and re-read the report before applying lower-priority suggestions — coarsening often
makes runtime-flag suggestions obsolete.
