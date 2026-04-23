---
name: graph-coarsen
description: Use when report.json shows overhead_pct > 60% (scheduling dominates compute). Restructures the graph for fewer, larger tasks via tile_size, group_size, or loop coarsening — the primary remedy for scheduling-bound pipelines.
---

# Skill: graph-coarsen

Reduce scheduling overhead by decreasing the number of tasks per stream. This is the
primary remedy when `overhead_pct > 60%` (scheduling dominates compute). Four coarsening
strategies are available; `optimization_suggestions` in `report.json` identifies which
applies.

## Trigger

- [diagnose](diagnose.md) reports `overhead_pct > 60%`
- `optimization_suggestions` contains a priority-1 entry with `category: "graph_topology"`
- User asks to "reduce scheduling overhead" or "coarsen the graph"

## Setup

Read `report.json` and extract:
- `summary.scheduling_overhead_diagnostic.overhead_pct`
- `critical_path.max_node_factor`
- `summary.total_tasks_per_stream`
- `optimization_suggestions[0]` (priority-1 entry: `knob`, `suggested_value`, `action`)

## Decision tree

### Case A — Tile-size coarsening (most common)

**Condition**: `max_node_factor >= 64` AND `overhead_pct > 60%`

The graph creates too many fine-grained tasks. Replace per-element tasks with per-tile tasks.

**Starting tile_size**: use `suggested_value` from `optimization_suggestions[0]` if available,
otherwise start with `tile_size = max_node_factor / 8`.

Follow the [AGENT.md Graph Coarsening Recipe](../AGENT.md):

```python
# Before: one task per element
tile_size = 64  # starting point; tune with --report feedback

for step in range(num_steps):
    width = compute_width(step)
    n_tiles = (width + tile_size - 1) // tile_size
    node = app.node(f"step_{step}", func="your_tile_fn",
                    factor=n_tiles,
                    args=[data, step, ss.usize(tile_size)])
```

The Rust kernel must process a tile internally:

```rust
#[tomii_export]
pub fn your_tile_fn(data: &Vec<f64>, step: usize, tile_size: usize, tile_idx: usize) -> Vec<f64> {
    let start = tile_idx * tile_size;
    let end = (start + tile_size).min(data.len());
    // process data[start..end]
    data[start..end].to_vec()
}
```

> Note: `tile_idx` is the node's instance index, passed automatically by the runtime as
> the last `usize` argument if named `idx` or matching the node's position.
> Check `examples/wavefront-bench/src/lib.rs` for a concrete tile kernel.

**Tile-size sweep**: after the initial run, sweep `tile_size` across `[16, 32, 64, 128]`
and pick the knee of the latency curve (where further doubling gives < 5% improvement).

### Case B — Graph loop restructuring

**Condition**: `max_node_factor < 16` AND `critical_path.length_nodes > 50` AND `overhead_pct > 60%`

The graph structure itself is wrong — it has many short sequential nodes rather than a
few parallel ones. The critical path is long not because of compute but because of the
number of scheduling hops.

Action: restructure the graph loop so that each "step" processes more work. This requires
redesigning the Python graph construction loop, not just changing `factor`. The specific
restructuring depends on the application; see `optimization_suggestions[0].action` for
guidance.

> This case cannot be mechanically applied without understanding the application semantics.
> Read `optimization_suggestions[0].action` carefully — it will describe the required
> structural change.

### Case C — Over-coarsened graph

**Condition**: `max_node_factor < 8` AND `20% < overhead_pct < 60%`

The graph is already coarser than optimal. There is insufficient parallelism to keep all
workers busy. Double the factor (halve tile_size) and re-measure.

```python
tile_size = tile_size // 2  # or double factor directly
```

### Case D — group_size (quick fix, no kernel change)

**Condition**: `max_node_factor >= 8` AND `overhead_pct > 40%` AND you want a quick fix
without modifying the kernel

Apply `group_size` to critical-path nodes. This groups consecutive task instances for
scheduling without changing the graph topology or kernel code:

```python
node = app.node("heavy_node", func="compute",
                factor=width,
                group_size=8,   # groups instances 0-7, 8-15, etc. for scheduling
                args=[...])
```

Typical starting value: `group_size = max(4, workers // 2)`.

> `group_size` is a scheduling hint, not a parallelism change. Each instance still runs
> independently; they are just submitted to the scheduler in batches.

## Applying the change

1. Modify `run_bench.py` per the chosen case above.
2. If Case A or B requires kernel changes, modify `src/lib.rs` (see [plugin-author](plugin-author.md)).
3. Rebuild and re-run:

```python
build_result = app.build(..., clean=False)
app.run(
    dylib=build_result.dylib,
    workers=<your_workers>,
    max_streams=20,
    exclude_streams=5,
    report="report.json",
)
```

4. Read the new `report.json`. Compare:

```
Before: total_tasks_per_stream=X, overhead_pct=Y%, avg_latency_us=Z
After:  total_tasks_per_stream=X', overhead_pct=Y'%, avg_latency_us=Z'
```

## Iteration criteria

| New overhead_pct | Action |
|-----------------|--------|
| < 20% | Compute-dominated now; proceed to [knob-search](knob-search.md) for fine-tuning |
| 20-60% | Mixed; proceed to [knob-search](knob-search.md) |
| > 40% (still high) | Iterate: try halving tile_size, or chain [knob-search](knob-search.md) with `coalesce_barriers=True` |

## Output

```
GRAPH-COARSEN RESULT
====================
Strategy: Case A (tile-size coarsening)

Before: total_tasks=X, overhead_pct=Y%, avg_latency_us=Z
After:  total_tasks=X', overhead_pct=Y'%, avg_latency_us=Z'
Improvement: -W.W%

tile_size used: N
  (sweep results: tile=16 → Zus, tile=32 → Z'us, tile=64 → Z''us ← best, tile=128 → Z'''us)

Modified files:
  run_bench.py: changed factor=width to factor=n_tiles, added tile_size parameter
  src/lib.rs:   added tile_fn kernel processing [start..end] range

Next: [knob-search](knob-search.md) to tune scheduler knobs on the coarsened graph.
```

## See also

- [knob-search](knob-search.md) — follow-up tuning after coarsening
- [plugin-author](plugin-author.md) — if the tile kernel requires non-trivial Rust changes
- [run-validate](run-validate.md) — to measure the coarsened graph with a full stream count
- [AGENT.md](../AGENT.md) — Graph Coarsening Recipe (canonical reference)
