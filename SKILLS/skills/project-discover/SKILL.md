---
name: project-discover
description: "Use when first dropped into an unknown Tomii project directory. Maps graph topology, plugin functions, tunable knob inventory, and prior performance baseline — run this before any other skill."
---

# Skill: project-discover

Orient an agent in an unknown Τομί project. Produces a structured project summary
containing the graph topology, plugin catalog, tunable knob inventory, and performance
baseline (if a prior run exists). Run this skill first before any other skill.

## Trigger

- Dropped into an unknown Τομί project directory
- Asked "what does this project do?"
- Starting a new optimization or development session

## Steps

### 1. Locate entry points

Search for the Python graph builder and the graph JSON:

```bash
find . -name "run_bench.py" -o -name "build_graph.py" -o -name "graph.json" -o -name "*.sh" | head -20
```

Read them. Also read any run script (`run.sh`, `run_bench.sh`): it holds the runtime flags
actually used. From the graph, extract:
- Node names, `factor`, `group_size`, `priority`, and dependency types (`$res`, `$barrier`, `$dep`)
  with their `indexes` and `group_by`
- Initialization variables (`initializations` / `app.var`) and which ones are used as factors
- Whether args pass the instance index (`{"type": "$ref", "value": "$index"}`)
- Whether the graph has `network_config` (network-input workload — see [network-ingest](../network-ingest/SKILL.md))
- Whether it uses `condition`, `loop`, `post_nodes`

To see the topology quickly:

```bash
python -m tomii --visualize graph.json --ascii      # or: python -m tomii --dump graph.json > g.dot
```

### 2. Load the knob inventory

```bash
python -m tomii --list-knobs-json             # all graph.run() options
python -m tomii --knob-space graph.json       # the search space for this graph
```

`--list-knobs-json` returns `{"version", "knobs": [...]}`. For each knob, record `name`,
`type`, `cli`, `role` (`perf` / `env` / `io` / `measurement`), `description`, `search_hint`
and `domain` (absent for knobs that are settable but not auto-searched). Only `perf` knobs
change performance; `measurement` knobs (`max_frames`, `exclude_frames`) must stay fixed
across a comparison. Hints are consumed by [knob-search](../knob-search/SKILL.md).

### 3. Load the graph construction schema

```bash
python -m tomii --schema
```

Node parameters (`factor`, `group_size`, `priority`, `condition`, `loop`, `use_workers`) and
argument types (`$res`, `$barrier`, `$dep`, `$ref`) with `optimization_hint` fields.
Cross-check examples against `website/docs/reference/json-graph-format.md` and the
Python API section of [graph-build](../graph-build/SKILL.md).

### 4. Catalog plugin functions

Find the plugin source. It is referenced by `func_path=` in `graph.build(...)` calls (or by
`--dylib` in a run script) — typically `src/lib.rs` (Rust), a `.h` file (C), or a Python
module (`@tomii.export`).

For **Rust plugins**: search for `#[tomii_export]` annotations:
```bash
grep -n "tomii_export" src/lib.rs
```

For each exported function, record: function name, parameter types, return type, and the
attribute: `variadic` (last parameter `Vec<T>` collects many predecessor instances) or
`bulk` (also runs a range of instances per call).

Check `Cargo.toml` for a `dynamic-registration` feature and `build.rs` for
`generate_self_contained_file`: such a plugin describes its own kernels
(`__tomii_exports`) and can be loaded without rebuilding the runtime (see
[plugin-author](../plugin-author/SKILL.md)).

For **C plugins**: search for `// @tomii_export` annotations:
```bash
grep -n "@tomii_export" include/*.h
```

For **Python kernels**: search for `@tomii.export` / `@tm.export` and `app.py_node(...)`.

### 5. Check for existing performance baseline

```bash
find . -name "report*.json" | head -5
```

If found, read and extract:
- `summary.avg_latency_us`, `p50_latency_us`, `p99_latency_us`, `p999_latency_us`,
  `throughput_frames_per_sec`, `total_frames`, `total_tasks_per_frame`
- `summary.stale_task_drops` (must be 0; non-zero means work was dropped)
- `summary.scheduling_overhead_diagnostic.overhead_pct` and `interpretation`
- `critical_path.max_node_factor`, `critical_path.length_nodes`
- `optimization_suggestions` (ranked list of recommended actions)

Check the report's age against the graph and plugin files: an old report describes an old
configuration.

### 6. Check for existing timing data

```bash
find . -name "timing*.txt" -o -name "timing_sched.csv" | head -5
```

If `timing_sched.csv` exists (written with `record=True`), analyze it with:
```bash
python3 scripts/analyze_sched.py timing_sched.csv --system-threads 1 --slots 1
```

### 7. Produce structured summary

```
PROJECT SUMMARY
===============
Entry point: <path to run_bench.py / build_graph.py / graph.json / run script>

Graph topology:
  Nodes: <list: name (factor=N, group_size=G, priority=P)>
  Dependencies: <key edges: predecessor → successor (type, indexes, group_by)>
  Network input: yes/no  (sockets, packets per frame, receiver flags in use)
  Conditions / loops / post-nodes: yes/no

Plugin: <path>  (Rust | C | Python; static | dynamic-registration)
  Functions: <list: fn_name(arg_types) -> return_type [variadic|bulk]>

Knobs: <count> (perf: <count>); current run flags: <from run script>

Performance baseline:
  <If report.json exists:>
    avg / p50 / p99 latency_us: N / N / N
    overhead_pct: N% (<interpretation>)
    stale_task_drops: N
    Top optimization_suggestions: <priority-1 entries>
  <If no report.json:>
    No baseline found. Run [run-validate](../run-validate/SKILL.md) to establish one.

Recommended next step: <one of: run-validate, graph-build, diagnose, knob-search, graph-coarsen, network-ingest>
```

## Output

A structured project summary as described above. This becomes the shared context
for all subsequent skills in the session.

## See also

- [graph-build](../graph-build/SKILL.md) — if no graph exists yet
- [run-validate](../run-validate/SKILL.md) — to establish a performance baseline
- [diagnose](../diagnose/SKILL.md) — if a baseline exists and performance needs investigation
- `website/docs/reference/` — CLI, knob catalog, JSON graph format, Python API
