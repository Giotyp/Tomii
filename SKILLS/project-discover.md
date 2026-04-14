---
name: project-discover
description: Orient in an unknown SynStream project — map graph topology, plugin functions, knob inventory, and performance baseline before any other task
---

# Skill: project-discover

Orient an agent in an unknown SynStream project. Produces a structured project summary
containing the graph topology, plugin catalog, tunable knob inventory, and performance
baseline (if a prior run exists). Run this skill first before any other skill.

## Trigger

- Dropped into an unknown SynStream project directory
- Asked "what does this project do?"
- Starting a new optimization or development session

## Steps

### 1. Locate entry points

Search for `run_bench.py` and `graph.json` in the project directory:

```bash
find . -name "run_bench.py" -o -name "graph.json" | head -20
```

Read both files. From them, extract:
- Node names, `factor` values, dependency types (`$res`, `$barrier`, `$dep`)
- Initialization variables and their types
- Whether the graph uses `network_config` (network-input workload)
- Whether it uses `conditions`, `loops`, `post_nodes`

### 2. Load the knob inventory

```bash
python -m synstream --list-knobs-json
```

Parse the JSON output. For each knob, record `name`, `type`, `description`, and
`search_hint`. These hints specify the search strategy (e.g., "unimodal; binary search
1-physical_cores", "try both True and False") and are consumed directly by
[knob-search](knob-search.md).

### 3. Load the graph construction schema

```bash
python -m synstream --schema
```

This returns the machine-readable graph API including available node parameters
(`factor`, `group_size`, `priority`, `condition`, `loop`) and argument types
(`$res`, `$barrier`, `$dep`, `$ref`) with embedded `optimization_hint` fields.

### 4. Catalog plugin functions

Find the plugin source. It is referenced by `func_path=` in `graph.build(...)` calls —
typically `src/lib.rs` (Rust) or a `.h` file (C).

For **Rust plugins**: search for `#[synstream_export]` annotations:
```bash
grep -n "synstream_export" src/lib.rs
```

For each exported function, record: function name, parameter types, return type.
Pay attention to `#[synstream_export(variadic)]` — these accept `Vec<T>` for
fan-in from multiple predecessor instances.

For **C plugins**: search for `// @synstream_export` annotations:
```bash
grep -n "@synstream_export" include/*.h
```

### 5. Check for existing performance baseline

Look for `report.json` or `report.txt`:

```bash
find . -name "report.json" -o -name "report.txt" | head -5
```

If found, read and extract:
- `summary.avg_latency_us`, `summary.p99_latency_us`, `summary.throughput_streams_per_sec`
- `summary.scheduling_overhead_diagnostic.overhead_pct` and `interpretation`
- `critical_path.max_node_factor`, `critical_path.length_nodes`
- `optimization_suggestions` (ranked list of recommended actions)

### 6. Check for existing timing data

```bash
find . -name "timing.csv" -o -name "timing.txt" -o -name "timing_sched.csv" | head -5
```

If `timing_sched.csv` exists, it can be analyzed with:
```bash
python3 scripts/analyze_sched.py timing_sched.csv
```

### 7. Produce structured summary

Output a project summary in this structure:

```
PROJECT SUMMARY
===============
Entry point: <path to run_bench.py or graph.json>

Graph topology:
  Nodes: <list: name (factor=N, type=dependency_type)>
  Dependencies: <key edges as predecessor → successor>
  Network input: yes/no
  Conditions: yes/no
  Post-nodes: yes/no

Plugin: <path>
  Functions: <list: fn_name(arg_types) -> return_type [variadic?]>

Knobs available: <count> (run `--list-knobs-json` for full list)
  Key runtime knobs: workers, slots, batching_size, coalesce_barriers, inline_continuation

Performance baseline:
  <If report.json exists:>
    avg_latency_us: N
    p99_latency_us: N
    overhead_pct: N% (<interpretation>)
    Top optimization_suggestions: <list priority-1 entries>
  <If no report.json:>
    No baseline found. Run [run-validate](run-validate.md) to establish one.

Recommended next step: <one of: run-validate, graph-build, diagnose, knob-search, graph-coarsen>
```

## Output

A structured project summary as described above. This becomes the shared context
for all subsequent skills in the session.

## See also

- [graph-build](graph-build.md) — if no graph exists yet
- [run-validate](run-validate.md) — to establish a performance baseline
- [diagnose](diagnose.md) — if a baseline exists and performance needs investigation
- [AGENT.md](../AGENT.md) — quick-reference for plugin authoring and performance model
