# Task: Optimize SynStream Wavefront Performance

## Context

You have a working SynStream wavefront implementation in `<WORKSPACE>`. Your goal is to reduce latency.

## Starting point

Read `report.json` in the workspace for current performance data.

## Reading the report

`report.json` contains three fields that direct optimization effort:

- `summary.scheduling_overhead_diagnostic.overhead_pct` — percentage of stream latency that is scheduling overhead (not compute). If this is >60%, graph topology is the bottleneck, not the kernel.
- `summary.scheduling_overhead_diagnostic.interpretation` — plain-English diagnosis of the bottleneck.
- `optimization_suggestions` — ranked list of concrete changes. **Apply priority-1 first**, then rebuild and re-read before applying lower priorities. Each entry has `knob`, `suggested_value`, `action`, and `estimated_speedup`.
- `critical_path.max_node_factor` — highest per-diagonal factor on the critical path; use this to compute a good `tile_size` (target ≈ `max_node_factor / 8`).
- `summary.total_tasks_per_stream` — total tasks spawned per stream; values >50K indicate high scheduling overhead.

## Your task

1. Read `report.json` and check `optimization_suggestions[0]` for the highest-priority action
2. Apply that change to `run_wavefront.py` and/or `src/lib.rs`
3. After the harness rebuilds and reports new performance, re-read `report.json` and apply the next suggestion if beneficial

Do **not** rebuild or re-run the benchmark yourself — the harness will do that after you finish and report performance back to you.

## Discovering options

Run `python -m synstream --list-knobs-json` for all `graph.run()` runtime flags with search hints (machine-readable JSON).
Run `python -m synstream --schema` for graph construction parameters: node options (factor, group_size, with optimization_hint fields), arg types ($ref, $res, $barrier).
Read `AGENT.md` → "Performance Model" section for the decision rule and tile coarsening recipe.

## Success criteria

- `avg_latency_us` is lower than the baseline in `report.json`
- `run_wavefront.py` still prints `PASS`
