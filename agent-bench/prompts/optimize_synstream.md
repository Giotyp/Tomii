# Task: Optimize SynStream Wavefront Performance

## Context

You have a working SynStream wavefront implementation in `<WORKSPACE>`. Your goal is to reduce latency.

## Starting point

Read `report.json` in the workspace for current performance data.

## Your task

1. Read `report.json` to understand current performance
2. Apply optimizations to `run_wavefront.py` and/or `src/lib.rs`

Do **not** rebuild or re-run the benchmark yourself — the harness will do that after you finish and report performance back to you.

## Discovering options

Run `python -m synstream --list-knobs-json` for all `graph.run()` runtime flags with search hints (machine-readable JSON).
Run `python -m synstream --schema` for graph construction parameters: node options (factor, group_size), arg types ($ref, $res, $barrier).
Read `AGENT.md` for a quick-reference summary of the same.

## Success criteria

- `avg_latency_us` is lower than the baseline in `report.json`
- `run_wavefront.py` still prints `PASS`
