# Task: Optimize Taskflow Wavefront Performance

## Context

You have a working Taskflow wavefront implementation in `<WORKSPACE>`. Your goal is to reduce `avg_latency_us`.

## Starting point

Read `report.json` in the workspace for current performance data.

## Your task

1. Read `report.json` to understand current performance
2. Apply optimizations to `wavefront.cpp`

Do **not** rebuild or re-run the benchmark yourself — the harness will do that after you finish and report performance back to you.

## Discovering options

Explore Taskflow headers at `<REPO_ROOT>/taskflow-bench/taskflow-lib/` for available
partitioner strategies, executor options, and graph construction APIs.

## Success criteria

- `avg_latency_us` is lower than the baseline in `report.json`
- Binary still compiles and runs correctly
