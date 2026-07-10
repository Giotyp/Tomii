---
title: Observability
sidebar_label: Observability
---

Four instruments cover the observability surface: per-node timing CSVs, the
structured JSON report, graph visualization, and runtime state snapshots.

## Timing output

```python
app.run(workers=4, timing="timing.csv", output="out.txt", record=True)
```

`timing` writes a per-node timing CSV; `output` writes the raw timing stream.
`record=True` enables the recording path — timing events go through a
dedicated lock-free async recorder thread, so recording does not block
workers. Use `exclude_frames=N` to drop warm-up frames from the
statistics, and `use_rdtsc=True` on x86 for sub-microsecond timer precision
(see [Running graphs](/docs/guide/getting-started/running)).

## The JSON report

```python
app.run(workers=4, report="report.json")
```

The report is the primary diagnostic. Its `summary` block carries avg/p50/p99
frame latency and throughput; `summary.scheduling_overhead_diagnostic`
splits time into scheduling overhead versus compute (`overhead_pct`,
`overhead_us`, plus an interpretation string); `per_node` gives per-node
avg/p99 execution time and an `on_critical_path` flag; and
`optimization_suggestions` is a prioritized list of category, action, knob,
and estimated speedup (repository `README.md`). Read `overhead_pct` first:
high values mean the graph is too fine-grained for its kernels, and the fix
is coarsening, not knob tweaks.

## Visualizing graphs

```bash
python -m tomii --dump graph.json --out graph.dot   # GraphViz DOT topology
python -m tomii --visualize graph.json              # browser view
python -m tomii --visualize graph.json --edit       # browser edit mode
python -m tomii --visualize graph.json --ascii      # terminal ASCII art
python -m tomii --visualize graph.json --port 8080  # custom port
```

The web view renders a color-coded DAG: green compute nodes, orange-bordered
conditional nodes, gray post-nodes; edges styled by type (`$res` solid blue,
`$dep` dashed, `$barrier` thick orange). Edit mode saves back to the file,
and the Export Python button downloads a ready-to-run builder script for the
current graph (repository `README.md`). The same views are available on a
`Graph` object via `app.visualize()`.

## Runtime state snapshots

```python
app.run(workers=4, dump_state="state.json")
```

`dump_state` (CLI `--dump-state FILE`) writes a JSON snapshot of per-slot
runtime state at shutdown. While the process is running, sending `SIGUSR1`
writes numbered live snapshots `FILE.1`, `FILE.2`, ... without stopping the
run:

```bash
kill -USR1 <pid>
```

This is the tool for wedged runs: if a frame never completes, a live
snapshot shows which slots are active, which dependencies are outstanding,
and how many packets are parked in the pending-frames buffer
(`tomii-core/src/runtime/dump.rs`).

## Where each tool fits

| Question | Tool |
|---|---|
| Is the run fast, and why not? | `report.json` |
| Which node instances are slow? | timing CSV |
| Is the topology what I think it is? | `--dump` / `--visualize` |
| Why is the run stuck? | `--dump-state` + `SIGUSR1` |

The report feeds directly into the tuning loop — see
[Runtime knobs](/docs/guide/tuning/knobs).
