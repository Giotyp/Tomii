---
title: Running graphs
sidebar_label: Running
---

`app.run(**options)` writes the graph JSON to a temp file and launches the
Tomii runtime binary. Every keyword maps 1:1 to a CLI flag of the same name
(`workers` → `--workers`). The tables below come from
`python -m tomii --list-knobs`; the same list, with search hints, is in the
[knob catalog reference](/docs/reference/knob-catalog).

## What a stream is, and what slots are

The runtime executes the graph once per stream. A slot is the in-flight
execution state for one stream: result buffers, dependency counters, and
per-node instances. With `slots=1` streams run one at a time, which minimizes
latency. With `slots=N` up to N streams execute concurrently and slot state
is reused generationally between streams, which raises throughput.

## Integer options

| Option | CLI flag | Meaning |
|---|---|---|
| `workers` | `--workers` | Rayon worker threads (match physical cores) |
| `core_offset` | `--core-offset` | First CPU to pin workers to (use 1 to leave CPU 0 for the OS) |
| `system_threads` | `--system-threads` | Resolution/scheduler threads (default 1) |
| `receiver_threads` | `--receiver-threads` | Dedicated network receiver threads |
| `slots` | `--slots` | Concurrent in-flight streams |
| `max_streams` | `--max-streams` | Total streams to process (0 = run indefinitely) |
| `max_runtime` | `--max-runtime` | Stop after N seconds (0 = run until `max_streams` complete) |
| `batching_size` | `--batching-size` | Max tasks per scheduler batch |
| `batching_limit` | `--batching-limit` | Max outstanding batches before back-pressure |
| `exclude_streams` | `--exclude-streams` | Skip the first N streams from timing output |
| `record_stream` | `--record-stream` | Record timing for one stream index only |

## Boolean options

| Option | CLI flag | Meaning |
|---|---|---|
| `fifo` | `--fifo` | FIFO task scheduling instead of the default depth-first |
| `custom` | `--custom` | Custom scheduling strategy |
| `slot_priority` | `--slot-priority` | Prioritize tasks from the earliest active slot |
| `coalesce_barriers` | `--coalesce-barriers` | Batch barrier fan-outs into bulk tasks |
| `inline_continuation` | `--inline-continuation` | Run single-successor tasks inline |
| `inits` | `--inits` | Re-run graph initializations on each stream |
| `record` | `--record` | Enable timing/event recording to file |
| `use_rdtsc` | `--use-rdtsc` | RDTSC for sub-microsecond timing (x86 only) |
| `debug` | `--debug` | Verbose debug output |

## Output options

| Option | CLI flag | Meaning |
|---|---|---|
| `output` | `--output` | Raw timing output file |
| `timing` | `--timing` | Per-node timing CSV |
| `report` | `--report` | JSON summary report |
| `dump_state` | `--dump-state` | Runtime state snapshot at shutdown |

`run()` also accepts `env={...}`, a dict of environment variables passed to
the runtime process. The matrix-compute example uses it to pass `SCRIPT_DIR`
so the plugin can resolve its output path.

## The report

Pass `report="report.json"` to get a structured performance report after each
run. Its keys (from the repository `README.md`):

| Key | Content |
|---|---|
| `summary.avg_latency_us` / `p50` / `p99` | Stream latency statistics |
| `summary.throughput_streams_per_sec` | End-to-end throughput |
| `summary.scheduling_overhead_diagnostic` | `overhead_pct`, `overhead_us`, interpretation |
| `per_node` | Per-node avg/p99 exec time, `on_critical_path` flag |
| `optimization_suggestions` | Prioritized list: category, action, knob, estimated speedup |

The report is the input to the tuning workflow — see
[Observability](/docs/guide/tuning/observability) and
[Runtime knobs](/docs/guide/tuning/knobs).

## Running without Python

The runtime binary takes the same flags directly:

```bash
cargo run -p tomii-core --bin main -- \
  --json graph.json --dylib plugin.so \
  --workers 4 --slots 2 --max-streams 100
```

See the [CLI reference](/docs/reference/cli) for the full flag list.
