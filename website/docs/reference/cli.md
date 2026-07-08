---
title: Command-line interfaces
sidebar_label: CLI
---

Tomii has two command-line entry points: the Python module
(`python -m tomii`) for inspection, schema, and visualization, and the Rust
runner binary that executes graphs. The Python `Graph.run()` method
constructs a runner invocation for you (see
[running](/docs/guide/getting-started/running)); this page documents both
interfaces directly.

## python -m tomii

Flag definitions live in `tomii/__main__.py`. Exactly one subcommand flag is
processed per invocation.

| Flag | Arguments | Description |
|---|---|---|
| `--list-knobs` | — | Human-readable list of `graph.run()` options (also the default with no arguments, and the output of `--help`) |
| `--list-knobs-json` | — | Machine-readable JSON knob catalog (see [knob catalog](/docs/reference/knob-catalog)) |
| `--knob-space` | `[graph.json] [--workload NAME]` | Versioned tuning search space; with a graph, adds per-graph knobs to the catalog's perf knobs |
| `--schema` | — | JSON schema for graph construction parameters |
| `--dump` | `graph.json [--out FILE]` | Emit graph topology as GraphViz DOT (to stdout, or to `FILE` with `--out`) |
| `--visualize` | `graph.json [--edit] [--ascii] [--port N]` | Interactive graph viewer |

`--visualize` modes:

| Invocation | Mode |
|---|---|
| `--visualize graph.json` | View (read-only, browser) |
| `--visualize graph.json --edit` | Edit (save back to the file) |
| `--visualize new.json` | Create (file does not exist yet) |
| `--visualize graph.json --ascii` | Terminal box-drawing art |
| `--visualize graph.json --port 8080` | Custom web-server port |

`--dump` emits topology only. Runtime state snapshots come from the runner
binary's `--dump-state` flag, described below.

## Runner binary

The runner is built by `cargo build -p tomii-core` (binary `main`). Flag
definitions live in `tomii-core/src/bin/main.rs`.

```bash
cargo run -p tomii-core --bin main -- \
  --json /path/to/graph.json \
  --dylib /path/to/plugin.so \
  --workers 4 \
  --core-offset 1 \
  --max-runtime 60
```

### Core flags

| Flag | Type / default | Description |
|---|---|---|
| `--json FILE` | string, required | Graph JSON file |
| `--dylib FILE` | string, required | Plugin library (`.so`) |
| `--workers N` | int, `1` | Worker (Rayon) threads |
| `--core-offset N` | int, `1` | First CPU to pin workers to |
| `--system-threads N` | int, `1` | Resolution threads |
| `--receiver-threads N` | int, `1` | Dedicated network receiver threads |
| `--slots N` | int, `1` | Concurrent in-flight streams |
| `--max-streams N` | int, `1` | Total streams to process |
| `--max-runtime SECS` | int, `0` | Stop after this many seconds; `0` = no time limit |

### Scheduling flags

| Flag | Type / default | Description |
|---|---|---|
| `--fifo` | bool, off | FIFO scheduler instead of the default work-stealing one |
| `--custom` | bool, off | Custom lock-free priority scheduler |
| `--batching-size N` | int, `1` | Completed tasks to batch before processing |
| `--batching-limit US` | int, `10` | Maximum time to wait for a batch, in microseconds |
| `--slot-priority` | bool, off | Process slots sequentially with automatic round-robin |
| `--coalesce-barriers` | bool, off | Coalesce barrier fan-outs into bulk tasks (`min(N, workers)` tasks instead of N) |
| `--inline-continuation` | bool, off | Run one ready successor inline on the completing worker instead of spawning it |
| `--no-fanout-bulk` | bool, off | Disable 1:1 fanout-bulk dispatch; bit-identical to the per-cell path, for correctness verification |
| `--resolution-strategy S` | string, `multi-slot-batch` | Batch resolution strategy; `multi-slot-batch` is the only available value |

`--coalesce-barriers` targets fine-grained workloads where per-task compute
is far below spawn overhead (e.g. wavefront); it serializes coarse-grained
tasks, so leave it off for MIMO-style graphs. `--inline-continuation`
removes a scheduler round-trip for chain-dominant graphs (`factor` 1 chains)
and must likewise stay off for coarse-grained workloads. Tuning guidance for
these flags lives in [knobs](/docs/guide/tuning/knobs).

### Output and diagnostics flags

| Flag | Type / default | Description |
|---|---|---|
| `--output FILE` | string, `stdout` | Redirect stdout to a file |
| `--timing FILE` | string, unset | Per-node timing CSV |
| `--report FILE` | string, unset | JSON performance report |
| `--record` | bool, off | Scheduler event recording |
| `--record-stream ID` | int, unset | Record only one stream (memory optimization) |
| `--exclude-streams N` | int, `0` | Exclude the first N streams from timing statistics |
| `--use-rdtsc` | bool, off | RDTSC-based timing (x86) |
| `--debug` | bool, off | Debug-level logging (`RUST_LOG` overrides it) |
| `--inits` | bool, off | Print initializations to stdout |
| `--dump-state FILE` | string, unset | Runtime state snapshot (JSON) at shutdown |

With `--dump-state FILE` set, the binary writes a JSON snapshot of per-slot
runtime state to `FILE` at shutdown. On Unix, sending `SIGUSR1` to the
running process additionally writes numbered live snapshots (`FILE.1`,
`FILE.2`, ...) — useful for diagnosing wedged runs. See
[observability](/docs/guide/tuning/observability).

### Low-level tuning flags

| Flag | Type / default | Description |
|---|---|---|
| `--batch-queue-capacity N` | int, `65536` | Capacity of the lock-free task-completion ring buffer |
| `--spin-iterations N` | int, `32` | Spin iterations before blocking recv in resolution threads |
| `--sched-flush-threshold N` | int, `32` | Flush accumulated successors to workers every N items during batch processing |
| `--socket-recv-buf-bytes BYTES` | int, `16777216` | UDP socket kernel receive buffer size |
| `--recv-pool-size N` | int, `1024` | Pre-allocated packet buffers per receiver thread |
| `--spin-wait-spin-iters N` | int, `64` | `spin_loop()` iterations before switching to `yield_now()` |
| `--spin-wait-yield-iters N` | int, `256` | `yield_now()` iterations before switching to `park_timeout()` |
| `--spin-wait-park-ns NS` | int, `100` | `park_timeout()` duration in nanoseconds |

The subset of flags exposed as tuning knobs through the Python API — with
roles, domains, and search hints — is cataloged in the
[knob catalog](/docs/reference/knob-catalog). Environment variables read by
the binary and the build are listed in
[environment variables](/docs/reference/environment).
