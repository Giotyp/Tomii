---
title: Knob catalog
sidebar_label: Knob catalog
---

Every `graph.run()` keyword argument maps to a runner-binary flag. The
canonical machine-readable form of this catalog is:

```bash
python -m tomii --list-knobs-json
```

This page transcribes catalog version 3. Each knob carries a name (the
`graph.run()` keyword), the CLI flag it maps to, a type, a role, an optional
search domain, and a description. The catalog is the single source the
knob-space generator ([`tomii.knob_space`](/docs/reference/python-api#knob_space))
builds search spaces from; see [knobs](/docs/guide/tuning/knobs) for tuning
guidance and [agent tuning](/docs/guide/tuning/agent-tuning) for the
search harness.

## Roles

| Role | Meaning |
|---|---|
| `perf` | Affects performance; safe to search (verifier-gated) |
| `measurement` | Benchmark parameter; fix it for fair comparison, never search it |
| `io` | Output/diagnostic plumbing; never search |
| `env` | Machine-specific (CPU pinning); set once per host, don't search |

In domains, `pow2` means the sensible search points are powers of two within
`[min, max]` (sample the exponent, not the value). The `workers` maximum is
resolved to the host's CPU count at catalog-generation time (128 on the
machine that produced this table).

## Catalog

| Name | CLI flag | Type | Role | Domain | Description |
|---|---|---|---|---|---|
| `workers` | `--workers` | int | perf | 1–128, pow2 | Rayon worker threads (match physical cores) |
| `core_offset` | `--core-offset` | int | env | — | First CPU to pin workers to (use 1 to leave CPU 0 for OS) |
| `system_threads` | `--system-threads` | int | perf | 1–4, linear | Resolution/scheduler threads (default 1; rarely needs changing) |
| `receiver_threads` | `--receiver-threads` | int | perf | 0–4, linear | Dedicated network receiver threads (for network-input graphs) |
| `max_runtime` | `--max-runtime` | int | measurement | — | Stop after N seconds (0 = run until max_frames complete) |
| `slots` | `--slots` | int | perf | 1–64, pow2 | Concurrent in-flight frames (1 for latency, &gt;1 for throughput) |
| `max_frames` | `--max-frames` | int | measurement | — | Total frames to process (0 = run indefinitely) |
| `batching_size` | `--batching-size` | int | perf | 1–512, pow2 | Max tasks per scheduler batch |
| `batching_limit` | `--batching-limit` | int | perf | 1–8, pow2 | Max outstanding batches before back-pressure |
| `exclude_frames` | `--exclude-frames` | int | measurement | — | Skip first N frames from timing output |
| `record_frame` | `--record-frame` | int | measurement | — | Record timing for this specific frame index only |
| `fifo` | `--fifo` | bool | perf | bool | Use FIFO task scheduling instead of default (depth-first) |
| `custom` | `--custom` | bool | perf | bool | Enable custom scheduling strategy |
| `inits` | `--inits` | bool | measurement | bool | Re-run graph initializations on each frame |
| `debug` | `--debug` | bool | io | bool | Print verbose debug output |
| `record` | `--record` | bool | io | bool | Enable timing/event recording to file |
| `use_rdtsc` | `--use-rdtsc` | bool | measurement | bool | Use RDTSC for sub-μs timing (x86 only; improves timer precision) |
| `slot_priority` | `--slot-priority` | bool | perf | bool | Prioritize tasks from the earliest active slot |
| `coalesce_barriers` | `--coalesce-barriers` | bool | perf | bool | Batch barrier fan-outs into bulk tasks (reduces overhead for fine-grained graphs) |
| `inline_continuation` | `--inline-continuation` | bool | perf | bool | Run single-successor tasks inline (reduces scheduling overhead) |
| `no_fanout_bulk` | `--no-fanout-bulk` | bool | perf | bool | — |
| `output` | `--output` | str | io | — | Path for raw timing output file |
| `timing` | `--timing` | str | io | — | Path for per-node timing CSV |
| `report` | `--report` | str | io | — | Path for JSON summary report (avg/p99 latency, bottleneck hints) |
| `dump_state` | `--dump-state` | str | io | — | Path for a runtime state snapshot (JSON) written at shutdown; SIGUSR1 writes numbered live snapshots (wedged-run debugging) |

## Search hints

Search hints from the same catalog, for knobs that carry one. Boolean knobs
without a specific hint carry the generic hint "try both True and False";
string knobs carry none.

| Name | Search hint |
|---|---|
| `workers` | unimodal; binary search 1–physical_cores; diminishing returns past core count |
| `core_offset` | set to 1 to leave CPU 0 for OS; rarely needs tuning otherwise |
| `system_threads` | leave at default 1 unless profiling shows scheduler bottleneck |
| `slots` | 1 minimizes latency; &gt;1 increases throughput; try 1,2,4,8 |
| `max_frames` | benchmark parameter; set to fixed value for fair comparison |
| `batching_size` | unimodal; binary search 1–512; larger reduces scheduling overhead for fine-grained graphs |
| `batching_limit` | unimodal; try 1,2,4; higher allows more outstanding work |
| `exclude_frames` | skip warmup frames; set to 1–3 to exclude JIT effects from timing |
| `fifo` | try both; depth-first (default) usually better for latency |
| `use_rdtsc` | enable for sub-us timing precision on x86; no effect on performance |
| `slot_priority` | try both when slots &gt; 1; can reduce tail latency under imbalanced graphs |
| `coalesce_barriers` | helpful when node factor &gt;&gt; worker count (reduces barrier overhead) |
| `inline_continuation` | try both; often reduces latency for linear chains or low-fan-out graphs |

The runner binary also accepts low-level flags that are not part of this
catalog (ring-buffer capacity, spin-wait parameters, socket buffer sizes);
those are listed under [command-line interfaces](/docs/reference/cli).
