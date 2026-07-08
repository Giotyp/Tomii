---
title: Comparison with other systems
sidebar_label: Comparison
---

Tomii occupies a point between two families: general task-parallel frameworks
(Taskflow, TBB flow graph, Rayon, Dask) and application-specific fused systems
(Agora for massive-MIMO baseband). This page places it next to both.

## Capability matrix

| | Tomii | Taskflow | TBB flow graph | Rayon | Dask | Agora |
|---|---|---|---|---|---|---|
| Topology defined as | JSON data | C++ code | C++ code | Rust code | Python code | C++ code (fused) |
| Kernel languages in one DAG | Rust + C + Python | C++ | C++ | Rust | Python | C++ (fused) |
| Concurrent stream replay with O(1) reset | Yes (64 slots) | Rebuild per stream | Rebuild per stream | Fork-join only | Per-task scheduling | Yes (hard-coded) |
| Network ingress as a graph primitive | Yes (`$network`) | No | No | No | Worker comms only | Yes (hard-coded) |
| Machine-readable tuning surface | Knob catalog + schema | No | No | No | Config objects | No |
| Reconfigure without recompiling | Graph edit / CLI flag | Rebuild | Rebuild | Rebuild | Re-run script | Rebuild |
| Dynamic topology / data-dependent fan-out | No | Yes | Yes | Yes | Yes | No |
| Sub-µs task dispatch cost | No (~40–85 ns type-erased) | Yes | Yes | Yes | No | Yes |

## Against general frameworks

**Taskflow and TBB flow graph** are the right choice for single-stream
micro-task DAGs: their pre-compiled graphs have lower per-invocation cost, and
on a fine-grained wavefront Tomii is ~2.4× slower
([benchmarks](/docs/overview/benchmarks)). What they lack is Tomii's stream
model — both reconstruct or re-submit the graph per invocation, while Tomii
replays the same compiled graph across 64 slots with generational reset — and
both bake topology and kernels into homogeneous C++.

**Rayon** is fork-join data parallelism, not a persistent graph runtime. For a
homogeneous `parallel_for`, use Rayon; Tomii cannot express it. Tomii's worker
pool is itself built on Rayon.

**Dask** schedules Python tasks dynamically, optionally across machines. It
answers a different question — elastic throughput for data science — rather
than single-machine, millisecond-latency streaming with pinned cores.

No framework in this family offers a single declarative DAG that mixes Rust,
C, and Python kernels at node granularity without a custom wrapper layer.

## Against fused systems: Agora

[Agora](https://github.com/Agora-wireless/Agora) is a software massive-MIMO
baseband where graph, kernels, and runtime control are fused into one
optimized codebase. That fusion is exactly what makes it fast — and exactly
what Tomii gives up on purpose.

Measured in our evaluation on the same uplink pipeline, Agora processes
frames in 0.50–2.90 ms across antenna configurations; Tomii runs 3.24–8.37 ms
(agent-optimized) — a bounded 3–4× latency factor on compute-intensive
configurations. The [benchmarks](/docs/overview/benchmarks) page has the full
table. The gap has three named sources — dynamic library loading, type-erased
dispatch, generic dependency tracking — all bounded costs that do not scale
with problem size.

What the 3–4× buys is the reconfiguration surface:

| Change | Tomii | Agora |
|---|---|---|
| Subcarrier count | Graph/config edit | Manager code change + recompile |
| Scheduling policy | CLI flag | Manager code change + recompile |
| Swap a kernel | Rebuild one plugin | Rebuild the application |
| Automated tuning | Knob catalog, verifier-gated | Not exposed |

The same evaluation measured application code size on a matched workload:
210 lines for a Taskflow C++ implementation against 133 for Tomii, with CPU
affinity, timing, and CSV output built into the runtime instead of
re-implemented per application.

If you are deploying production baseband at the latency limit, use Agora. If
you are iterating on pipeline structure, kernels, or scheduling — the
prototyping loop — the fused design cannot support that iteration without
source changes, and Tomii is built for it.

## The one-line summary

Choose the general frameworks for raw micro-task dispatch, the fused system
for absolute latency, and Tomii for the middle: streaming pipelines you want
to define, share, and optimize as data while staying within a bounded factor
of hand-fused performance.
