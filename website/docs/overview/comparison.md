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
| Concurrent frame replay with O(1) reset | Yes (64 slots) | Rebuild per frame | Rebuild per frame | Fork-join only | Per-task scheduling | Yes (hard-coded) |
| Network ingress as a graph primitive | Yes (`$network`) | No | No | No | Worker comms only | Yes (hard-coded) |
| Machine-readable tuning surface | Knob catalog + schema | No | No | No | Config objects | No |
| Reconfigure without recompiling | Graph edit / CLI flag | Rebuild | Rebuild | Rebuild | Re-run script | Rebuild |
| Dynamic topology / data-dependent fan-out | No | Yes | Yes | Yes | Yes | No |
| Sub-µs task dispatch cost | No (~40–85 ns type-erased) | Yes | Yes | Yes | No | Yes |

## Against general frameworks

**Taskflow and TBB flow graph** are the right choice for single-frame
micro-task DAGs: their pre-compiled graphs have lower per-invocation cost, and
on a fine-grained wavefront Tomii is ~2.4× slower than Taskflow
([benchmarks](/docs/overview/benchmarks)). What they lack is Tomii's frame
model — both reconstruct or re-submit the graph per invocation, while Tomii
replays the same compiled graph across 64 slots with generational reset — and
both bake topology and kernels into homogeneous C++.

**Rayon** is fork-join data parallelism, not a persistent graph runtime. For a
homogeneous `parallel_for`, use Rayon; Tomii cannot express it. Tomii's worker
pool is itself built on Rayon.

**Dask** schedules Python tasks dynamically, optionally across machines. It
answers a different question — elastic throughput for data science — rather
than single-machine, millisecond-latency streaming with pinned cores.

**GNU Radio** is the standard framework for software-defined radio: a large
block library, a mature ecosystem, and hardware drivers Tomii does not have.
If you need those, use GNU Radio. What it lacks is a latency-predictable
frame model: on an FMCW radar workload with identical DSP kernels, Tomii
matches GNU Radio 4.0 end-to-end (~1.04×) while cutting the post-chirp tail
~1.8× at p50 (315 µs vs 567 µs) and running on 8.5× fewer cores (2.23 vs
18.9). GR4 wins at small CPIs; the trade is tail latency and core budget, not
a blanket speedup
([benchmarks](/docs/overview/benchmarks), `bench/radar-bench/`).

No framework in this family offers a single declarative DAG that mixes Rust,
C, and Python kernels at node granularity without a custom wrapper layer.

## Against fused systems: Agora

[Agora](https://github.com/Agora-wireless/Agora) is a software massive-MIMO
baseband where graph, kernels, and runtime control are fused into one
optimized codebase. That fusion is exactly what makes it fast — and exactly
what Tomii gives up on purpose.

Measured in our evaluation on the same uplink pipeline with the same kernels
(LDPC decode reinstated for an apples-to-apples comparison against Agora's
published figures, Tables 4–5), the **kernels run at parity** — Tomii's decode
is 0.81× Agora's per-task cost (37.5 vs 46.5 µs), with byte-identical output.
The remaining end-to-end gap is a bounded few tenths of a millisecond (e.g.
+0.23 ms median vs Agora's published 6130 figures) at a 10% longer frame period
(1.1 vs 1.0 ms), over kernel UDP against Agora's DPDK — and most of the
post-arrival term is a *tunable* graph barrier, where a ~30-line per-symbol
barrier edit cuts it ~70% with bit-identical output. Not a cost of the decoupled
design. The [benchmarks](/docs/overview/benchmarks) page has the numbers.

What the decoupling buys is the reconfiguration surface:

| Change | Tomii | Agora |
|---|---|---|
| Subcarrier count | Graph/config edit | Manager code change + recompile |
| Scheduling policy | CLI flag | Manager code change + recompile |
| Swap a kernel | Rebuild one plugin | Rebuild the application |
| Automated tuning | Knob catalog, verifier-gated | Not exposed |

The same evaluation measured cost-of-change rather than raw line counts. A
scheduling-policy change is a CLI flag (0 s) against a whole-binary rebuild in
the hand-built baselines; a kernel swap rebuilds one plugin (~1.15 s, against
~6.1 s to rebuild a Taskflow baseline) and leaves the runtime binary
byte-identical; and *inserting a new stage* — a ~23.9 s runtime rebuild under
static registration — drops to ~0.6 s (plugin-only) with dynamic kernel
registration, against ~83.9 s for the GNU Radio 4.0 baseline. Reaching Tomii's
per-packet parity with Taskflow/oneTBB in the first place took a ~649-line
hand-written streaming layer that Tomii provides from a graph and kernels.

If you are deploying production baseband at the latency limit, use Agora. If
you are iterating on pipeline structure, kernels, or scheduling (the
prototyping loop) the fused design cannot support that iteration without
source changes, and Tomii is built for it.

## The one-line summary

Choose the general frameworks for raw micro-task dispatch, the fused system
for absolute latency, and Tomii for the middle: streaming pipelines you want
to define, share, and optimize as data while staying within a bounded factor
of hand-fused performance.
