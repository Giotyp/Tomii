---
title: When to use Tomii
sidebar_label: When to use
---

Tomii has a deliberate performance envelope. This page states it plainly:
the workload classes Tomii targets, the classes it does not, the intrinsic
costs behind that boundary, and the measured losses. If your workload sits
outside the envelope, one of the frameworks in the
[comparison](/docs/overview/comparison) will serve you better.

## Workloads Tomii targets

**Streaming MIMO pipelines.** Large per-task work (≥ 10 µs), tens to hundreds
of nodes, 1–64 concurrent slots, long-running processes. Network packet
ingress, FFT/CSI/beamforming stages, and barriers on the critical path. The
scheduling abstraction and slot lifecycle amortize well here; on a real 4×4
MIMO uplink Tomii measures 1.26–1.39× faster than a Taskflow port running
identical kernel binaries (`bench/mimo-bench/`).

**Multi-slot fan-out.** The same graph topology applied to many independent
frames concurrently. Slot parallelism hides resolution-thread latency, and
frame completion is an O(1) generational reset instead of a per-frame graph
rebuild. Session-processing patterns where the same pipeline fires repeatedly
on arriving frames fit this shape.

**Heterogeneous DAGs with barriers.** Mixed compute and network nodes,
conditional routing, grouped synchronization across fan-in nodes; expressed
declaratively with `$barrier`, `$dep`, and `group_by` rather than hand-written
coordination code.

**Machine-driven optimization research.** The JSON graph and the typed knob
catalog form a structured, parse-validated search surface. If your research
loop is "an optimizer edits the pipeline and measures", Tomii ships that loop
[out of the box](/docs/guide/tuning/agent-tuning), verifier-gated.

## Workloads Tomii does not target

**Sub-microsecond micro-tasks.** Per-task work below ~1 µs is dominated by
dispatch and resolution overhead. Static-graph executors (Taskflow, TBB flow
graph) have lower per-invocation cost; on a fine-grained anti-diagonal
wavefront Tomii measures ~2.4× slower than both (`bench/anti-diag-bench/`).
As a practical rule, keep per-task compute at or above ~16 µs — below that,
scheduling overhead exceeds 15% of compute even at high slot counts.

**Pure `parallel_for` workloads.** Homogeneous loops over independent
elements are better served by Rayon or TBB directly. Tomii's static topology
cannot express data-dependent fan-out, so a `parallel_for` reduction is not
just slow — it is inexpressible.

**Dynamic-topology DAGs.** The graph is compiled once at startup and reused
across all slots. Workloads where the node set or edges change between
frames (dynamic batching, irregular graph analytics) are out of scope.

**Latency-sensitive single frames at high slot counts.** Throughput improves
with slots; per-frame latency does not. If you need one frame to finish as
fast as possible, run it with few slots — or use a fused system.

## The intrinsic costs

Three costs are structural. They are the price of the decoupling model and
cannot be removed without abandoning multi-slot correctness or plugin
isolation:

1. **Cross-slot dependency counters.** Every task arrival performs a
   sequentially-consistent atomic update on a shared dependency counter.
   With W workers and S slots, fan-in nodes with large in-degree contend on
   the same cache line.
2. **Type-erased dispatch, ~40–85 ns per call.** Kernels are called through
   a type-erased boundary so plugins stay independently compiled. The cost is
   constant in data size — it is in type extraction and packaging, not data
   movement — and falls below 1% of compute for millisecond-granularity
   kernels.
3. **The resolution-thread state machine.** A dedicated thread runs
   dependency propagation and slot lifecycle. It is what makes O(1)
   generational reset possible, and it becomes the bottleneck at very high
   slot counts or very large fan-out.

The [benchmarks](/docs/overview/benchmarks) page quantifies both sides of
this trade, including the configurations where Tomii loses.

## In one sentence

Use Tomii when you are prototyping a streaming pipeline with concurrent
frames, per-task work above ~16 µs, and a topology you want to edit, share,
and optimize as data. Use a fused or fork-join system when you are past
prototyping and need the last 3–4× of absolute latency.
