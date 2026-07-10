---
title: Benchmarks
sidebar_label: Benchmarks
---

This page collects Tomii's measured results: wins and losses. Numbers
reproducible from the repository cite their benchmark directory; numbers from
our full evaluation setup are labeled as such.

## Methodology

Every comparison on this page follows the same rules:

- **One threshold per workload row**, applied identically to every framework.
  No framework gets a softer target.
- **Identical fixtures**: same seeds, input sizes, warm-up and iteration
  counts, same machine, same pinned core set, same clock.
- **Correctness gates performance.** Each workload ships a verifier; a run
  that fails verification is recorded as failing, not measured.
- **Same-binary attribution.** Runtime feature costs are measured with
  environment-variable toggles on one binary, never by rebuilding with source
  edits.
- MIMO kernel parity: the Tomii and Taskflow MIMO harnesses link the *same*
  compiled kernel libraries, so timing differences are pure scheduling.

## Massive-MIMO uplink vs Agora: the cost of generality

Agora is an application-specific MIMO engine; graph, kernels, and runtime
control fused into one codebase. Comparing against it measures the price of
Tomii's decoupling at the performance limit. Tomii implements the same uplink
pipeline (FFT → equalization → demodulation) declaratively, with dynamically
loaded kernels.

Uplink processing latency, mean ± std over 500 frames after warm-up. Agora
uses 26 workers; Tomii uses 26 workers + 4 resolution threads. *Default* is
the best manual configuration; *AI-opt* is the best of 5 agent-guided
iterations over 7 scheduler knobs.

| BS × UE | Pilot | UL | Agora | Tomii default | Tomii AI-opt |
|---|---|---|---|---|---|
| 8×8 | 1 | 13 | 0.50 ms ± 2.6 µs | 4.19 ± 0.28 ms | 4.07 ± 0.39 ms (−2.8%) |
| 16×16 | 16 | 13 | 1.62 ms ± 9.5 µs | 4.62 ± 1.88 ms | 4.27 ± 1.81 ms (−7.6%) |
| 64×8 | 1 | 13 | 1.04 ms ± 5.8 µs | 3.69 ± 1.80 ms | 3.24 ± 0.41 ms (−12.2%) |
| 64×16 | 16 | 13 | 2.90 ms ± 70.5 µs | 9.33 ± 1.62 ms | 8.37 ± 1.19 ms (−10.2%) |

*Measured in our evaluation; the Agora comparison is not reproducible from
this repository.*

Agora wins on absolute latency everywhere. On the
compute-intensive configurations (16×16, 64×16) the gap is a bounded 3–4×;
at 8×8 it grows to ~8× because fixed orchestration overhead dominates
sub-microsecond tasks. The extra latency has three concrete sources — dynamic
library loading, type-erased argument dispatch (under 100 ns per call), and
generic dependency tracking that Agora's hard-coded manager avoids; bounded
costs that do not scale with problem size.

What Tomii buys with that factor: subcarrier count, scheduling policy, and
kernel changes are graph edits or CLI flags; in Agora they are manager code
changes and recompilation. The *AI-opt* column exists **because** of that
surface: an agent proposed and evaluated configurations over seven scheduler
knobs, recovering 2.8–12.2% latency from the defaults with every trial
verifier-gated. The fused design has no equivalent loop.

## MIMO uplink vs Taskflow (reproducible)

Like-for-like framework comparison: both harnesses link identical Intel
kernel `.so` binaries; the workload is a real-PHY 4×4 uplink driven by UDP
packets. Source: `bench/mimo-bench/`.

| Configuration | Tomii | Taskflow | Ratio |
|---|---|---|---|
| 4×4, W=4, S=4 | 0.926 ms/slot | 1.168 ms/slot | **Tomii 1.26× faster** |
| 4×4, full W×S sweep | 0.923–3.259 | 1.168–1.283 | Tomii 1.26–1.39× faster |
| 16×16, W=24, S=4 | 47.98 ms/slot | 55.18 ms/slot | Tomii 1.15× faster |
| 16×16, W=24, S=16 | 14.41 ms/slot | 20.58 ms/slot | Tomii 1.41× faster |

The mechanism is architectural: Tomii dispatches FFT tasks as each UDP packet
arrives, while Taskflow must collect all packets before submitting the DAG.
At 4×4 this overlap recovers 280–360 µs per frame. The advantage persists
from the sender-rate-limited regime (4×4) to the compute-limited regime
(16×16).

## Multi-frame pipeline vs Taskflow (reproducible)

Tomii does not win this benchmark. A linear pipeline with ~16 µs per-task
compute, swept over S concurrent frames. Source: `bench/pipeline-bench/`.

| Concurrent frames | Tomii vs Taskflow |
|---|---|
| S=1 | Tomii 2.45× slower |
| S=16 | Tomii 1.33× slower |
| S=64 | Tomii ~1.3× slower |

The claim is that the gap *closes* — multi-slot amortization is measurable —
not that Tomii wins. At S=1 Tomii pays a fixed ~5 ms per frame of runtime
overhead; at S≥16 the resolution-thread cost spreads across lanes and
scheduling overhead stays below 15% of compute.

**Memory growth**, same benchmark: Tomii adds +83 kB per slot against
Taskflow's +131 kB — a 1.6× lower growth rate (`/usr/bin/time -v`, W=4,
measured at S=1 and S=64). Note the caveat: Tomii's *baseline* RSS is higher
(pre-allocated worker stacks and resolution-thread machinery). The advantage
is the slope, which matters for long-running services with many frames, not
the base.

## Slot reuse

Completing a frame in Tomii is a generational reset: bump a counter, mark
the slot free (O(1)). Taskflow's eager model reconstructs the task graph per
frame. At 16,384 frame completions this measures **151× faster** in our
evaluation. *Paper measurement; not reproducible as a repo microbenchmark.*
The gap widens linearly with frame count — it is structural, not tuned.

## Anti-diagonal wavefront (a loss, reproducible)

Fine-grained single-frame wavefront (N=512, W=1): **Tomii is ~2.4× slower
than TBB and Taskflow**. Source: `bench/anti-diag-bench/`. The cost is
intrinsic — sequentially-consistent dependency counters and 40–85 ns
type-erased dispatch on tasks too small to amortize them. This workload is
outside Tomii's envelope; see [When to use Tomii](/docs/overview/when-to-use).

## Agent tuning: four arms, one verifier

Four search strategies over the generated knob space
(`python -m tomii --knob-space`): runtime CLI knobs plus per-graph knobs
(factor variables, `group_by` widths). Graph knobs can silently violate
workload invariants, so a hard-coded verifier gates every trial — a rejected
trial never counts toward best. Three workloads, best ms/frame among passing
trials. Source: `examples/agent-tuning/`, `run_all.sh`.

| Arm | stream-analytics (50 iter) | pipeline (50 iter) | mimo (30 iter) |
|---|---|---|---|
| Baseline (default knobs) | 1.69 | 26.78 | 23.35 |
| Random | 0.190 (1/50 pass) | 4.54 (6/50) | 6.01 (10/30) |
| Bayesian (Optuna TPE) | 0.191 (23/50) | 5.88 (27/50) | 46.48 (18/30) |
| Grid | 0.118 (4/50) | 17.48 (18/50) | — (0/30) |
| **Agent (Claude)** | **0.0874 (41/50)** | **1.29 (45/50)** | **5.92 (20/30)** |

The generated space is ~14 million cells for stream-analytics and includes
correctness-critical graph knobs. That is the point of the benchmark: naive
search mostly produces invalid configurations — random found 1 valid config
in 50 trials — while the agent stays in the valid region (41/50), wins
single-trial best on all three workloads, and uses ~9× less wall time than
random search. A machine-readable tuning surface plus verifier gating is what
lets a language model tune a graph it has never seen. That is the property
the [tuning guide](/docs/guide/tuning/agent-tuning) builds on.

Two honest footnotes. Bayesian's MIMO result is a harness artifact: rejected
trials return no objective value, so TPE retreated to a feasible-but-slow
region. Grid's MIMO failure is enumeration order: its first 30 cells all fall
in a corner that cannot run a network workload. Both are documented as known
harness fixes in the example README. On an earlier hand-written 2,048-cell
space of runtime knobs only, all four arms converged to within 0.04 ms with
0 rejected trials — small spaces do not separate the strategies; the
generated space does.

## Reproducing

Each `bench/` directory contains the harness, a verifier, and a README with
the exact methodology. Result CSVs are regenerated outputs and are not
committed; run the sweep scripts to reproduce them. The MIMO benchmark
requires Intel MKL and the Agora packet sender (documented in
`bench/mimo-bench/README.md`).
