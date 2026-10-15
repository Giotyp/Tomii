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
  counts, same machine, and same clock. Cores are pinned; where a comparison
  uses different core *budgets* (radar vs GNU Radio), the core count is
  reported as a result, not held equal.
- **Correctness gates performance.** Each workload ships a verifier; a run
  that fails verification is recorded as failing, not measured.
- **Same-binary attribution.** Runtime feature costs are measured with
  environment-variable toggles on one binary, never by rebuilding with source
  edits.
- **Kernel parity.** Comparative harnesses link the *same* compiled kernel
  libraries (Tomii vs Taskflow on MIMO; Tomii vs GNU Radio on radar), so
  timing differences are pure scheduling.

## Massive-MIMO uplink vs Agora: the cost of generality

Agora is an application-specific MIMO engine; graph, kernels, and runtime
control fused into one codebase. Comparing against it measures the price of
Tomii's decoupling at the performance limit. Tomii implements the same uplink
pipeline (FFT → equalization → demodulation → LDPC decode) declaratively, with
dynamically loaded kernels.

The pipeline runs with the **same kernels on both sides** — LDPC decode
reinstated so the comparison is apples-to-apples with Agora's published
CoNEXT'20 Table 3. This isolates the price of Tomii's decoupling from any
kernel difference.

**The kernels run at parity.** Tomii's LDPC decode is 40.8 µs/task against
Agora's reported 46.5 µs (0.88×), and Tomii's output is byte-identical from
W=1 to W=26; the per-stage costs match Table 3.

**Where the latency gap comes from.** On 64×16, Tomii's post-arrival tail is
1.08 ms against Agora's ~0.18 ms. That gap decomposes into roughly **3%**
intrinsic decoupling overhead, **36%** decode compute that Agora pipelines
across frames, and **60%** a *tunable* graph barrier — a one-line graph edit
(full barrier → per-symbol grouped barrier) recovers ~94% of that term,
dropping the tail to **0.54 ms** with byte-identical output. End-to-end
latency is **ingest-bound** (Tomii's UDP receiver vs Agora's DPDK); the best
hand-written Taskflow and oneTBB baselines hit the same ingest wall, so it is
not a cost of decoupling.

The takeaway is modularity-first: the kernels run at parity, and the remaining
latency is tunable configuration plus ingest — not the price of the decoupled
design. What the decoupling buys is the reconfiguration surface: subcarrier
count, scheduling policy, and kernel changes are graph edits or CLI flags,
where in Agora they are manager code changes and recompilation.

*The Agora figures are the published CoNEXT'20 Table 3 numbers; no Agora
binary is run, and this comparison is not reproducible from this repository.*

## MIMO uplink vs Taskflow / oneTBB (reproducible)

Like-for-like comparison: every harness links the identical Intel kernel
`.so` binaries, so timing differences are pure orchestration. Two Taskflow
formulations are measured — a per-packet arrival-driven one (`tf-async`) and
the collect-then-submit DAG (`tf-orig`) — plus an oneTBB `flow_graph`
(`tbb-flow`). Source: `bench/mimo-bench/`.

Mean per-frame latency (ms), best configuration per system:

| Configuration | Tomii | tf-async | tbb-flow |
|---|---|---|---|
| 4×4, S=4 | 0.971 | 0.962 | 0.954 |
| 16×16, S=8 | 5.958 | 5.914 | 5.904 |
| 16×16, S=16 (steady) | 3.071 | 3.020 | 3.008 |
| 64×16, S=8 | 23.609 | 23.559 | 23.513 |

**Tomii matches the best hand-written execution — parity, not a win.** It
lands within **0.2–2.1%** of the fastest per-packet baseline (`tf-async` /
`tbb-flow`) across configurations. Against the natural *collect-then-submit*
formulation (`tf-orig`, which waits for every packet before submitting the
DAG) Tomii is **11–49% faster**, because it dispatches each FFT as its UDP
packet arrives.

The parity result is the point: reaching it with Taskflow/oneTBB required a
**~649-line** hand-written streaming layer (UDP receiver, frame parking, slot
pipelining, core pinning, warm-up) plus per-mode glue. Tomii provides all of
that from a graph and kernels, with no application-specific runtime code.

## FMCW radar vs GNU Radio (reproducible)

A software radar receiver: a sender streams one chirp per UDP packet; the
pipeline runs range FFT → Doppler FFT → CA-CFAR detection → clustering and
reports per-frame latency. Four orchestrators run the same workload: Tomii,
GNU Radio 3.10 with Python blocks, GNU Radio 3.10 with native C++ blocks, and
GNU Radio 4.0. Source: `bench/radar-bench/` (`compare.py`); the workload is
`examples/radar-pipeline/`.

The baselines are the recommended implementations, not a strawman:

- Stock schedulers (3.10 thread-per-block, 4.0 `multiThreaded`), the stock
  native FFT block for the range FFT, and custom source/sink blocks written
  the way GNU Radio documentation prescribes (`gr.sync_block` /
  `gr::Block<>`).
- Both the common deployment style (Python blocks) and the
  latency-practitioner style (native C++ blocks) are measured. They agree
  within 2% at p50 — the gap below is the scheduler, not the block language.
- GNU Radio 4.0 is the ground-up C++23 rewrite built to address 3.10's
  latency. It is included as the strongest available baseline (pinned to
  commit `92278b6`), and it does improve on 3.10: ~3.5× at the tail.
- One deliberate deviation, disclosed: the Doppler/CFAR/clustering stages
  call the same compiled kernel `.so` in every system, so the DSP is
  byte-identical and the measurement isolates orchestration.

Fixtures follow the rules above: same UDP stream and seeded scene, chirps
paced at the physical chirp interval, warm-up excluded, median of 3 runs.
The radar verifier gates every run: all ground-truth targets detected in
every frame, and full frame coverage (a run that drops frames fails, it is
not measured).

Per-frame latency at a 1024-sample × 128-chirp CPI on the same Linux machine.
Every system carries the same ~6.35 ms chirp-arrival floor (127 chirp
intervals × 50 µs — physics, not software), so end-to-end latency is close
across systems; the **post-CPI tail** (work after the last chirp) and the
**core budget** are the informative columns. The GNU Radio baselines are
pinned and tuned — an earlier revision compared against unpinned baselines and
claimed a "same pinned core set"; that is corrected here.

| System | e2e mean / p99.9 | Post-CPI p50 / p99.9 | Cores |
|---|---|---|---|
| **Tomii (T32, W16)** | **6.64 / 6.82 ms** | **310 / 475 µs** | **2.56** |
| GNU Radio 4.0 (tuned) | 6.91 / 7.71 ms | 573 / 1387 µs | 18.9 |
| GNU Radio 3.10 (tuned) | 6.92 / 7.22 ms | 580 / 874 µs | 0.47 |

End-to-end, Tomii is **~1.04×** GNU Radio 4.0 — a match, not a blanket win.
Where it separates is the post-chirp tail (**~1.85×** lower at p50, **~2.9×**
lower at the tail) achieved on **7.4× fewer cores** than GR4 (2.56 vs 18.9).
The mechanism is the same one the MIMO row measures: Tomii dispatches a
range-FFT task the moment each chirp packet arrives, so finishing the frame
after the last chirp costs microseconds of scheduling, not milliseconds of
buffered hand-offs. Caveats, stated plainly: **GR4 wins at small CPIs**
(512×64), and GR3 runs at a very low core budget but with a higher tail. The
honest summary is "matches end-to-end, roughly halves the post-chirp tail, at
a fraction of GR4's cores," not "faster everywhere."

### CPU vs GPU on the same radar graph

The radar kernels exist twice behind one C ABI — FFTW (CPU) and cuFFT (GPU) —
and `run_bench.py --gpu` swaps the `.so` with no graph or plugin change.
Measured on a separate machine (RTX 4090; `examples/radar-pipeline/`,
`run_bench.py` + `bisect_rate.py` with 3/3-robust sustained-rate gating).
These numbers are not comparable with the table above. Source:
`bench/radar-bench/` (`RESULTS-GPU.md`).

**The GPU wins on both latency and sustained rate.** The post-arrival tail is
lower on GPU at every CPI, with tighter jitter. Sustained throughput (robust
3/3): at 4096×512 the GPU holds **1.30×** the CPU rate (50.0 vs 38.5 fps); at
1024×128 all configurations tie at the receiver-bound grid floor; at 2048×256
the GPU is highest but the boundary is high-variance run-to-run — read it as a
**range** (GPU ~111–167 vs CPU ~83–91 fps), not a point. (An earlier revision
claimed "CPU sustains 1.85× the GPU at 4096×512"; that does not reproduce on
current main — the GPU is ahead.)

The useful decoupling result is not which backend wins in isolation but what
the `.so` swap buys under contention:

- **Sharing one GPU across N pipelines:** CUDA MPS is the lever (not
  placement) — with MPS both all-GPU and hybrid scale to N=4.
- **Resilience to an uncontrolled co-tenant:** under a GPU-saturating cuBLAS
  hog, all-GPU is starved (**0/5** runs pass) while the hybrid placement (one
  `.so` swap — CPU range-FFT → GPU Doppler/CFAR → CPU cluster) holds
  GPU-class latency (**4/5** at 4096×512). That resilience is a
  zero-source-line change in Tomii.

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
frame. At 16,384 frame completions this measures **231× faster** in our
evaluation (generational reset ~20 ns flat vs eager ~4.6 µs). *Paper
measurement; not reproducible as a repo microbenchmark.*
The gap widens linearly with frame count — it is structural, not tuned.

## Anti-diagonal wavefront (a loss, reproducible)

Fine-grained single-frame wavefront (N=512, W=1): **Tomii is ~2.4× slower
than Taskflow**. Source: `bench/anti-diag-bench/` (Taskflow harness + data;
there is no TBB wavefront harness, so no TBB comparison is claimed). The cost
is intrinsic — sequentially-consistent dependency counters and 40–85 ns
type-erased dispatch on tasks too small to amortize them. This workload is
outside Tomii's envelope; see [When to use Tomii](/docs/overview/when-to-use).

What the static graph *does* buy on this workload is reconfiguration without a
rebuild: coarsening the tile granularity (1→32) is a graph-only edit worth
**10.6× at W=1**, and switching to a 2D block-DAG is worth **23.5× at W=8** —
in both cases the kernel `.so` *and* the runtime binary stay byte-identical;
only the graph artifact changes.

## Agent tuning: four arms, one verifier

Four search strategies over the generated knob space
(`python -m tomii --knob-space`): runtime CLI knobs plus per-graph knobs
(factor variables, `group_by` widths). Graph knobs can silently violate
workload invariants, so a hard-coded verifier gates every trial — a rejected
trial never counts toward best. Three workloads, best ms/frame among passing
trials. Source: `examples/agent-tuning/`, `run_all.sh`.

| Workload | Agent valid-trial rate | Agent best (ms/frame) |
|---|---|---|
| stream-analytics | 90% | 0.041 |
| pipeline | 90% | 0.87 |
| mimo | 97% | 5.58 |

The black-box optimizers (random, Optuna TPE, grid) stay valid on only
**4–70%** of trials on the same generated space, because it includes
correctness-critical graph knobs that silently violate workload invariants; a
hard-coded verifier gates every trial, so a rejected configuration never
counts toward best.

Two honest robustness points. Near a hard constraint (the MIMO keep-up
deadline) a single-trial best is a mirage — it can pass once and fail on the
next seed — so only configurations **re-validated across K=5 seeds** count,
and the agent is the only arm robust on every seed (5/5). An ablation over the
harness components shows the **machine-readable knob catalog** is the most
load-bearing one: a language model can tune a graph it has never seen only
because the surface is enumerable and verifier-gated. That is the property the
[tuning guide](/docs/guide/tuning/agent-tuning) builds on.

## Reproducing

Each `bench/` directory contains the harness, a verifier, and a README with
the exact methodology. Result CSVs are regenerated outputs and are not
committed; run the sweep scripts to reproduce them. The MIMO benchmark
requires Intel MKL and the Agora packet sender (documented in
`bench/mimo-bench/README.md`).
