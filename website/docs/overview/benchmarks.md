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

The pipeline runs with the **same kernels on both sides**, LDPC decode
included, so the comparison is apples-to-apples with Agora's published numbers.
This isolates the price of Tomii's decoupling from any kernel difference.

**The kernels run at parity.** Tomii's LDPC decode is 37.5 µs/task against
Agora's published 46.5 µs, and Tomii's output is byte-identical from W=1 to
W=26.

**The latency cost of generality is bounded and small.** On Agora's 64×16
uplink frame (full decode, 26 worker threads), Tomii runs **drop-free at a
1.1 ms frame period** with end-to-end latency **1.42 ms median / 1.65 ms p99.9**
(post-arrival 0.23 / 0.34 ms). Agora's published figures are **1.19 / 1.29 ms**
at a 1.0 ms period on 4× Xeon Gold 6130, and 1.12–1.13 / 1.15–1.19 ms on newer
Xeons. So the generality gap is **+0.23 ms median / +0.36 ms p99.9** against the
6130 numbers (up to +0.30 / +0.50 ms against the newer-Xeon rows) — at a 10%
longer frame period, over kernel UDP against Agora's DPDK fast path. At the
1.0 ms period Tomii stays drop-free (1.36 ms median) but its tail queues
(p99.9 ~13 ms); 1.1 ms is where it holds a bounded tail. Beyond the 26 workers
Tomii uses 13 support cores (one main, four resolution, eight receiver), against
Agora's fused design.

**The remaining post-arrival time is tunable configuration, not decode.** A
~30-line graph edit — replacing the full decode barrier with a per-symbol
grouped barrier — cuts median post-arrival latency from 0.78 ms to **0.23 ms
(−70%)** and p99.9 from 0.99 ms to 0.34 ms, with bit-identical output and
byte-identical kernels and runtime.

The takeaway is modularity-first: the kernels run at parity and the gap to a
fused DPDK engine is a few tenths of a millisecond at a slightly longer frame
period. What the decoupling buys is the reconfiguration surface — subcarrier
count, scheduling policy, and kernel changes are graph edits or CLI flags, where
in Agora they are manager-code changes and recompilation.

*The Agora figures are its published numbers (CoNEXT'20, Tables 4–5); no Agora
binary is run, so this comparison is not reproducible from this repository.*

## MIMO uplink vs Taskflow / oneTBB (reproducible)

Like-for-like comparison: every harness links the identical Intel kernel
`.so` binaries, so timing differences are pure orchestration. Two Taskflow
formulations are measured — a per-packet arrival-driven one (`tf-async`) and
the collect-then-submit DAG (`tf-orig`) — plus an oneTBB `flow_graph`
(`tbb-flow`). Source: `bench/mimo-bench/`.

Mean per-frame latency (ms), best configuration per system:

| Configuration | Tomii | tf-async | tbb-flow |
|---|---|---|---|
| 4×4, S=4 | 0.965 | 0.951 | 0.947 |
| 16×16, S=8 | 5.902 | 5.882 | 5.876 |
| 16×16, S=16 (steady) | 3.014 | 2.988 | 2.981 |
| 64×16, S=8 | 23.451 | 23.480 | 23.437 |

Median of 5 runs, byte-exact verified, no decode.

**Tomii matches the best hand-written execution — parity, not a win.** It
lands within **0.1–1.9%** of the fastest per-packet baseline (`tf-async` /
`tbb-flow`) across configurations (and edges ahead of `tf-async` at 64×16).
Against the natural *collect-then-submit* formulation (`tf-orig`, which waits
for every packet before submitting the DAG) Tomii is **1.07–2.9× faster**
(6–66%, the gap widening with more in-flight frames), because it dispatches each
FFT as its UDP packet arrives; it is still **1–12% faster** than the best
static-DAG variant.

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
| **Tomii (T32, W16)** | **6.65 / 6.77 ms** | **315 / 422 µs** | **2.23** |
| GNU Radio 4.0 (tuned) | 6.90 / 6.95 ms | 567 / 613 µs | 18.9 |
| GNU Radio 3.10 (tuned) | 6.92 / 7.19 ms | 578 / 853 µs | 0.47 |

End-to-end, Tomii is **~1.04×** GNU Radio 4.0 — a match, not a blanket win,
because the ~6.35 ms chirp window dominates every system. Where it separates is
the post-chirp tail: **1.8× lower at the median** (against both GR versions) and
**1.45× (vs GR4) to 2.0× (vs GR3) lower at p99.9**, achieved on **8.5× fewer
cores than GR4** (2.23 vs 18.9). Sustained throughput points the same way —
Tomii holds **1195 fps** with 2 frames in flight and **1435 fps** with 8,
against GR3/GR4's 1088/1190 fps (all UDP-receiver-bound). The mechanism is the
same one the MIMO row measures: Tomii dispatches a range-FFT task the moment
each chirp packet arrives, so finishing the frame after the last chirp costs
microseconds of scheduling, not milliseconds of buffered hand-offs. Caveats,
stated plainly: **GR4 wins at small CPIs** (512×64), and GR3 runs at a far lower
core budget (0.47) though with a higher tail. The honest summary is "matches
end-to-end, ~1.8× lower post-chirp tail at the median, at a fraction of GR4's
cores," not "faster everywhere."

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

Tomii does not win this benchmark; the point is that the per-frame gap
*closes* as slots pipeline. A linear 4-stage pipeline with a fixed per-task
compute kernel, swept over S concurrent frames. Source: `bench/pipeline-bench/`.

At S=1 Tomii pays a fixed per-frame runtime overhead and runs several-fold
slower than Taskflow; by S≥16 multi-slot amortization spreads the
resolution-thread cost across lanes and the gap narrows to a small constant
factor. *The per-frame latency comparison has not been re-measured on the
current evaluation pin — treat the S-scaling result qualitatively until the
harness is re-run.*

**Memory growth**, same benchmark (re-measured on the pin): Tomii adds
**+12–14 kB per slot** against Taskflow's **+130 kB** — an **~10× lower growth
rate** (`/usr/bin/time -v`, W=4, slope from S=1 to S=64, same glibc malloc
settings for both). The caveat is the floor: at S=1 Tomii's RSS is **~9.1 MB**
against Taskflow's **~4.8 MB** (pre-allocated worker stacks and resolution-thread
machinery). The advantage is the slope — it matters for long-running services
with many in-flight frames, not the base.

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
**6.4× at W=1**, and switching to a 2D block-DAG is worth **23× at W=8** — in
both cases the kernel `.so` *and* the runtime binary stay byte-identical; only
the graph artifact changes.

## Agent tuning: four arms, one verifier

Four search strategies over the generated knob space
(`python -m tomii --knob-space`): runtime CLI knobs plus per-graph knobs
(factor variables, `group_by` widths). Graph knobs can silently violate
workload invariants, so a hard-coded verifier gates every trial — a rejected
trial never counts toward best. Three workloads, best ms/frame among passing
trials. Source: `examples/agent-tuning/`, `run_all.sh`.

| Arm | stream-analytics | pipeline | mimo |
|---|---|---|---|
| Random | 4% / 0.11 | 38% / 1.28 | 83% / 1.62 |
| Optuna TPE | 18% / 0.19 | 68% / 0.89 | 87% / 1.62 |
| Grid | 4% / 0.090 | 36% / 0.92 | 90% / 2.05 |
| **Agent (`claude-sonnet-5`)** | **92% / 0.042** | **94% / 0.87** | **93% / 1.45** |

Each cell is *valid-trial rate / best ms-per-frame among passing trials*
(5 seeds; each run's best re-measured 5×). Workload defaults for reference:
0.134 / 17.35 / 23.29 ms/frame. On MIMO the agent's 1.45 ms/frame is a **16×**
improvement over the default and **1.12× faster** than the best black-box arm.

The black-box optimizers (random, Optuna TPE, grid) stay valid on only
**4–90%** of trials on the same generated space, because it includes
correctness-critical graph knobs that silently violate workload invariants; the
hard-coded verifier gates every trial, so a rejected configuration never counts
toward best.

Robustness is seed-dependent, and the honest picture is mixed. Every arm's
single best re-validates 5/5 when re-measured. Counting seeds whose best lands
within 10% of the best known configuration: on **pipeline** the agent is 5/5
against the best black-box arm's 4/5; on **MIMO** the agent is 4/5 against
≤2/5; on **stream-analytics** the agent is only 1/5 — a null result there (the
best-known config is an outlier). An ablation over the harness components shows
the **machine-readable knob catalog** is the most load-bearing one: a language
model can tune a graph it has never seen only because the surface is enumerable
and verifier-gated. That is the property the
[tuning guide](/docs/guide/tuning/agent-tuning) builds on.

## Live reconfiguration

Changing a deployment — a new kernel, a different core layout, a code fix — is a
graph edit or a CLI flag in Tomii, not a rebuild of the orchestrator. On a
streaming MIMO 4×4 workload at a 1 ms frame period:

- **Restart with a rebuild:** Tomii loses **1,241 frames (~1.24 s)** of the
  stream against Taskflow's 6,179 and oneTBB's 5,894 — the baselines recompile
  the orchestrator, Tomii only the plugin.
- **A core-affinity change** is a flag in Tomii (**75 frames** lost) versus a
  rebuild for the baselines (~5,900–6,200 frames).
- **Prebuilt restart** (binary already built) costs **54–91 frames** for every
  system — the floor is process restart, not compilation.
- **Live socket handover** (`--handover-listen` / `--takeover`): a replacement
  process takes the bound sockets over from the running one, losing **1 frame**
  (a ~2.7 ms gap), with worst post-switch latency 0.97–1.18 ms against 0.96 ms
  steady state. This is a standard technique — socket passing over a Unix-domain
  socket — supported out of the box for network graphs on Linux.

## Reproducing

Each `bench/` directory contains the harness, a verifier, and a README with
the exact methodology. Result CSVs are regenerated outputs and are not
committed; run the sweep scripts to reproduce them. The MIMO benchmark
requires Intel MKL and the Agora packet sender (documented in
`bench/mimo-bench/README.md`).
