# Radar CPU (FFTW) vs GPU (cuFFT) crossover — eval/radar-gpu

End-to-end FMCW radar pipeline (`examples/radar-pipeline`), same graph / plugin /
verifier / scene for every backend — **only the kernel `.so` changes** (FFTW on
the CPU vs cuFFT on the GPU). Every number below comes from a run whose
detections passed the ground-truth coverage gate (every target in every frame,
exact frame count). Regenerate with `bench/radar-bench/gpu_crossover.py`.

## TL;DR

- **Latency & jitter: GPU wins at every CPI, and the margin grows with CPI.**
  The per-frame processing tail (critical-path exec) is **4.4× / 10.0× / 17.3×**
  lower on GPU at 1024×128 / 2048×256 / 4096×512, and GPU jitter (p99.9−p50) is
  **5× / 21× / 83×** tighter. Once a CPI is resident, cuFFT clears it almost
  instantly and deterministically.
- **Sustained throughput: GPU wins at every CPI (robust 3/3).** 1024×128 all three
  tie at the receiver-bound grid floor (≥208 fps). 2048×256 GPU sustains **1.69×**
  the CPU rate (153.8 vs 90.9 fps); 4096×512 GPU keeps **1.09×** (45.5 vs 41.7). The
  per-chirp H2D + launch + sync tax (512×/frame at 4096) shrinks the GPU's margin as
  `n_chirps` grows, but on this box it never flips to a CPU win. **The website's "CPU
  sustains 1.85× GPU at 4096×512" does not reproduce on current main.**
- **Mixed placement (hybrid: CPU range → GPU doppler/CFAR → CPU cluster) reaches
  GPU-class latency/jitter using ~8 GPU launches/frame instead of 512.** On robust
  throughput it beats CPU (2048×256: 111 vs 91 fps) and ties GPU at 4096×512 (45.5),
  but does **not** exceed pure GPU on a single dedicated graph.
- **Where the hybrid actually pays off: shared/contended GPU.** Running N pipelines
  on one GPU, hybrid sustains far more aggregate throughput at 2048×256 (N=4: 167 fps
  vs all-GPU cannot sustain N=4 at all; N=2: 200 vs 125), and under an adversarial GPU
  hog it keeps GPU-class latency (2048: 8.5 ms, fully sustained) where all-GPU is
  starved (wedged; 4096: 197 ms). It does *not* help at 4096×512, which is GPU-compute
  -bound. So decoupling buys resilience when the pipeline is GPU-**launch**-bound or
  the GPU is contended — via one `.so` swap, no graph/runtime change.
- Net: the kernel choice is an **SLA/parallelism decision** measured cleanly by one
  harness — GPU (or the launch-frugal hybrid) for per-frame latency and jitter; the
  hybrid to share a contended GPU across pipelines; CPU when GPU cores are the scarce
  resource.

## Provenance

| | |
|---|---|
| Branch / base | `eval/radar-gpu` off `main` `ffb51d3` |
| Tomii SHA | `ffb51d30558c4c09c6958217e56679cc525edbd5` |
| GPU | NVIDIA GeForce RTX 4090 (device index 1) |
| Driver / CUDA | 575.57.08 / CUDA 12.9 (`nvcc` 12.9.r12.9), `sm_89` |
| CPU FFTW | fftw3f 3.3.11 (conda env `radar`) |
| Tomii config | 8 workers, 2 slots, 2 system threads, 2 receiver threads, `core_offset=1` |
| Low-latency cfg | custom scheduler + coalesce_barriers + inline_continuation + slot_priority + RDTSC |
| Measured | 2026-09-25, this box (shared multi-GPU host; device 1 was idle) |

Device 1 (not the GPU-5 house default) was used because GPU 5 was non-idle at
measurement time and prior radar-crossover work also ran on device 1; all six
GPU cells ran on the same device.

## Method

- **Scenes** (`data/out/cpi_*`, git-ignored): three CPIs at fs=20.48 MHz,
  B=307.2 MHz, fc=77 GHz. `chirp_interval = n_samples/fs` → 50/100/200 µs →
  **physical CPI 6.4 / 25.6 / 102.4 ms**. Three targets shared across sizes
  (`30 m,+2 m/s`, `75 m,−3 m/s`, `120 m,+1 m/s`), low velocity so they stay inside
  every size's unambiguous window (v_max shrinks with CPI: ±19.5/±9.7/±4.9 m/s),
  **uniform 6 dB per-sample SNR** — the 2D-FFT processing gain (51–63 dB) makes
  detection certain while keeping target skirts low enough to avoid the
  strong-target CA-CFAR self-masking seen with 20–30 dB targets at 4096×512.
  So the coverage gate reflects throughput/latency, not detection sensitivity.
- **Latency cells:** frame period = **1.45 × physical CPI** (~69% duty),
  chirps paced at the physical chirp interval, `--repeat 5`, verifier-gated,
  per-column **median of 5**. Frame latency is first-packet-to-done = the physical
  CPI arrival span **plus** the processing tail; the tail is where CPU/GPU differ.
  (Frame period = 1×CPI, i.e. 100% duty, drops occasional packets at small CPI and
  fails the coverage gate — hence the 1.45× headroom.)
- **Max sustained rate:** `bisect_rate.py --gate-search` (added on this branch) —
  bisects the frame period under compressed streamed pacing
  (`chirp_gap = period/n_chirps`) with the **coverage/detection verifier on at
  every step**, so P* is the fastest rate that *passes the gate*, i.e. the
  baseline at its best. `--tol-ms 2`, 250 frames/step, warmup 30.
- **Metric definitions:** `p50/p99/p99.9` = per-frame latency percentiles from the
  runtime JSON report; **processing tail** = `critical_path_exec_us` from the
  report's scheduling diagnostic (per-frame compute on the critical path, minus
  chirp-arrival wait).

### Detection parity: why SNR was lowered 30 → 6 dB

The initial scenes used strong 20–30 dB targets (matching the README big-scene). At
4096×512 the **CPU/FFTW path intermittently missed the 30 dB target while the GPU/
cuFFT path detected it** — the two backends disagreed on detection, which shouldn't
happen. Root cause is a **numerical-precision difference at a near-threshold
operating point**, not a logic bug:

- A very strong target self-masks in CA-CFAR: its spectral skirt leaks into the
  training window (guard=2, train=8), inflating the local noise estimate and pushing
  the target's own cell toward its threshold.
- The two CFAR kernels sum that window differently in **float32**: the CPU
  (`radar_cpu.c`) keeps a **sliding-window running sum** along the Doppler axis
  (`s_out += col[d+K+1] − col[d−K]`), which accumulates rounding error and suffers
  catastrophic cancellation when subtracting ~1e19-scale leakage terms; the GPU
  (`radar_gpu.cu`) recomputes each window with a **direct per-cell sum**. Algebraically
  identical, but at a self-masking margin they round to opposite sides of the
  threshold. (The GPU comment "same result" holds away from such margins.)

Dropping to a uniform 6 dB per-sample SNR (still 51–63 dB after 2D-FFT gain) removes
the margin, so both backends detect all targets in all frames and the coverage gate
measures latency/throughput, not detection sensitivity. Worth noting for the paper:
the "same kernel, swap the .so" parity is exact except at detection margins; a
robuster CFAR accumulation (Kahan or fp64 sums, or more guard cells) would close even
that gap.

## Table 1 — Frame latency & processing tail (median of 5, verifier-gated)

Latency cells at 1.45× physical CPI, physical chirp pacing.

| CPI | backend | p50 (ms) | p99 (ms) | p99.9 (ms) | jitter p99.9−p50 (µs) | proc. tail (ms) |
|-----|---------|---------:|---------:|-----------:|----------------------:|----------------:|
| 1024×128 | CPU | 7.31 | 7.65 | 7.82 | 509 | 0.77 |
| 1024×128 | **GPU** | **6.62** | **6.68** | **6.72** | **95** | **0.17** |
| 1024×128 | hybrid | 6.63 | 6.68 | 6.79 | 156 | 0.19 |
| 2048×256 | CPU | 29.03 | 30.24 | 30.89 | 1858 | 2.51 |
| 2048×256 | **GPU** | **25.89** | **25.96** | **25.98** | **87** | **0.25** |
| 2048×256 | hybrid | 26.08 | 26.13 | 26.16 | 81 | 0.38 |
| 4096×512 | CPU | 115.69 | 119.60 | 123.06 | 7373 | 9.69 |
| 4096×512 | **GPU** | **103.11** | **103.19** | **103.20** | **89** | **0.56** |
| 4096×512 | hybrid | 104.04 | 104.15 | 104.16 | 118 | 1.18 |

*hybrid* = CPU/FFTW range-FFT → GPU/cuFFT doppler+CFAR → CPU cluster (mixed
placement; see below). It tracks GPU latency and jitter closely at every size.

- **Processing-tail advantage (GPU):** 4.4× (1024) → 10.0× (2048) → 17.3× (4096).
- **Jitter advantage (GPU):** 5× → 21× → 83×. GPU p99.9−p50 stays ~90 µs at every
  size; CPU jitter grows with CPI (more chirps → more scheduling variance).
- p50 is arrival-dominated (≈ physical CPI) by construction; the CPU/GPU gap in
  p50 (0.7 / 3.1 / 12.6 ms) is exactly the processing-tail gap.

## Table 2 — Max sustained frame rate (robust 3/3, verifier-gated)

Fastest grid period that passes **3/3** verifier-gated repeats (200 frames,
compressed streamed pacing) — the defensible sustained rate. See note on why the
single-run bisect is not used here.

| CPI | CPU fps | GPU fps | hybrid fps | note |
|-----|--------:|--------:|-----------:|------|
| 1024×128 | ≥208 | ≥208 | ≥208 | tie — all pass at the grid floor (4.8 ms); receiver/loopback-bound, not compute |
| 2048×256 | 90.9 | **153.8** | 111.1 | **GPU 1.69× CPU**; hybrid 1.22× CPU |
| 4096×512 | 41.7 | **45.5** | **45.5** | **GPU & hybrid 1.09× CPU** |

- **Why 3/3 and not the single-run bisect.** Sustained-rate boundaries here are
  *bistable*: a period can pass once and fail on a re-run. The single-run gated
  bisect over-reports — e.g. it claimed 2048 CPU 124 fps (robust 90.9), 4096 GPU
  47.8 / hybrid 61.3 fps (robust 45.5 / 45.5). So every rate above is the fastest
  period passing 3 consecutive gated runs (CPU 2048 fails at 10 ms; GPU 2048 at
  6.0 ms; hybrid 2048 at 8 ms; at 4096 CPU fails at 22 ms, GPU/hybrid at 20 ms).
  1024×128 still passes 3/3 at the fastest grid point (4.8 ms, ≥208 fps) for all
  three — the loopback receiver path binds there, not compute.
- **GPU wins throughput at every CPI**; the margin is largest at mid CPI (2048,
  1.69×) and narrows to 1.09× at 4096 as the per-chirp launch tax (512×/frame H2D +
  launch + sync) grows. It does **not** flip to a CPU win at any size on this box —
  contrary to the website's "CPU 1.85× at 4096×512" (see Interpretation).
- **Hybrid** sits between CPU and GPU on throughput, reaching GPU parity at 4096.

## Interpretation

The two faces of the same mechanism: cuFFT clears a resident CPI in a few hundred
µs with almost no variance (flat ~90 µs jitter across a 16× compute range), so the
**GPU owns per-frame latency and tail**. But every chirp costs the GPU a
host→device copy, a kernel launch and a stream sync; that per-chirp overhead scales
with `n_chirps`, so as the CPI grows the GPU's **sustained-throughput** margin over
the CPU shrinks (1.69× at 2048 → 1.09× at 4096, robust 3/3). It does not flip to a
CPU win at any size here — the published headline (CPU ~1.85× at 4096×512) is a
stronger, opposite-signed effect that this box/config (RTX 4090, 8 workers, main
`ffb51d3` with the per-slot cuFFT + per-(slot,tile) CFAR-stream fixes) does not
reproduce. **Reconcile which machine/commit produced the 1.85× before the paper
leans on a CPU-throughput-win claim** — as measured now the GPU is ahead throughout.

## Mixed placement (hybrid CPU+GPU)

Does splitting one graph across devices recover both latency and throughput? The
**hybrid** twin runs range-FFT on the CPU (FFTW, per chirp, as packets stream in)
and doppler-FFT + CA-CFAR on the GPU (cuFFT), clustering on the CPU — the same rd/
power/dets layout, so only the kernel `.so` changes (`--hybrid`;
`kernels/radar_hybrid.cu`). The motive: the all-GPU twin launches ~`n_chirps`
per-chirp range kernels/frame (512 at 4096×512); doing range on the CPU cuts that to
one bulk host→device upload per Doppler tile (~`n_tiles`=8/frame).

Result — **parity with all-GPU at a fraction of the GPU launches, not a win:**

- **Latency/jitter:** hybrid tracks GPU closely (Table 1). At 4096×512 the
  processing tail is 1.18 ms (GPU 0.56, CPU 9.69) and jitter is 118 µs (GPU 89, CPU
  7373) — GPU-class, far from CPU.
- **Throughput (robust 3/3):** at 4096×512 hybrid ties GPU at 45.5 fps (both 22 ms);
  at 2048×256 hybrid sustains 111 fps — above CPU (91) but below GPU (154). So
  removing the per-chirp launches recovers GPU-class latency and closes most of the
  throughput gap to GPU, but does not push past pure GPU on one dedicated graph.
- **Where it could still pay (not measured here):** the hybrid consumes ~8 GPU
  launches/frame vs ~528 for all-GPU, so it frees GPU launch/queue capacity — likely
  to matter when the GPU is *shared* across concurrent graphs, or on GPUs with
  higher launch latency. That is the promising follow-up for a "decoupling buys you
  something" claim; on a single dedicated graph it buys launch-frugality at parity.
- **Method caution surfaced:** the single-run gated bisect flagged hybrid at 61 fps
  (4096); a 3/3-repeat check cut that to 45.5 fps. Boundary rates here are bistable —
  report the robust (multi-pass) number.

## Shared-GPU: concurrent pipelines & contention (the decoupling payoff)

Does launch-frugal mixed placement let more radar pipelines share one GPU? `N`
independent Tomii processes (N∈{1,2,4}) share the one RTX 4090, each pinned to a
disjoint CPU core set (4 workers, stride 6) with its own UDP port, sender, graph and
report — no CUDA MPS. Only the kernel `.so` differs (all-GPU vs hybrid). Coverage
gate is *soft* here: detection-correct **and** ≥98% of frames present (tolerates the
1-frame EOS-flush artifact; real overload loses far more). Harness:
`bench/radar-bench/shared_gpu.py`. Part B is single-trial per grid point (boundary is
bistable — read the direction, not the last digit); Part C is single-trial.

**Part B — max aggregate sustained rate (fps) vs N** (per-pipeline fps = agg / N):

| CPI | backend | N=1 | N=2 | N=4 |
|-----|---------|----:|----:|----:|
| 2048×256 | GPU | 142.9 | 125.0 | **fail** |
| 2048×256 | **hybrid** | **166.7** | **200.0** | **166.7** |
| 4096×512 | GPU | 38.5 | 41.7 | 45.5 |
| 4096×512 | hybrid | 45.5 | 38.5 | 38.5 |

- **2048×256: hybrid wins concurrency decisively.** All-GPU aggregate *drops* as N
  grows (143 → 125 → cannot sustain N=4 even at the slowest grid); hybrid *holds/rises*
  (167 → 200 → 167). Moving the 256 per-chirp range launches/frame off the GPU removes
  the launch/context contention that N all-GPU pipelines pile onto the device.
- **4096×512: no hybrid advantage.** Both sit at ~40–45 fps aggregate regardless of N
  — here the GPU is *compute*-bound on the 16×-larger Doppler/CFAR, which both
  placements still run on the GPU, so freeing the range launches doesn't add capacity.

**Part C — adversarial GPU hog** (a background cuBLAS matmul loop pins the device at
~100%), N=1 radar pipeline at the fixed rate (2048: 8 ms; 4096: 30 ms):

| CPI | GPU (all) | hybrid |
|-----|-----------|--------|
| 2048×256 | **wedged** — no frames complete | **PASS**, p50 8.5 ms, all frames |
| 4096×512 | **starved** — p50 197 ms, 2 frames | p50 ~30 ms (GPU-class) but drops >2% → soft-fail |

- Under GPU contention the all-GPU pipeline is starved (its 256–512 range launches/
  frame fight the hog); the hybrid runs range on the CPU and lands only ~8 GPU
  launches/frame, which slot between the hog's kernels — keeping GPU-class latency
  (2048: fully sustained; 4096: ~6× lower tail than all-GPU even while shedding a few
  frames).

**Takeaway for "decoupling buys you something":** mixed placement pays off precisely
when the pipeline is GPU-**launch**-bound — a smaller CPI (2048×256) where per-chirp
launches dominate, and under GPU **contention** (concurrent pipelines or a co-tenant).
It does **not** help when the pipeline is GPU-**compute**-bound (4096×512 Doppler/CFAR
saturates the device either way). The lever is one kernel `.so` swap — no graph,
plugin or `tomii-core` change — which is the paper-relevant point: per-stage placement
is a cheap knob that recovers a contended accelerator.

## Caveats

1. Numbers are from one shared multi-GPU host; absolute rates depend on the box.
   The **ratios** (GPU latency/jitter advantage, throughput crossover direction)
   are the portable result.
2. p99.9 at 4096×512 rests on 350 steady frames/run × 5 runs; the deep tail is
   coarser there than at 1024×128 (1800 frames/run).
3. Result CSV/JSON and IQ scenes are git-ignored (repo policy); this file plus
   `gpu_crossover.py` are the committed, reproducible record.
4. All Table 2 sustained rates are the robust 3/3-pass measurement; the single-run
   gated bisect (still committed for exploration) over-reports because the boundary
   is bistable. 1024×128 is a floor (grid stopped at 4.8 ms; true ceiling ≥208 fps).
5. **The website's "CPU sustains 1.85× GPU throughput at 4096×512" is NOT reproduced
   on current main (ffb51d3): the GPU is 1.09× ahead robustly, and ahead at every
   size.** Likely the 1.85× predates the per-slot cuFFT / CFAR-stream fixes or used a
   different GPU. Reconcile the source before relying on a CPU-throughput-win claim.
6. Shared-GPU (Part B/C) is single-trial per grid point with the soft coverage gate
   and no CUDA MPS; read the *direction* (large gaps like N=4 fail-vs-167 fps), not the
   last digit. A 3/3-robust + MPS-on sweep would firm up the concurrency boundary.

## Reproduce

```bash
# from repo root; CPU/hybrid kernels need conda fftw3f on PKG_CONFIG_PATH
export PKG_CONFIG_PATH=$HOME/miniconda3/envs/radar/lib/pkgconfig
python3 bench/radar-bench/gpu_crossover.py --gpu-device 1 --phase all   # cpu, gpu, hybrid
# -> bench/radar-bench/results/gpu-crossover/summary.json (git-ignored)
```

```bash
# robust 3/3 sustained rates (Table 2):
python3 bench/radar-bench/robust_rate.py --gpu-device 1
```

```bash
# shared-GPU concurrent-pipelines + adversarial-hog experiment (Table B/C):
python3 bench/radar-bench/shared_gpu.py            # parts B,C by default
```

Underlying harness (all committed): `run_bench.py --gpu|--hybrid`,
`bisect_rate.py --gate-search [--gpu|--hybrid]`, `robust_rate.py` (3/3 grid),
`shared_gpu.py` (N concurrent pipelines + GPU hog), `kernels/radar_hybrid.cu`
(+ `make -C kernels hybrid`).
