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
- **Sustained throughput: GPU ahead, margin shrinks with CPI.** 1024×128 both hit
  the same receiver-bound ceiling (192 fps). 2048×256 GPU sustains **1.20×** the CPU
  rate; 4096×512 GPU keeps a slim **1.09×** (45.5 vs 41.7 fps, robust 3/3). The
  per-chirp H2D + launch + sync tax (512×/frame at 4096) erodes the GPU's throughput
  edge as `n_chirps` grows but doesn't flip it at these sizes.
- **Mixed placement (hybrid: CPU range → GPU doppler/CFAR → CPU cluster) buys
  latency/throughput parity with all-GPU using ~8 GPU launches/frame instead of
  512** — it matches GPU latency, jitter and robust sustained rate (45.5 fps at
  4096), but does **not** exceed pure GPU on this single-graph metric.
- Net: the kernel choice is an **SLA/parallelism decision** measured cleanly by one
  harness — GPU (or the launch-frugal hybrid) for per-frame latency and jitter; CPU
  when GPU cores are the scarce resource.

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

## Table 2 — Max sustained frame rate (gated bisect)

Fastest frame period that passes the coverage/detection gate, compressed streamed
pacing.

| CPI | CPU fps | GPU fps | hybrid fps | note |
|-----|--------:|--------:|-----------:|------|
| 1024×128 | 192.3 | 192.3 | 192.3 | tie — receiver-bound ceiling (~5.2 ms), not compute |
| 2048×256 | 124.2 | **149.3** | 124.2 | **GPU 1.20×**; hybrid = CPU |
| 4096×512 | 41.7 | **45.5** | **45.5** | **GPU & hybrid 1.09× CPU** (robust 3/3) |

- 1024×128 / 2048×256: fastest period the gated bisect confirmed (single confirm
  run, 220/220 frames).
- **4096×512: robust measurement.** The single-run gated bisect is *bistable* at
  4096 — its one-shot bounds (CPU 51.6, GPU 47.8, hybrid 61.3 fps) each failed a
  re-run at the reported period. So the 4096 row is instead the **fastest 5 ms-grid
  period that passes 3/3 verifier-gated repeats** (200 frames, compressed pacing):
  CPU 24 ms (41.7 fps), GPU & hybrid 22 ms (45.5 fps). Treat the sub-2 ms bisect
  bounds as optimistic; the 3/3 numbers are the defensible ones. (The optimistic
  hybrid 61 fps did **not** survive the 3/3 check — a caution about boundary noise.)
- **Crossover:** GPU leads sustained rate at mid CPI (2048×256, 1.20×). At the
  largest CPI GPU keeps a slim lead (1.09×) once measured robustly — the per-chirp
  launch tax (512×/frame H2D + launch + sync) narrows the GPU's throughput edge as
  `n_chirps` grows, but at these sizes 8 CPU cores don't overtake it.

## Interpretation

The two faces of the same mechanism: cuFFT clears a resident CPI in a few hundred
µs with almost no variance (flat ~90 µs jitter across a 16× compute range), so the
**GPU owns per-frame latency and tail**. But every chirp costs the GPU a
host→device copy, a kernel launch and a stream sync; that per-chirp overhead scales
with `n_chirps`, so as the CPI grows the GPU's **sustained-throughput** margin over
the CPU shrinks (1.20× at 2048 → 1.09× at 4096). It does not flip to a CPU win at
these sizes — the published headline (CPU ~1.85× at 4096×512) is a stronger,
opposite-signed effect that this box/config (RTX 4090, 8 workers) does not reproduce;
worth reconciling before the paper leans on a CPU-throughput-win claim.

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
- **Throughput:** at 4096×512 hybrid ties GPU at the robust 45.5 fps (both 22 ms,
  3/3); at 2048×256 hybrid matches CPU (124 fps), below GPU (149). So removing the
  per-chirp launches recovers GPU-class latency **and** GPU-class robust throughput,
  but does not push past pure GPU here.
- **Where it could still pay (not measured here):** the hybrid consumes ~8 GPU
  launches/frame vs ~528 for all-GPU, so it frees GPU launch/queue capacity — likely
  to matter when the GPU is *shared* across concurrent graphs, or on GPUs with
  higher launch latency. That is the promising follow-up for a "decoupling buys you
  something" claim; on a single dedicated graph it buys launch-frugality at parity.
- **Method caution surfaced:** the single-run gated bisect flagged hybrid at 61 fps
  (4096); a 3/3-repeat check cut that to 45.5 fps. Boundary rates here are bistable —
  report the robust (multi-pass) number.

## Caveats

1. Numbers are from one shared multi-GPU host; absolute rates depend on the box.
   The **ratios** (GPU latency/jitter advantage, throughput crossover direction)
   are the portable result.
2. p99.9 at 4096×512 rests on 350 steady frames/run × 5 runs; the deep tail is
   coarser there than at 1024×128 (1800 frames/run).
3. Result CSV/JSON and IQ scenes are git-ignored (repo policy); this file plus
   `gpu_crossover.py` are the committed, reproducible record.
4. Sustained rates at 1024/2048 are single-confirm gated-bisect values; at 4096
   they are the robust 3/3-pass measurement (the single-run bisect is bistable
   there and over-reports — see Table 2). Treat 1024/2048 as mild upper bounds too.

## Reproduce

```bash
# from repo root; CPU/hybrid kernels need conda fftw3f on PKG_CONFIG_PATH
export PKG_CONFIG_PATH=$HOME/miniconda3/envs/radar/lib/pkgconfig
python3 bench/radar-bench/gpu_crossover.py --gpu-device 1 --phase all   # cpu, gpu, hybrid
# -> bench/radar-bench/results/gpu-crossover/summary.json (git-ignored)
```

Underlying harness (all committed): `run_bench.py --gpu|--hybrid`,
`bisect_rate.py --gate-search [--gpu|--hybrid]`, `kernels/radar_hybrid.cu`
(+ `make -C kernels hybrid`). The 4096×512 robust 3/3 sustained rates were taken
with `run_bench.py --repeat 3` under the coverage gate across a 5 ms period grid.
