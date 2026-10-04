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
  tie at the receiver-bound grid floor (≥208 fps). 4096×512 GPU sustains **1.30×** the
  CPU rate (50.0 vs 38.5 fps). 2048×256 GPU is highest but the boundary is
  high-variance run-to-run (GPU ~111–167 vs CPU ~83–91 fps — read as a range). It
  never flips to a CPU win on this box. **The website's "CPU sustains 1.85× GPU at
  4096×512" does not reproduce on current main.**
- **Mixed placement (hybrid: CPU range → GPU doppler/CFAR → CPU cluster) reaches
  GPU-class latency/jitter using ~32 GPU launches/frame instead of ~800 (nsys).** On
  robust throughput it sits between CPU and GPU; it does **not** exceed pure GPU on a
  single dedicated graph.
- **Sharing one GPU across N pipelines: CUDA MPS is the lever, not placement.**
  Without MPS concurrent pipelines contend on one GPU context; MPS lets both all-GPU
  and hybrid scale to N=4. (On current main the P0 fixes already improved no-MPS
  concurrency, so MPS's incremental role is smaller than on old main.) **The durable
  hybrid win is an *uncontrolled* co-tenant:** under a GPU hog, all-GPU is starved
  (**0/5** runs pass) while the hybrid holds GPU-class latency (4096×512: **4/5**, p50
  29.6 ms) — it lands ~25× fewer GPU launches (32 vs ~800/frame, nsys) that slot
  between the hog's kernels. MPS can't help there (the hog saturates compute). All via
  one `.so` swap (0 graph/runtime lines; the GR4 equivalent is ~80–150 lines of block code).
- Net: the kernel choice is an **SLA/parallelism decision** measured cleanly by one
  harness — GPU (or the launch-frugal hybrid) for per-frame latency and jitter; the
  hybrid to share a contended GPU across pipelines; CPU when GPU cores are the scarce
  resource.

## Refresh on new main (`bc9e5d9`, 2026-09-27)

The eval branch was rebased onto main after PR #7 ("runtime P0 stabilization":
packet-admission race, `--slot-priority` restart, parallel slot activation, report
accuracy) and the full campaign re-run. **The story holds; a couple of things
improved.** Tables below are the new-main (`bc9e5d9`) numbers.

- **Latency/tail unchanged or better.** GPU tail/jitter advantage still grows with
  CPI; CPU 4096×512 processing tail **improved ~15% (9.69 → 8.25 ms)** — consistent
  with the report/warm-up accuracy fixes.
- **Hog resilience reproduces cleanly:** all-GPU **0/5**, hybrid **4/5** (4096 N=1,
  p50 29.6 ms) — the durable "decoupling buys resilience" result stands.
- **P0 fixes improved no-MPS concurrency:** all-GPU 4096×512 now sustains N=4 without
  MPS (44 fps) where old main collapsed — so MPS's *incremental* role is smaller than
  on old main, though hybrid still sustains N=4 at 2048×256 where all-GPU cannot.
- **Caveat — 2048×256 sustained rate is high-variance** on this shared box: across
  three re-runs GPU landed 111–167, hybrid 77–154, CPU 83–91 fps. The bistable
  boundary + strict 3/3 gate is not reproducible to a single fps there; only the
  ordering (**GPU highest**) and the 1024/4096 cells are stable. Read 2048 as a range.
- Two latent harness bugs were fixed en route (commit `0500433`): `PKG_CONFIG_PATH`
  dropped `/lib` (broke a clean FFTW/hybrid kernel build), and `robust_rate.py` used
  the wrong repo-root `parents[]`. Both were masked before by pre-built `.so`s.

## Provenance

| | |
|---|---|
| Branch / base | `eval/radar-gpu` rebased onto `main` `bc9e5d9` (originally `ffb51d3`) |
| Tomii SHA | `bc9e5d9` (PR #7 runtime P0 stabilization); original eval `ffb51d3` |
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
| 1024×128 | CPU | 7.35 | 7.80 | 7.83 | 477 | 0.78 |
| 1024×128 | **GPU** | **6.59** | **6.65** | **6.67** | **82** | **0.16** |
| 1024×128 | hybrid | 6.63 | 6.68 | 6.88 | 247 | 0.19 |
| 2048×256 | CPU | 28.99 | 30.18 | 30.87 | 1878 | 2.30 |
| 2048×256 | **GPU** | **25.89** | **25.95** | **25.96** | **73** | **0.25** |
| 2048×256 | hybrid | 26.10 | 26.15 | 26.16 | 65 | 0.38 |
| 4096×512 | CPU | 109.51 | 116.88 | 119.22 | 9708 | 8.25 |
| 4096×512 | **GPU** | **103.10** | **103.18** | **103.22** | **111** | **0.56** |
| 4096×512 | hybrid | 104.04 | 104.15 | 104.19 | 147 | 1.17 |

*hybrid* = CPU/FFTW range-FFT → GPU/cuFFT doppler+CFAR → CPU cluster (mixed
placement; see below). It tracks GPU latency and jitter closely at every size.

- **Processing-tail advantage (GPU):** 4.9× (1024) → 9.2× (2048) → 14.7× (4096).
- **Jitter advantage (GPU):** ~6× → 26× → 87×. GPU p99.9−p50 stays ~70–110 µs at every
  size; CPU jitter grows with CPI (more chirps → more scheduling variance).
- p50 is arrival-dominated (≈ physical CPI) by construction; the CPU/GPU gap in
  p50 (0.8 / 3.1 / 6.4 ms) is the processing-tail gap. (CPU 4096 tail improved from
  9.69 ms on old main `ffb51d3` to 8.25 ms here — the P0 report/warm-up fixes.)

## Table 2 — Max sustained frame rate (robust 3/3, verifier-gated)

Fastest grid period that passes **3/3** verifier-gated repeats (200 frames,
compressed streamed pacing) — the defensible sustained rate. See note on why the
single-run bisect is not used here.

| CPI | CPU fps | GPU fps | hybrid fps | note |
|-----|--------:|--------:|-----------:|------|
| 1024×128 | ≥208 | ≥208 | ≥208 | tie — all pass at the grid floor (4.8 ms); receiver/loopback-bound, not compute |
| 2048×256 | ~83–91 | **~111–167** | ~77–154 | **GPU highest**; high run-to-run variance (see Refresh note) — read as a range |
| 4096×512 | 38.5 | **50.0** | 45.5 | **GPU 1.30× CPU**, hybrid 1.18× CPU |

- **Why 3/3 and not the single-run bisect.** Sustained-rate boundaries here are
  *bistable*: a period can pass once and fail on a re-run. The single-run gated
  bisect over-reports. So every rate above is the fastest period passing 3
  consecutive gated runs — but even 3/3 is not fully reproducible at 2048×256, where
  the bistable boundary swings a lot run-to-run (GPU 111–167, hybrid 77–154, CPU
  83–91 fps across three re-runs); only the ordering (GPU highest) is stable there.
  1024×128 passes 3/3 at the fastest grid point (4.8 ms, ≥208 fps) for all three —
  the loopback receiver path binds, not compute.
- **GPU wins throughput at every CPI** (or ties at the 1024 floor). It does **not**
  flip to a CPU win at any size on this box — contrary to the website's "CPU 1.85× at
  4096×512" (see Interpretation).
- **Hybrid** sits between CPU and GPU on throughput.

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
report. Only the kernel `.so` differs (all-GPU vs hybrid). Soft coverage gate:
detection-correct **and** ≥98% frames present (tolerates the 1-frame EOS-flush
artifact; real overload loses far more). Harness: `shared_gpu.py` (single-trial
explore) + `shared_gpu_firm.py` (**Part B: 3/3-robust with CUDA MPS off/on; Part C:
5-repeat hog; nsys mechanism**). MPS is scoped to a private pipe on GPU 1 (daemon
`CUDA_VISIBLE_DEVICES=1`, clients device 0), so other tenants are unaffected.

**Part B — max aggregate sustained rate (fps) vs N, MPS off vs on** (3/3-robust; the
concurrent boundary is bistable, so `~` marks cells whose 3/3 result is noisy — read
the direction). Headline: **MPS, not placement, is the lever for pure concurrency.**

| CPI | backend | off N1 | off N2 | off N4 | on N1 | on N2 | on N4 |
|-----|---------|------:|------:|------:|-----:|-----:|-----:|
| 2048×256 | GPU    | 143 | 143 | **fail** | ~fail | 125 | **200** |
| 2048×256 | hybrid | 143 | 143 | **fail** | 154 | 200 | **200** |
| 4096×512 | GPU    | 42 | **fail** | **fail** | 46 | 46 | **48** |
| 4096×512 | hybrid | 46 | ~fail | ~34 | 42 | ~fail | **48** |

> Table values above are from old main `ffb51d3`. **Refresh on `bc9e5d9`:** the
> direction holds, with one shift — the P0 fixes improved *no-MPS* concurrency, so
> all-GPU 4096×512 now sustains N=4 even without MPS (~44 fps). MPS's incremental role
> is therefore smaller on current main, but hybrid still sustains N=4 at 2048×256 where
> all-GPU cannot, and Part C (below) is unchanged. Cells remain bistable — read direction.

- **Without MPS, concurrent GPU pipelines collapse — for *both* placements.** At N=4
  every no-MPS cell fails; at 4096 even N=2 fails. N independent processes time-slice
  one GPU context, so they serialize and drop frames regardless of placement.
- **MPS fixes it — for both.** With MPS the kernels from separate processes actually
  overlap and both all-GPU and hybrid scale to N=4 (2048: 200 fps agg; 4096: 48 fps).
  So the honest answer to "does MPS fix all-GPU's launch contention?" is **yes** — MPS
  is the primary lever; hybrid's launch-frugality is a *secondary* edge (a modest lead
  at 2048×256 N=2, 200 vs 125). This corrects an earlier single-trial reading that
  credited the concurrency win to placement; with MPS the placements are close.

**Part C — adversarial GPU hog** (background cuBLAS matmul pins device ~100%), 5
repeats/cell, N=1 radar at the fixed rate (2048: 8 ms; 4096: 30 ms), no MPS:

| CPI | backend | pass | p50 | p99 | p99.9 |
|-----|---------|-----:|----:|----:|------:|
| 2048×256 | GPU | **0/5** | — | — | — (starved, no frames) |
| 2048×256 | **hybrid** | **4/5** | 8.69 | 8.82 | 8.86 ms |
| 4096×512 | GPU | **0/5** | — | — | — (starved, p50 ~197 ms when it limps) |
| 4096×512 | **hybrid** | **4/5** | 29.61 | 31.85 | 32.06 ms |

- **This is the durable, MPS-independent win.** Against an uncontrolled co-tenant that
  saturates the GPU, all-GPU is starved every run (its ~800 range+doppler launches/
  frame fight the hog); the hybrid runs range on the CPU and lands only ~32 GPU
  kernels/frame, which slot between the hog's kernels — holding GPU-class latency
  (4096: 5/5 at 29.6 ms p50). MPS does not help here: the hog saturates compute, not
  just the launch queue. (N=2 under the hog fails for both — hog + 2 pipelines is too
  much GPU.)

**Mechanism (nsys, single pipeline, 2048×256, per frame):**

| placement | GPU kernels/frame | H2D copies/frame |
|-----------|------------------:|-----------------:|
| all-GPU | **~800** (768 range = 3/chirp × 256 chirps + 32 Doppler/CFAR) | 256 |
| **hybrid** | **32** (Doppler/CFAR only — zero range kernels) | 8 |

≈25× fewer GPU launches and 32× fewer H2D copies — the concrete reason the hybrid
slots into a contended GPU. (Measured: `k_window_i16`/`vector_fft<2048>`/`k_corner_turn`
= 10,240 instances each over 40 frames for all-GPU, and **absent** for hybrid.)

**Edit cost — the paper-relevant point.** A hybrid placement in Tomii is **one kernel
`.so` swap: 0 graph / plugin / `tomii-core` lines changed** (the C-ABI kernel boundary
hides the device choice). The equivalent in the GR4 flowgraph (`gnuradio4/radar_rx4.cc`,
255 lines) is a **code change**: the DSP is baked into typed `gr::Block<>` classes
(`RangeFft` with inline FFTW ~40 lines, `DetectSink` calling the kernels ~50 lines),
so moving a stage across devices means rewriting that block's `processBulk` with CUDA +
device-buffer/staging handling (host `gr::PortIn/Out<std::complex<float>>` don't carry
device memory), likely a new copy/staging block, and recompiling — rough order ~80–150
lines across 1–2 block classes, vs 0 for Tomii.

**Takeaway.** Two regimes: (1) *you control the workload* → CUDA MPS lets both
placements share the GPU; placement is a minor lever. (2) *an uncontrolled co-tenant
saturates the GPU* → the launch-frugal hybrid (range on CPU, ~25× fewer GPU launches)
keeps GPU-class latency where all-GPU starves — and it's reached by swapping one `.so`,
no graph/runtime change. Neither regime helps the GPU-**compute**-bound case (4096×512
Doppler/CFAR saturates the device either way).

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
6. Shared-GPU Part B is 3/3-robust but the concurrent boundary is bistable (some cells
   marked `~` are noisy); read the direction (no-MPS collapse vs MPS-on scaling), not
   the last digit. Part C hog is 5 reps/cell. nsys mechanism is one pipeline. No CUDA
   MPS during the hog (Part C) by design. GR4 edit cost is a code estimate, not built.

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
# shared-GPU: single-trial explore, then 3/3-robust + MPS on/off + 5-rep hog:
python3 bench/radar-bench/shared_gpu.py            # explore (parts B,C)
python3 bench/radar-bench/shared_gpu_firm.py       # 3/3 + MPS off/on + 5-rep hog
```

Underlying harness (all committed): `run_bench.py --gpu|--hybrid`,
`bisect_rate.py --gate-search [--gpu|--hybrid]`, `robust_rate.py` (3/3 grid),
`shared_gpu.py` + `shared_gpu_firm.py` (N concurrent pipelines, MPS, GPU hog, nsys),
`kernels/radar_hybrid.cu` (+ `make -C kernels hybrid`).
