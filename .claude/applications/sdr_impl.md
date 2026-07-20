# SDR / FMCW Radar Pipeline — Implementation Status (`apps/SDR` branch)

Companion to [SDR.md](SDR.md) (the original proposition). This documents what
was actually built, how the baselines were constructed, and what the results
show so far.

## What it does, in simple terms

We built a software radar receiver. A sender streams simulated radar antenna
data (chirps) over UDP, one packet per chirp, 128 chirps per "frame"
(a coherent processing interval, CPI). The receiver turns each frame into a
list of detected targets — "something at 80 m moving 5 m/s toward us" — by:

1. **Range FFT** — one FFT per chirp: converts echo delay into distance.
2. **Doppler FFT** — one FFT across the 128 chirps at each distance:
   converts phase drift into velocity.
3. **CFAR detection** — an adaptive threshold that compares every
   (distance, velocity) cell against its local noise neighborhood.
4. **Clustering** — merges adjacent detections into one target report.

Because the simulated scene places targets at *known* positions/velocities,
a verifier can check every frame's detections against ground truth. Every
performance number on this branch is gated on that verifier passing
(all targets found, in every frame, full frame coverage) — a perf number
from a broken pipeline is impossible by construction.

The pipeline runs identically on four orchestrators (Tomii, GNU Radio 3.10
Python-blocks, GNU Radio 3.10 C++-blocks, GNU Radio 4.0) and, within Tomii,
with the math on either CPU (FFTW) or GPU (CUDA/cuFFT) — selected by swapping
a shared library, with zero changes to the graph or plugin.

## What was implemented

**`examples/radar-pipeline/`** — the Tomii application:
- `data/make_scene.py` — seeded synthetic FMCW scene generator (targets with
  known range/velocity/SNR + noise) with per-frame ground-truth bins.
- `data/reference_check.py` — NumPy golden reference (windowed 2D FFT +
  CA-CFAR + clustering); the C kernels match it exactly (bin-identical
  detections, ~1e-4 float error on power maps).
- `sender.py` — stdlib UDP sender; paces chirps at the scene's physical
  chirp interval by default (a full-frame burst overruns kernel socket
  buffers and was the source of one GPU-box wedge).
- `kernels/radar_cpu.c` — FFTW implementation of a small C ABI (`radar.h`):
  range/Doppler FFT, CFAR, clustering, plus buffer/workspace management.
- `kernels/radar_gpu.cu` — CUDA/cuFFT twin exporting the *same symbols*:
  device-resident buffers, per-slot streams/plans, pinned-host staging,
  batched Doppler FFTs, CFAR kernel with atomic detection append.
- `src/lib.rs` — Tomii plugin: packet parsing, network callbacks, and thin
  `_cm` bridges into the kernel ABI. Includes `TOMII_RADAR_CHECK=1`
  env-gated stage-order instrumentation (counts per-frame stage completions,
  reports duplicates / early starts) — this is what isolated the core bug.
- `build_graph.py` — declarative 4-node graph
  (`$network → range_fft×128 → doppler_fft×8 → cfar×8 → cluster`).
- `run_bench.py` / `verify.py` — build + run + verifier-gated latency
  percentiles (p50/p99/p99.9) from the runtime report; `--gpu` selects the
  CUDA kernels via `RADAR_KERNELS_DIR`; sweeps write a summary CSV.

**`bench/radar-bench/`** — the baselines and comparison driver:
- `gnuradio/radar_rx.py` — GNU Radio 3.10 flowgraph, custom blocks in Python.
- `gnuradio/radar_rx.cc` — same flowgraph, custom blocks in native C++.
- `gnuradio4/radar_rx4.cc` — GNU Radio 4.0 (C++23) with native `gr::Block<>`
  types on the `multiThreaded` scheduler; GR4 pinned to commit `92278b6`.
- `compare.py` — runs all four systems on the same stream, verifier-gates
  each run, reports median-of-N percentiles.

**Runtime (tomii-core) changes made along the way:**
- **Dispatch-race fix** (`b4cd2cc`): full-range 1:1 `$res` edges (the
  `$network → per-packet-node` shape) fell to the ordinal threshold scan;
  with ≥2 resolution threads, racing instance-claim CASes could pair a task
  instance with the wrong predecessor's packet (one consumed twice, one
  never — stale data corruption in ~6% of frames). Contiguous ranges now
  qualify for exact 1:1 dispatch. Found by the radar verifier; mimo-bench's
  network edge has the same shape and was exposed to the same bug.
- **Report extension** (`e987d67`): `p999_latency_us` + the sorted per-frame
  latency array in the JSON report (single-run jitter CDFs).
- Known remaining gap (framework-level, not yet fixed): no incomplete-frame
  eviction — a frame that loses even one packet wedges its slot forever.

**Kernel-level correctness fixes** (found on the GPU box, mirrored to CPU):
- cuFFT plans are not thread-safe across in-flight frames → per-slot
  sub-workspaces (plan/stream/scratch per slot). The CPU twin had the same
  sharing flaw — FFTW doesn't hang, it silently corrupts — and got the same
  per-slot fix.
- Verifier hardening: coverage gate (`--expect-frames`) so overload runs
  that drop whole frames fail instead of passing on the surviving frames.

## Is the GNU Radio comparison standard practice?

Mostly yes, with one deliberate deviation, and we bracket both sides:

- **Standard:** stock GNU Radio schedulers (3.10 thread-per-block; 4.0
  `multiThreaded`), the stock native FFT block for the range FFT in 3.10,
  custom source/sink blocks written exactly as GR documentation prescribes
  (`gr.sync_block` / `gr::Block<>`), default buffering. The Python-block
  variant is what most GR users deploy; the C++-block variant is what a
  latency-focused practitioner would write. Both are measured — and they
  agree within 2% at p50, which shows the gap vs Tomii is the scheduler,
  not the block language.
- **Deviation (deliberate):** the Doppler/CFAR/cluster stages call the same
  `libradar_kernels.so` used by Tomii instead of chaining stock GR blocks.
  This makes the DSP byte-identical across all systems, so the measurement
  isolates *framework orchestration* — which is the claim under test. A
  pure-stock-block GR chain would differ numerically and add unrelated work
  to the comparison.
- Same UDP stream, same packet format, same pacing, same verifier and
  tolerances, warmup excluded, median-of-N runs, end-of-stream flush
  artifact excluded (documented in `compare.py`).

## Results so far

**Framework comparison** (this box, 1024×128 CPI, paced sender, 100 Hz,
500 frames/run, median-of-3, all runs verifier-gated). All systems share a
~6.35 ms chirp-arrival floor (physics: 127 chirps × 50 µs), so the
"processing tail after the last chirp lands" is the informative column:

| system | p50 | p99 | p99.9 | ≈ tail after last chirp |
|---|---|---|---|---|
| Tomii (CPU) | 6.92 ms | 6.97 ms | 7.07 ms | **~0.6 ms** |
| GNU Radio 4.0 | 10.1 ms | 10.7 ms | 14.0 ms | ~3.8 ms |
| GNU Radio 3.10 C++ | 13.8 ms | 14.5 ms | 14.7 ms | ~7.5 ms |
| GNU Radio 3.10 py | 14.1 ms | 14.5 ms | 17.7 ms | ~7.7 ms |

Tomii's p50→p99.9 spread is ~150 µs; GR 4.0's is ~3.9 ms. GNU Radio 4.0's
rewrite genuinely improves on 3.10 (≈3.5× at the tail) — and Tomii still
leads it ≈6× on processing tail with far tighter jitter.

**CPU↔GPU crossover** (RTX 4090 box, same graph/verifier, kernel `.so` swap
only):

| CPI | Tomii CPU | Tomii GPU | verdict |
|---|---|---|---|
| 1024×128 | ~2.8 ms | ~2.0 ms | wash — GPU overhead-bound at tiny FFTs |
| 4096×512 (16×) | 19.4 ms, falls behind real-time | 10.4 ms, keeps up | GPU 1.9×; only GPU sustains the rate |

**What this showcases:**
1. *Predictable low latency*: declarative DAG + pinned runtime beats both
   generations of flowgraph schedulers on the same DSP, with microsecond-
   scale jitter instead of millisecond-scale.
2. *Modularity*: CPU→GPU is a deployment decision (`RADAR_KERNELS_DIR`),
   not a rewrite, and pays off exactly where the compute grows — with the
   verifier proving numerics are unchanged across the swap.
3. *The verifier-gated methodology has teeth*: it caught a real runtime
   race, a real kernel thread-safety bug (both fixed), and an overload
   coverage hole — silent wrongness could not have shipped as a number.

**Caveats:** cross-machine numbers are not comparable (different CPUs; the
GPU box is noisy); the large-CPI percentiles rest on few steady-state frames
(12 CPU / 44 GPU) and deserve a longer run; GR baselines have not been run
on the GPU box; incomplete-frame eviction remains open (any real packet loss
wedges a slot — the honest next fix, in tomii-core).
