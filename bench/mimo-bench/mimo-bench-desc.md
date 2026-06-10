# MIMO-bench Methodology

## Workload

Real LTE/5G uplink slot processing. One "stream" = one OFDM slot.

```
$network (bs_ant_num UDP sockets, Agora wire format)
   ├──► fft    (factor = total_ul_symbols  = ul_symbols × bs_ant_num)
   │               └──┐
   ├──► csi    (factor = total_pilot_symbols = pilot_symbols × bs_ant_num)
   │               ↓
   │           beam  (factor = beam_events_per_symbol = ofdm_data_num / beam_block_size,
   │                  $barrier on csi[0..total_pilot_symbols])
   │               └──┐
   └───────────────────┴──► demul  (factor = total_demul_tasks,
                                    $barrier on fft[0..total_ul_symbols] group_by bs_ant_num,
                                    $barrier on beam[0..beam_events_per_symbol])
```

**Default configuration (4×4):**

| Parameter | Value |
|---|---|
| BS antennas | 4 |
| UE streams | 4 |
| FFT size | 256 |
| OFDM data subcarriers | 192 |
| Modulation | 16-QAM |
| Symbols per slot | 14 (1P + 13UL, frame schedule `PUUUUUUUUUUUUU`) |
| Pilot symbols | 1 |
| UL data symbols | 13 |
| Total streams | 2000 (+ 200 warmup) |
| S (concurrent slots) sweep | {1, 4, 16, 64} |
| W (worker threads) sweep | {1, 2, 4, 8} |

Decode (LDPC) is **intentionally excluded**: FlexRAN SDK is non-redistributable
and the decode stage contributes ≈4 % of total slot compute at 64×8 configs.

## Kernels

| Stage | Kernel | Library |
|---|---|---|
| FFT (UL data) | `SimdConvertShortToFloat` → `DftiComputeForward` → fftshift → `PartialTranspose` | MKL DFTI + `libfftfuncs.so` |
| CSI (pilots) | Same FFT path → `expand_csi` | MKL DFTI + `libfftfuncs.so` |
| Beamweights | `PartialTransposeGather` → `cblas_cherk` → `LAPACKE_cpotrf/cpotrs` → `Precoder` | MKL BLAS/LAPACK + `libbeamfuncs.so` |
| Demul | `DemulGather` → `Equalization` → `Demod_wrap` | `libdemod.so` + `libbeamfuncs.so` |

Both Tomii and Taskflow link against **identical binaries**: same MKL installation,
same vendored `.so` files from `tomii/lib/`. Kernel parity is structural — the
same C function pointer is called with the same buffer layout on both sides.

## Metric

```
ms_per_slot = total_wall_seconds × 1000 / total_streams
```

- Tomii: measured by `time.monotonic()` around the blocking `binary` call in
  `run_bench.py`; warmup streams are inside the same run but excluded from the
  denominator via `--exclude-streams`.
- Taskflow: `total_wall_ms / measured_streams` reported by `tf_mimo` directly.

## Taskflow Comparator

`taskflow/src/main.cpp` implements the identical 4-node topology as a
`tf::Taskflow` DAG. Packet ingestion uses `recvmmsg` on the same UDP port range
as Tomii (controlled by `bs_server_addr` / `bs_server_port` in the tddconfig).
The Taskflow DAG uses gate tasks (`fft_sync`, `beam_sync`) to express the
barriers: all FFT tasks precede `fft_sync`, which precedes all demul tasks;
all beam tasks precede `beam_sync`, which also precedes all demul tasks.

## Tomii Runtime Configuration

All Tomii runs use the following flags (hardcoded in `tomii/run_bench.py`):

| Flag | Effect |
|---|---|
| `--custom` | Lock-free priority scheduler (replaces Rayon). Required: the Rayon path does not inject the instance-index argument that the network-driven dispatch expects. |
| `--coalesce-barriers` | Dispatches all ready barrier successors in one batch; reduces resolution-thread overhead at high task counts per slot. |
| `--inline-continuation` | Worker thread resolves single non-condition successors inline rather than re-queuing. Eliminates one scheduler round-trip per 1:1 edge on the critical path. |
| `--slot-priority` | Assigns higher scheduling priority to tasks belonging to older (earlier-started) slots; reduces head-of-line blocking under concurrent slot dispatch. |
| `--use-rdtsc` | RDTSC-based per-node timing (for the `--timing` report; does not affect execution). |

Taskflow uses its default `tf::Executor` with no special flags. Tomii's published numbers reflect its **recommended configuration** for packet-driven streaming workloads, not a default out-of-the-box run.

## Methodology Rules

1. **Identical kernel binaries.** Both sides call the same C-ABI symbols from
   the same `.so` files. Timing differences cannot come from kernel logic.
2. **Identical packet input.** Both sides receive packets from the same Agora
   sender process (`~/Agora/run_sender.sh`) in the same run.
3. **Tomii scheduler.** `--custom` (lock-free priority scheduler) is used for all Tomii runs. The default Rayon scheduler is not compatible with this workload's network-driven dispatch pattern and is not tested here.
4. **Correctness gate.** Run `python mimo-bench/tomii/verify.py` before
   recording any perf number. The verifier checks that two consecutive
   single-frame runs produce byte-for-bit-identical post-demul output.

## Honest Caveats

1. **4×4 is small.** At 4 antennas and 192 subcarriers, per-slot compute is
   on the order of a few hundred µs. Tomii's fixed per-slot overhead (~5 ms
   at S=1) will dominate at low concurrency. The interesting comparison is
   at S=16–64 where scheduling overhead is amortised.
2. **Agora sender required.** Both pipelines consume live UDP packets; there
   is no in-process packet generator. Run `~/Agora/run_sender.sh` in a
   separate terminal before either benchmark.
3. **Decode dropped.** Post-demul soft-bit buffers are not further processed.
   This affects absolute per-slot timing but not the relative Tomii vs
   Taskflow scheduling comparison.
4. **Intel/Agora deps.** MKL 2024.0 and the vendored `.so` files are required.
   The benchmark is not self-contained.

---

## 16×16 Configuration (compute-limited regime)

**Config**: `tddconfig-16x16.json` — 16 BS antennas, 16 UEs, FFT=2048, 1200 OFDM
subcarriers, 16 pilot + 13 UL symbols per slot (`PPPPPPPPPPPPPPPPUUUUUUUUUUUUU`).

At 4×4 the workload is **sender-rate limited** (both systems wait mostly for packets).
At 16×16 with `MKL_NUM_THREADS=1` the workload is **compute-limited**: per-slot beam
computation (400 tasks × 16×16 Cholesky per task) dominates scheduling overhead. This
tests whether Tomii's streaming-overlap advantage persists when compute, not dispatch,
is the bottleneck.

### Configuration parameters

| Parameter | 4×4 | 16×16 |
|---|---|---|
| BS antennas | 4 | 16 |
| FFT size | 256 | 2048 |
| OFDM data subcarriers | 192 | 1200 |
| Pilot symbols | 1 | 16 |
| UL data symbols | 13 | 13 |
| Total packets/frame | 56 | 464 |
| Beam tasks/slot | 24 | 400 |
| FFT tasks/slot | 52 | 208 |

### Why `frame_duration` differs from the 4×4 bench

At 4×4 the sender uses `--frame_duration=1000` (1 ms, matching the real LTE slot
period) and processing happens in ~1 ms — the bench happens to be near real-time.
At 16×16 with `MKL_NUM_THREADS=1` and W=24, processing takes ~48 ms/slot. Using 1 ms
frame_duration would have the sender outpace the pipeline by 48×, dropping virtually
all frames. The bench instead sets `frame_duration` so the sender rate roughly matches
the processing rate, keeping all S slots occupied without overflow:

| S | frame_duration | Reasoning |
|---|---|---|
| 4 | 50 ms | ≈ per-slot compute / S = 48/4 × safety margin |
| 16 | 15 ms | ≈ per-slot compute / S = 48/16 × safety margin |

The relative Tomii vs Taskflow comparison is fair: both sides use identical
`frame_duration`, see the same packet arrival rate, and are measured with the same
first-packet-to-last-task-completion metric.

### Results (2026-05-13, W=24, 200 streams, 5 warmup)

**Tomii (first-pkt→done):**

| W\S | S=4 | S=16 |
|-----|-----|------|
| W=24 | 47.98 ms | 14.41 ms |

**Taskflow (first-pkt→done):**

| W\S | S=4 | S=16 |
|-----|-----|------|
| W=24 | 55.18 ms | 20.58 ms |

**Speedup (Taskflow / Tomii):**

| W\S | S=4 | S=16 |
|-----|-----|------|
| W=24 | **1.15×** | **1.41×** |

Reproduce: `tomii/run_bench.py --slots 4 16 --workers 24 --config graphs/tddconfig-16x16.json`
and the Taskflow comparator's `run_bench.py`. Result CSVs are regenerated outputs
(git-ignored under `results/` and `build/`); the values above are the committed record.

### Interpretation

**The streaming-overlap advantage persists at compute-limited scale.**

At S=4 the advantage is smaller (1.15×) than the 4×4 result (1.26×): with heavy
per-slot compute, the absolute packet-receive span (~22 ms for 464 packets at
50 ms/frame) is a smaller fraction of total latency. Tomii can still pipeline FFT
and CSI tasks during the receive phase, but the compute-dominated beam and demul
stages limit the gain.

At S=16 the advantage is *larger* than 4×4 (1.41×): multi-slot amortisation
interleaves beam tasks from 16 concurrent slots across the 24 workers, eliminating
the serialisation that a single slot sees. Taskflow must reconstruct a fresh `tf::Taskflow`
per frame and cannot share workers across pending frames during packet reception.

**Why the source of advantage is different at 16×16 vs 4×4:**

| Effect | 4×4 | 16×16, S=4 | 16×16, S=16 |
|--------|-----|------------|-------------|
| Streaming overlap (per-packet dispatch) | dominant | present, smaller fraction | present |
| Multi-slot worker amortisation | moderate | moderate | dominant |
| Absolute ms saved vs Taskflow | 0.24 ms | 7.2 ms | 6.2 ms |

### Honest caveats

1. **W=24 only.** At W=4 per-slot latency is ~200 ms (MKL_NUM_THREADS=1, 400 beam
   Cholesky tasks per slot). W=24 is the practical minimum for timing within the
   sender's frame window. Agora uses `worker_thread_num=26` for the same reason.
2. **Agora gap remains large.** Agora's published 1.62 ms at 16×16 reflects a
   highly optimised C++ implementation with MKL multi-threading per task; our bench
   uses `MKL_NUM_THREADS=1` to isolate scheduling overhead. The Tomii-vs-Agora
   comparison is not meaningful here — it is documented in the paper.
3. **Dropped frames at S=16.** 3–5 frames dropped (no available slots) out of
   200 measured. The latency average is over frames that completed successfully.
4. **S=1 Taskflow stall.** Same as 4×4: Taskflow S=1 discards frame N+1 packets
   while frame N is in-flight. Tomii S=1 is not measured here (not a streaming
   scenario at 48 ms/slot).
5. **Verifier.** Two-pass byte-for-bit equality check passes at W=24, S=4 on the
   16×16 graph. Run `tomii/verify.py --config graphs/tddconfig-16x16.json
   --sender-config files/config/ci/tddconfig-16x16.json` (the graph, including the
   determinism dump node, is generated from the config via `build_graph.py`).
