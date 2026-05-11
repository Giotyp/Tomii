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
