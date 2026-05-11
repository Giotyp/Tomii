# mimo-bench: Real-PHY MIMO Benchmark (Tomii vs Taskflow)

This benchmark runs a real LTE/5G uplink PHY pipeline — OFDM FFT → channel
estimation → ZF beamforming → symbol demap — through both Tomii and Taskflow
using **the same precompiled Intel libraries** for each stage, so timing
differences reflect scheduling overhead rather than kernel differences.

## External dependencies

> **Warning:** This benchmark has hard external dependencies that the rest of
> Tomii's examples do not require.

| Dependency | Notes |
|---|---|
| **Intel oneAPI MKL 2024.0** | FFT (DFTI) and BLAS/LAPACK. Default path: `/opt/intel/oneapi/mkl/2024.0`. |
| `libbeamfuncs.so`, `libdemod.so`, `libfftfuncs.so` | Precompiled; vendored under `tomii/lib/` (originally from the Agora project). |
| **Agora packet sender** | Must be running alongside the benchmark. Build Agora separately: <https://github.com/Agora-wireless/Agora>, then run `~/Agora/run_sender.sh`. |

If you want a fully self-contained Tomii benchmark with no external dependencies,
see `pipeline-bench/` and `examples/matrix-compute/`.

## Graph topology (4 nodes, 3 barriers, decode dropped)

```
$network (bs_ant_num UDP sockets)
   ├──► fft    (factor=total_ul_symbols, 1:1 from UL data packets)
   │              └──┐
   ├──► csi    (factor=total_pilot_symbols, 1:1 from pilot packets)
   │              ↓
   │          beam  (factor=beam_events_per_symbol,
   │                 $barrier on csi[0..total_pilot_symbols])
   │              └──┐
   └────────────────┴──► demul  (factor=total_demul_tasks,
                                  $barrier on fft[0..total_ul_symbols] group_by antennas,
                                  $barrier on beam[0..beam_events_per_symbol])
```

Decode (LDPC + scrambler) is intentionally dropped: it requires FlexRAN SDK
(AVX-512/ICC, non-redistributable) and contributes only ~4 % of slot compute.
All of Tomii's distinguishing scheduling features remain: 3 barriers, 2 grouped
barriers, 4 distinct factor expressions.

## Quick start

```bash
# 1. Build and start the Agora sender (separate terminal):
cd ~/Agora && bash run_sender.sh

# 2. Run Tomii side:
source mimo-bench/tomii/scripts/export.sh
python mimo-bench/tomii/run_bench.py

# 3. Run Taskflow side:
python mimo-bench/taskflow/run_bench.py

# 4. Compare:
python mimo-bench/mimo-comparison.py
```

## Tomii Runtime Configuration

All Tomii runs use the following flags (hardcoded in `tomii/run_bench.py`):

| Flag | Effect |
|---|---|
| `--custom` | Lock-free priority scheduler (replaces Rayon). Required: the Rayon path does not inject the instance-index argument that the network-driven dispatch expects. |
| `--coalesce-barriers` | Dispatches all ready barrier successors in one batch; reduces resolution-thread overhead at high task counts. |
| `--inline-continuation` | Worker thread resolves single non-condition successors inline rather than re-queuing. Eliminates one scheduler round-trip per 1:1 edge on the critical path. |
| `--slot-priority` | Assigns higher scheduling priority to tasks belonging to older (earlier-started) slots; reduces HOL blocking under concurrent slot dispatch. |
| `--use-rdtsc` | RDTSC-based per-node timing (for the `--timing` report; does not affect execution). |

Taskflow uses its default `tf::Executor` with no special flags. Tomii's published numbers therefore reflect its **recommended configuration** for packet-driven streaming workloads.

## Tuning knobs

| Flag | Default | Applies to | Effect |
|---|---|---|---|
| `--slots` | `1 4 16 64` | both | Concurrent slot counts to sweep |
| `--workers` | `1 2 4 8` | both | Worker thread counts to sweep |
| `--streams` | 2000 | both | Measured streams per cell |
| `--warmup` | 200 | both | Warmup streams excluded from timing |
| `--receiver-threads` | 4 | Tomii | Dedicated UDP receiver threads |
| `--system-threads` | 2 | Tomii | Resolution threads |
| `--no-clean` | — | both | Skip rebuild |

## Output

Both sides write `ms_per_slot` (wall-clock ms ÷ streams) to a CSV:

| Side | CSV |
|---|---|
| Tomii | `tomii/results/mimo_sweep.csv` |
| Taskflow | `taskflow/build/tf_mimo_sweep.csv` |

`mimo-comparison.py` reads both CSVs and emits `mimo-comparison.png`.

See `mimo-bench-desc.md` for full methodology, metric definition, and caveats.
