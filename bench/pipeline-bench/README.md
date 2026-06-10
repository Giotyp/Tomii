# pipeline-bench: Multi-stream Pipeline S-scaling (Tomii vs Taskflow)

A self-contained streaming benchmark (no external/MKL dependencies) that measures how
Tomii's per-stream scheduling overhead amortises as the number of concurrent slots (S)
grows. Both frameworks run the **same 4-stage fan-out/fan-in DAG** with an identical
heavy compute kernel, so timing differences reflect scheduling, not kernel work.

## Graph topology (4 stages)

```
ingest[0..N]      (factor=N; produces f64 = (idx+1)/N)
     ↓ 1:1
transform[0..N]   (factor=N; heavy_transform — 8192 sin accumulations, ~64 µs/call)
     ↓ variadic fan-in
aggregate         (arithmetic mean of all N transform outputs)
     ↓
emit              (returns the mean)
```

| Parameter | Value |
|---|---|
| N (items per stream) | 256 |
| TRANSFORM_ITERS | 8192 (~64 µs/task) |
| Total streams | 2000 (+200 warmup) |
| S (concurrent slots) sweep | {1, 4, 16, 64} |
| W (worker threads) sweep | {1, 2, 4, 8} |

The ~64 µs/task kernel keeps scheduling overhead below ~15 % of wall time, so the
S-scaling comparison measures dispatch efficiency rather than resolution overhead
(see `audit-2026-05-07.md` for the sub-µs kernel analysis that motivated this).

## Quick start

```bash
cd bench/pipeline-bench

# Correctness gate FIRST (no perf number is valid without it):
python tomii/verify.py --transform-iters 8192

# Tomii side:
python tomii/run_bench.py --workers 4 --slots 1 4 16 64 --streams 2000

# Taskflow side:
python taskflow/run_bench.py --workers 4 --slots 1 4 16 64 --streams 2000
```

## Metric

```
ms_per_stream = total_wall_seconds * 1000 / total_streams
```

Both sides report wall-clock **throughput** (warmup streams excluded from the
denominator). Tomii is timed via `time.monotonic()` around `graph.run()`; Taskflow runs
S graph clones across one shared `tf::Executor` and divides total wall time by streams.

## Tomii runtime configuration

All Tomii runs use these flags (hardcoded in `tomii/run_bench.py`): `--custom`
(lock-free priority scheduler), `--coalesce-barriers`, `--inline-continuation`,
`--use-rdtsc`, `--batching-size 1`. Taskflow uses the default `tf::Executor`. Tomii's
published numbers therefore reflect its **recommended** streaming configuration.

## Headline results (W=4)

| S | Tomii ms/stream | Taskflow ms/stream | Ratio |
|---|---|---|---|
| 1 | 3.02 | 1.22 | 2.48× slower |
| 16 | 1.55 | 1.18 | **1.33× slower** |
| 64 | 1.54 | 1.20 | 1.28× slower |

**Tomii does not win this benchmark.** The claim is that the gap *closes* — from ~2.5×
at S=1 to 1.28–1.36× at S≥16 — as multi-slot amortisation distributes the
resolution-thread cost across concurrent lanes.

### Per-slot memory growth

Measured by `scripts/memory_measure.sh` (peak RSS of the binary, S=1 vs S=64, 3 runs;
see `memory_results.txt`):

| | Tomii | Taskflow |
|---|---|---|
| Per-slot RSS growth | **+81 kB/slot** | +132 kB/slot (**1.6× steeper**) |

Tomii's base RSS is *higher* (≈8.4 MB vs 4.7 MB at S=1, from preallocated worker stacks +
resolution machinery); the advantage is in the **growth rate**, relevant to long-running
services with many concurrent streams — not S=1 jobs.

## Reproducibility note

Result CSVs are regenerated outputs and are **not committed** (git-ignored under
`results/` and `build/`). A committed regression baseline ships under
`tomii/results/post_r1/`. Re-run the scripts above to regenerate the live sweep.

See `pipeline-bench-desc.md` for full methodology, the metric-mismatch history, and
honest caveats.
