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

## What the sweep shows

Tomii does **not** win this benchmark on absolute throughput. The point of the S-sweep
is to show how the per-stream gap *closes* as multi-slot amortisation distributes the
resolution-thread cost across concurrent lanes, and that per-slot RSS growth is lower
than Taskflow's (relevant to long-running services with many concurrent streams, not S=1
jobs). Measured per-slot memory growth comes from `scripts/memory_measure.sh` (peak RSS,
S=1 vs S=64). Comparison numbers are published in the project documentation.

## Reproducibility note

Result CSVs are regenerated outputs and are **not committed** (git-ignored under
`results/` and `build/`). A committed regression baseline ships under
`tomii/results/post_r1/`. Re-run the scripts above to regenerate the live sweep.

See `pipeline-bench-desc.md` for full methodology, the metric-mismatch history, and
honest caveats.
