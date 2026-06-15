# anti-diag-bench

Anti-diagonal wavefront benchmark used as the **regression gate** for Tomii runtime changes.

## Workload

An N×N grid wavefront where each cell `(i,j)` on anti-diagonal `d` depends on `(i-1,j)` and
`(i,j-1)`. Each cell computes `grid[i][j] = 0.5*(grid[i-1][j] + grid[i][j-1])`.

Two dispatch variants are benchmarked:

| Variant | Function | Description |
|---|---|---|
| `wf_cell` | per-cell dispatch | One task per cell; N tasks per diagonal; `$barrier` between diagonals |
| `wf_cell_bulk` | bulk dispatch | One task per diagonal covers the full range `(0..diag_width)`; Tier 4 bulk path |

The benchmark measures **ms per diagonal sweep** (total wall time / iterations) at N=512.

## Runtime Configuration

All Tomii runs use (hardcoded in `tomii/run_bench.py`):

| Flag | Effect |
|---|---|
| `--custom` | Lock-free priority scheduler (replaces Rayon) |
| `--coalesce-barriers` | Batch-dispatch ready barrier successors |
| `--inline-continuation` | Inline single non-condition successor resolution on the worker thread |
| `--use-rdtsc` | RDTSC per-node timing |
| `--batching-size 1` | Immediate batch processing |
| `--slots 1` | Single concurrent stream (wavefront is sequential per stream) |

Taskflow uses its default `tf::Executor`. The `--custom` flag is important: the Rayon code
path is not compatible with the barrier argument-injection pattern this workload uses.

## Reproduce

```bash
# Build and run (default: N=512, W=1/2/4/8, 100 iters, both variants)
cd bench/anti-diag-bench/tomii
python run_bench.py --n 512 --workers 1 2 4 8 --iterations 100 --func all

# Taskflow comparator
cd bench/anti-diag-bench/taskflow
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j
python run_bench.py

# Side-by-side comparison plot
cd bench/anti-diag-bench
python tomii-vs-taskflow.py
```

Results are written to `tomii/results/`.

## Role in CI / Regression Gate

Before merging changes to the resolution loop or batch-processing path, capture a
baseline run, apply your change, re-run, and compare. Max acceptable regression: **+2%**
across any (N, W, variant) cell; improvements are expected for resolution-path
optimisations. Run outputs are generated locally and are not committed.
