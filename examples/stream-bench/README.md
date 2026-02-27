# stream-bench

SynStream implementation of the classic [STREAM](https://www.cs.virginia.edu/stream/) memory-bandwidth benchmark.

## What this tests

STREAM measures **sustained memory bandwidth** by running four simple array kernels that are bottlenecked entirely by DRAM throughput, not arithmetic:

| Kernel | Operation | Arrays read/written |
|--------|-----------|---------------------|
| Copy   | `a[i] = b[i]` | 2 |
| Scale  | `a[i] = scalar * b[i]` | 2 |
| Add    | `a[i] = b[i] + c[i]` | 3 |
| Triad  | `a[i] = b[i] + scalar * c[i]` | 3 |

Each worker operates on its own independent `Vec<f64>` array (default 256 M elements = 2 GB).
The array intentionally exceeds the last-level cache so every access hits DRAM.

The benchmark answers: **how much of the machine's peak memory bandwidth can SynStream's
scheduler deliver as worker count scales?**

## Graph structure

Each kernel maps to a linear SynStream task graph:

```
gen_b ──┐
        ├──► kernel_op (× N workers) ──► sink
gen_c ──┘
```

- `gen_b` / `gen_c` — `factor=N` parallel array generation nodes
- `kernel_op` — `factor=N` parallel kernel nodes, one per worker
- `sink` — single barrier node that waits for all kernel instances before recording timing

The `sink` node uses a `$barrier` predecessor referencing `"0-num_workers"` to enforce a
full barrier across all worker outputs before the stream is marked complete.

## Running

```bash
# From repo root — activate venv first
source .venv/bin/activate
source examples/mimolib/scripts/export.sh

# Full sweep: all 4 kernels × workers {1, 2, 4, 8}
python examples/stream-bench/run_bench.py

# Single quick smoke-test
python examples/stream-bench/run_bench.py \
    --workers 1 \
    --kernels copy \
    --array-size 1048576 \
    --max-streams 5 \
    --exclude-streams 1 \
    --no-clean
```

Key options:

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | `1 2 4 8` | Worker thread counts to sweep |
| `--kernels` | all four | Subset of `copy scale add triad` |
| `--array-size` | `268435456` | f64 elements per worker |
| `--max-streams` | `20` | Measurement repetitions |
| `--exclude-streams` | `3` | Warm-up repetitions excluded from stats |
| `--results-dir` | `examples/stream-bench/results/` | CSV output directory |
| `--no-clean` | — | Skip `cargo clean` on subsequent runs |

## Output

One CSV per `(kernel, workers)` combination in `results/`:

```
results/synstream_stream_copy_w1.csv
results/synstream_stream_triad_w4.csv
...
```

CSV columns: `system, kernel, array_size, workers, elapsed_s, gb_s`

GB/s is computed from the mean kernel elapsed time across warm-up-excluded streams:
- Copy / Scale: `2 × array_size × 8 bytes / elapsed_s / 1e9`
- Add / Triad:  `3 × array_size × 8 bytes / elapsed_s / 1e9`

## Files

```
stream-bench/
├── Cargo.toml          plugin crate (libstreambench.so)
├── src/
│   ├── functions.rs    pure STREAM kernels (no SynStream dependencies)
│   └── lib.rs          #[no_mangle] CmTypes wrappers for dynamic loading
├── wrappers.rs         libloading symbol cache — injected at build time
├── reg.rs              get_func() dispatcher — injected at build time
├── graphs/             JSON graph definitions (one per kernel)
└── run_bench.py        build + sweep entry point
```

## Comparison baseline

The companion Timely Dataflow implementation lives in `timely-bench/src/bin/stream_bench.rs`.
Run `benchmarks/run_all_benchmarks.sh` to execute both systems and generate side-by-side plots.
