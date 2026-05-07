# matrix-compute

FFT + matrix-multiply benchmark using a Rust plugin (nalgebra, rustfft).

## Graph topology

```
gen_vec(buf_size) ─────────────────────────────────► vec_to_mat
                  └─► fft_planner ─► compute_fft ──► vec_to_mat
                                                      └─► mat_mul ─► write_result
```

## Requirements

- Rust toolchain (`cargo`)
- Python 3.10+ with `tomii` installed (`pip install -e .` from repo root, or `uv run`)

## Build and run

```bash
# From repo root (incremental build + run):
python examples/matrix-compute/run_bench.py

# Tune parameters:
python examples/matrix-compute/run_bench.py --workers 4 --slots 8 --max-streams 100
```

The first run compiles `libmatcomp.so` and regenerates the function registry.
Subsequent runs skip the build (`--no-clean` skips the clean step).

## Verify

```bash
bash examples/matrix-compute/verify.sh
```

Builds the `perfval` binary and runs numerical validation. Prints `PASS` on success.

## Tuning knobs

| Flag | Default | Effect |
|------|---------|--------|
| `--workers` | 2 | Rayon worker threads |
| `--system-threads` | 3 | Resolution threads |
| `--slots` | 2 | Concurrent stream slots |
| `--max-streams` | 1 | Streams processed before exit |
| `--batching-size` | 1 | Tasks dispatched per batch |
| `--no-clean` | — | Skip library rebuild |

## Output

Timing CSV written to `out.txt` (configurable). The `perfval` binary cross-checks matrix
output against a NumPy reference and writes per-element error statistics to `validation.txt`.
