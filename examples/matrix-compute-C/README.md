# matrix-compute-C

FFT + matrix-multiply benchmark using a C plugin (FFTW + OpenBLAS). Demonstrates
polyglot plugin support: the same Tomii graph topology as `matrix-compute/` but
driven by C functions compiled to a shared library.

## Graph topology

Same as `matrix-compute/` (gen → fft_planner → compute_fft → vec_to_mat → mat_mul → write_result).

## Requirements

- GCC
- FFTW3 single-precision (`libfftw3f`) and OpenBLAS (`libopenblas`) — must be
  discoverable via `pkg-config`
- Python 3.10+

On Ubuntu/Debian:
```bash
sudo apt install libfftw3-dev libopenblas-dev pkg-config
```

## Build and run

```bash
# From repo root:
python examples/matrix-compute-C/run_bench.py

# The runner builds libmatcomp_c.so via Make, then rebuilds tomii-core with
# FUNC_PATH pointing at the C header before launching the graph.
python examples/matrix-compute-C/run_bench.py --workers 4 --no-clean
```

## Tuning knobs

| Flag | Default | Effect |
|------|---------|--------|
| `--workers` | 2 | Rayon worker threads |
| `--system-threads` | 3 | Resolution threads |
| `--slots` | 2 | Concurrent stream slots |
| `--no-clean` | — | Skip library rebuild |

## Note on validation

No standalone verifier is provided; the C library's `validation` target
(`make validation`) builds a self-contained binary that cross-checks outputs
against `matrix-compute/`'s Rust reference.
