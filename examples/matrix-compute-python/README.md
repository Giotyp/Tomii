# matrix-compute-python

FFT + matrix-multiply benchmark using a Python plugin (NumPy, decorated with
`@tomii.export`). Demonstrates zero-boilerplate Python plugins and the GIL / free-threaded
Python story.

## Graph topology

Same as `matrix-compute/` (gen → fft_planner → compute_fft → vec_to_mat → mat_mul → write_result).

## Requirements

- Python 3.10+ (stock)
- `uv` (used by `run_bench.sh` to create an isolated venv)
- For free-threaded mode: Python 3.13t (`python3.13t`)

## Build and run

```bash
# Canonical entry point — creates venv, installs tomii + numpy, runs:
bash examples/matrix-compute-python/run_bench.sh

# Free-threaded Python (removes GIL entirely):
bash examples/matrix-compute-python/run_bench.sh --python-interpreter python3.13t

# Quick iteration:
bash examples/matrix-compute-python/run_bench.sh --no-clean
```

## GIL behaviour

| Python build | mat_mul / compute_fft | Notes |
|---|---|---|
| CPython 3.12 (stock) | Parallel | NumPy/BLAS release GIL internally |
| CPython 3.13t (free-threaded) | Parallel | GIL absent; all Python runs in parallel |

`@tomii.procs()` is available for pure-Python functions (loops, comprehensions) that
would otherwise serialise under the GIL; `matcomp.py` omits it because NumPy already
releases the GIL.

## Tuning knobs

Passed directly to `run_bench.sh` and forwarded to `run_bench.py`:

| Flag | Default | Effect |
|------|---------|--------|
| `--workers` | 2 | Rayon worker threads |
| `--slots` | 2 | Concurrent stream slots |
| `--python-interpreter` | `python3` | Python binary for the venv |
| `--no-clean` | — | Skip library rebuild |
