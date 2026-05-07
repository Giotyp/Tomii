# Examples

Each subdirectory is a self-contained workload. All share the same structure:
`run_bench.py` (or `run_bench.sh`) builds the plugin and runs the graph;
`verify.py` / `verify.sh` checks correctness.

## Capability matrix

| Example | Plugin language | DAG features | Verify |
|---|---|---|---|
| [matrix-compute](matrix-compute/) | Rust (nalgebra, rustfft) | linear chain, shared initialisation | `verify.sh` |
| [matrix-compute-C](matrix-compute-C/) | C (FFTW, OpenBLAS) | same topology, C plugin | — |
| [matrix-compute-python](matrix-compute-python/) | Python (NumPy, `@tomii.export`) | same topology, GIL / free-threaded demo | — |
| [stream-analytics](stream-analytics/) | Rust | conditional branches, grouped barriers, `$dep` ordering, priority levels | `verify.py` |
| [mapreduce](mapreduce/) | C | fan-out / fan-in (Map→Reduce), variadic barrier | `verify.sh` |
| [gpu-vectoradd](gpu-vectoradd/) | CUDA C++ | `use_workers` GPU-thread pinning, host↔device copies | inline |

## Quick start

```bash
# Rust plugin (no system deps):
python examples/matrix-compute/run_bench.py

# C plugin (needs fftw3f + openblas via pkg-config):
python examples/matrix-compute-C/run_bench.py

# Python plugin:
bash examples/matrix-compute-python/run_bench.sh

# Conditional branching:
python examples/stream-analytics/run_bench.py

# Map→Reduce wordcount:
python examples/mapreduce/run_bench.py

# CUDA vector-add (GPU required):
python examples/gpu-vectoradd/run_bench.py
```

All commands are run from the repo root with the Tomii Python package available
(`source .venv/bin/activate` or prefix with `uv run`).
