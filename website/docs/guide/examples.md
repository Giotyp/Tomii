---
title: Examples
sidebar_label: Examples
---

Each example under `examples/` is a self-contained workload with the same
structure: `run_bench.py` (or `run_bench.sh`) builds the plugin and runs the
graph; `verify.py` / `verify.sh` checks correctness
(`examples/README.md`). The example code is not on this site — the links go
to the repository.

## Capability matrix

| Example | Plugin language | DAG features | Verify | External deps |
|---|---|---|---|---|
| [matrix-compute](https://github.com/Giotyp/Tomii/tree/main/examples/matrix-compute) | Rust (nalgebra, rustfft) | linear chain, shared initialization | `verify.sh` | none |
| [matrix-compute-C](https://github.com/Giotyp/Tomii/tree/main/examples/matrix-compute-C) | C (FFTW, OpenBLAS) | same topology, C plugin | `make validation` | `libfftw3f`, `libopenblas` |
| [matrix-compute-python](https://github.com/Giotyp/Tomii/tree/main/examples/matrix-compute-python) | Python (NumPy) | same topology, GIL / free-threaded demo | — | `uv`, NumPy |
| [stream-analytics](https://github.com/Giotyp/Tomii/tree/main/examples/stream-analytics) | Rust | conditional branches, grouped barriers, `$dep` ordering, priorities | `verify.py` | none |
| [mapreduce](https://github.com/Giotyp/Tomii/tree/main/examples/mapreduce) | C | fan-out / fan-in, variadic barrier | `verify.sh` | none (GCC) |
| [gpu-vectoradd](https://github.com/Giotyp/Tomii/tree/main/examples/gpu-vectoradd) | CUDA C++ | `use_workers` GPU-thread pinning, host↔device copies | inline | CUDA GPU |
| [radar-pipeline](https://github.com/Giotyp/Tomii/tree/main/examples/radar-pipeline) | Rust + C (FFTW) / CUDA (cuFFT) | UDP `$network` source, 1:1 packet edges, grouped barriers, kernel-`.so` swap (CPU↔GPU) | `verify.py` (ground truth + coverage) | `libfftw3f`; CUDA 12 for `--gpu` |
| [agent-tuning](https://github.com/Giotyp/Tomii/tree/main/examples/agent-tuning) | — (harness) | 4-arm verifier-gated knob search | per-workload gate | Optuna, `claude` CLI |
| [scheduler-plugin](https://github.com/Giotyp/Tomii/tree/main/examples/scheduler-plugin) | Rust | pluggable `TaskScheduler` (FIFO) | — | none |

## Running them

All commands run from the repository root with the `tomii` package installed
(`examples/README.md`):

```bash
python examples/matrix-compute/run_bench.py          # Rust plugin
python examples/matrix-compute-C/run_bench.py        # C plugin
bash   examples/matrix-compute-python/run_bench.sh   # Python plugin
python examples/stream-analytics/run_bench.py        # conditional branching
python examples/mapreduce/run_bench.py               # Map→Reduce wordcount
python examples/gpu-vectoradd/run_bench.py           # CUDA (GPU required)
python examples/radar-pipeline/run_bench.py          # FMCW radar over UDP
```

## Where each one is covered in this guide

- matrix-compute is the walkthrough in
  [Your first graph](/docs/guide/getting-started/first-graph) and the Rust
  half of [Rust plugins](/docs/guide/plugins/rust).
- The three matrix-compute variants together are the subject of
  [Polyglot plugins](/docs/guide/plugins/polyglot).
- stream-analytics drives [Control flow](/docs/guide/graphs/control-flow).
- mapreduce and matrix-compute-C appear in [C plugins](/docs/guide/plugins/c).
- agent-tuning is documented in
  [Agent tuning](/docs/guide/tuning/agent-tuning).
- radar-pipeline is the network-driven example in
  [Network sources](/docs/guide/graphs/network-sources) and the subject of the
  radar section of the [benchmarks page](/docs/overview/benchmarks); its
  README carries the signal-chain diagram and full runbook.

Comparator benchmarks against Taskflow and TBB live under `bench/`, not
`examples/` — see the [benchmarks page](/docs/overview/benchmarks) for
methodology and measured numbers.
