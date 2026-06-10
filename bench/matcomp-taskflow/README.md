# matcomp-taskflow: Taskflow polyglot-regime baseline

This is a **Taskflow-only comparator**, not a head-to-head sweep. It provides the
scheduling baseline against which the polyglot Tomii example (`examples/matrix-compute`,
`matrix-compute-C`, `matrix-compute-python`) is measured: same 5-stage matrix-compute
DAG, same C kernels (FFTW3 single-precision + OpenBLAS `cblas_cgemm`), so the difference
isolates **what Tomii's polyglot marshaling costs versus a native C++ Taskflow lambda.**

## DAG per stream (N items)

```
gen_vec[0..N]     factor=N   generate_vector() → complex_f32[buf_size]
    ↓
compute_fft[0..N] factor=N   in-place FFT (fftwf_execute_dft, FFTW ESTIMATE)
    ↓
vec_to_mat[0..N]  factor=N   reshape flat vector → Matrix
    ↓
mat_mul[0..N]     factor=N   Matrix self-product (cblas_cgemm)
    ↓
write_res         factor=1   serialise all N matrices to file
```

Execution model: S independent `tf::Taskflow` clones submitted concurrently on one
shared `tf::Executor`.

## Per-task measurements

For every task the harness records `kernel_us` (pure lambda body), `dispatch_us`
(eligibility→pickup scheduling latency), and `total_us = kernel + dispatch`. Together
these answer: what does Tomii's marshaling add over a TF kernel, and what does TF's
dispatch add — the polyglot-cost decomposition used in `examples/matrix-compute`.

## Run

```bash
cd bench/matcomp-taskflow
# Polyglot regime: N=200, buf=100, S=4, W=4, 30 measured + 10 warmup streams,
# workers pinned to cores 3-6 (NUMA node 0), matching the polyglot run setup.
bash run_bench.sh                       # builds via CMake if needed
bash run_bench.sh /path/to/output.csv   # custom output path
```

Requires FFTW3 (single-precision) and OpenBLAS. Result CSVs are regenerated outputs and
are not committed to the repo — re-run `run_bench.sh` to produce fresh numbers.
