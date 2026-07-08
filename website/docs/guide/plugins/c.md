---
title: C plugins
sidebar_label: C
---

A C plugin is a shared library plus a header whose functions carry
`// @tomii_export` comment annotations. The converter reads the header and
generates the same wrapper/registry pair it generates for Rust, so the
runtime treats both identically.

## Annotating a header

From `examples/matrix-compute-C/include/matcomp.h`:

```c
/* Returns a malloc'd array of n complex_f32. The wrapper copies the data
 * into a Rust Vec and calls free_vector to release the C allocation. */
// @tomii_export(out_len=n, free=free_vector)
complex_f32* generate_vector(size_t n);

// @tomii_export
void* fft_planner(size_t buf_size);

/* buffer is borrowed mutably; buffer_len is derived from the Vec length. */
// @tomii_export(buffer: mut_array)
void compute_fft(void* planner, complex_f32* buffer, size_t buffer_len);

// @tomii_export(vector: array)
void* vec_to_mat(const complex_f32* vector, size_t vector_len);

// @tomii_export(free=free_string)
char* get_out_file(const char* env_var, const char* out_file);

// @tomii_export(variadic)
void write_to_file(const char* file_path, void** buffers, size_t num_buffers);
```

Annotation options used above:

| Option | Meaning |
|---|---|
| `out_len=<param>` | The return value is an array whose length is the named parameter; the wrapper copies it into a `Vec` |
| `free=<fn>` | Helper called to release the C allocation after copying to Rust |
| `<param>: array` | Pointer parameter is a borrowed array; its `_len` companion is derived from the `Vec` length |
| `<param>: mut_array` | Same, borrowed mutably for in-place modification |
| `variadic` | Fan-in: the function receives a pointer-array of predecessor results plus a count |

Opaque state crosses the boundary as `void*` handles: `fft_planner` returns
an FFTW plan, `vec_to_mat` returns a `Matrix*`, and `mat_mul` consumes two of
them. The runtime stores and routes handles without inspecting them. Memory
helpers (`free_vector`, `free_matrix`, `free_string`) are loaded by the
wrappers but not exposed to the graph.

## Building and running

The C library builds with Make, not Cargo. The example runner builds
`libmatcomp_c.so` via `make`, then rebuilds `tomii-core` with `FUNC_PATH`
pointing at the header (`examples/matrix-compute-C/README.md`):

```bash
# Needs libfftw3f + libopenblas discoverable via pkg-config
python examples/matrix-compute-C/run_bench.py --workers 4
```

From the Python builder, point `func_path` at the header:

```python
app.build(func_path="include/matcomp.h")
```

## A second example: mapreduce

`examples/mapreduce` is a pure-C word-count plugin with no external
dependencies. It shows the canonical fan-out / fan-in shape: `num_shards`
parallel `map_tokens` tasks feed a single variadic `reduce_all` node
(`examples/mapreduce/README.md`):

```bash
python examples/mapreduce/run_bench.py --num-shards 32 --workers 4
bash examples/mapreduce/verify.sh   # byte-compares against golden output
```
