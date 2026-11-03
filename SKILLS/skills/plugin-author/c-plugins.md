# C plugins (converter)

## Basic pattern

Annotate C function declarations in a header file with `// @tomii_export`:

```c
// @tomii_export
void* fft_planner(size_t buf_size);
```

Set `func_path` in `app.build(...)` to the header file path. The converter generates
Rust FFI wrappers automatically.

## Annotations

The annotation body is a comma-separated list directly after `@tomii_export`, with no
space before `(`. Unknown tokens are silently ignored, so spell them exactly:

```c
// @tomii_export(out_len=n, free=free_vector)    ← returns a malloc'd array of length n, freed after copy
complex_f32* generate_vector(size_t n);

// @tomii_export(vector: array)                   ← read-only array; its length param is derived
void* vec_to_mat(const complex_f32* vector, size_t vector_len);

// @tomii_export(buffer: mut_array)               ← mutable array (borrowed mutably)
void compute_fft(void* planner, complex_f32* buffer, size_t buffer_len);

// @tomii_export(free=free_string)                ← returned C string freed after copy
char* get_out_file(const char* env_var, const char* out_file);

// @tomii_export(variadic)                        ← last (void**, size_t) pair collects predecessors
void write_to_file(const char* file_path, void** buffers, size_t num_buffers);
```

Implement the named free functions in the C library (e.g. `void free_vector(complex_f32* p) { free(p); }`).

## Build setup for C plugins

Build the C shared library yourself, generate the runtime's wrappers from the header,
and pass the library to `run()`:

```python
subprocess.run(["make", "-C", str(HERE)], check=True)        # builds libmylib.so
app.build(func_path=str(HERE / "include" / "mylib.h"),        # no plugin_manifest
          release=True, env={...})
app.run(dylib=str(HERE / "libmylib.so"), ...)
```

(`plugin_manifest` is a Cargo manifest; it is not used for C plugins.) Full example:
`examples/matrix-compute-C/run_bench.py`.
