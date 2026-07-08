---
title: Rust plugins
sidebar_label: Rust
---

A Rust plugin is a `cdylib` crate of plain functions annotated with
`#[tomii_export]`. The graph names these functions; the runtime loads the
compiled `.so` and calls them through generated wrappers.

## Annotating functions

From `examples/matrix-compute/src/lib.rs`:

```rust
use tomii_macro::tomii_export;

#[tomii_export]
pub fn generate_vector(n: usize) -> Vec<Complex32> {
    functions::generate_vector(n)
}

#[tomii_export]
pub fn fft_planner(buf_size: usize) -> Arc<dyn Fft<f32>> {
    functions::fft_planner(buf_size)
}

#[tomii_export]
pub fn compute_fft(fft_planner: &Arc<dyn Fft<f32>>, buffer: &mut Vec<Complex32>) {
    functions::compute_fft(fft_planner.clone(), buffer)
}
```

The function bodies are ordinary Rust — no Tomii types appear in signatures.
Parameters and return values must be representable in the `CmTypes` enum
(`tomii-types`): scalars, `String`, `Vec<T>`, complex numbers, and arbitrary
shared objects behind `Arc` (the FFT planner above is an
`Arc<dyn Fft<f32>>`). Borrowed parameters (`&T`, `&mut Vec<T>`) read from and
write to the per-slot result buffers in place.

## Variadic functions

A fan-in node receives a range of predecessor results as one `Vec`:

```rust
#[tomii_export(variadic)]
pub fn write_to_file(file_path: &str, buffers: Vec<DMatrix<Complex32>>) { ... }
```

This pairs with a range dependency in the graph:
`mat_mul.out(end=num_nodes)` delivers all 200 results as `buffers`.

## What the converter generates

At build time, `tomii-converter` reads the annotated source and generates two
files: `wrappers.rs` (a `_cm`-suffixed FFI twin per function, converting
`CmTypes` arguments to the real signature) and `reg.rs` (the name → function
registry the runtime uses to resolve `"function"` strings in the graph). You
never edit these files.

## Building

The Python builder drives everything:

```python
app.build(func_path="plugin/src/lib.rs",       # annotated source
          plugin_manifest="plugin/Cargo.toml", # the cdylib crate
          release=True)
```

`func_path` becomes the `FUNC_PATH` environment variable for the converter;
`WRAP_PATH` and `REG_PATH` override the generated file locations if you need
to (see the [environment reference](/docs/reference/environment)). Building
outside Python is the same mechanism:

```bash
FUNC_PATH=$(pwd)/examples/matrix-compute/src/lib.rs cargo build --release
```

One detail from the example source: in a `cdylib`, all `pub` functions are
exported symbols, so rustc warns about non-FFI types like `&str` with
`improper_ctypes_definitions`. The real FFI boundary is the generated `_cm`
twin, so the example suppresses the lint crate-wide
(`examples/matrix-compute/src/lib.rs`).

## Next

- [C plugins](/docs/guide/plugins/c) — the same mechanism driven by header
  annotations.
- [Polyglot plugins](/docs/guide/plugins/polyglot) — one graph, three kernel
  languages.
