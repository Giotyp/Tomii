---
name: plugin-author
description: "Use when authoring or modifying a #[tomii_export] Rust function, a // @tomii_export C function, or an @tomii.export Python kernel. Produces a type-correct plugin body that conforms to Tomii's CmTypes system and FFI requirements."
---

# Skill: plugin-author

Write Τομί plugin functions that conform to the `#[tomii_export]` macro convention
and the `CmTypes` type system. Covers Rust plugins, C plugins (via the converter), Python
kernels, and special patterns for mutation, tile kernels, bulk execution and variadic
fan-in.

## Trigger

- A new computation stage needs a plugin function
- An existing function signature needs to change (e.g., for [graph-coarsen](graph-coarsen.md))
- The build fails with type mismatch or FFI errors
- Writing a plugin from scratch (usually follows [graph-build](graph-build.md))

## Rust plugins

### Basic pattern

```rust
use tomii_macro::tomii_export;

#[tomii_export]
pub fn function_name(param1: Type1, param2: &Type2) -> ReturnType {
    // ...
}
```

**Rules:**
- Function must be `pub`
- Parameters map to graph `args` in order (the same count, the same order)
- Primitives (`bool`, integers, floats, `&str`, `String`) are taken by value
- Non-primitive values (predecessor results, init objects) can be taken as `&T`
  (borrowed, no copy), `&mut T` (exclusive access, e.g. writing into a shared buffer
  init), or owned `T` (cloned on every call — avoid for large data)
- The instance index is an ordinary `usize` parameter fed by a `$index` arg
- The return type can be any owned `T: Any + Send + Sync` (`()` for none); it is stored as
  a type-erased value for successors

### Supported argument and return types

| Τομί type | Rust type |
|---------------|-----------|
| `usize` | `usize` |
| `i32`, `i64`, `i128` | `i32`, `i64`, `i128` |
| `u32`, `u64`, `u128` | `u32`, `u64`, `u128` |
| `f32`, `f64` | `f32`, `f64` |
| `bool` | `bool` |
| `String` | `String` (owned) / `&str` for input only |
| `Vec<T>` | `Vec<T>` (owned) / `&Vec<T>` for input |
| `Complex64` | `num_complex::Complex64` |
| Custom structs / handles | any `T: Any + Send + Sync`, stored type-erased; consumers take `&T` / `&mut T` |

See `tomii-types/src/lib.rs` for the `CmTypes` enum.

### Variadic fan-in (collecting all predecessor instances)

When a node uses a range `$res` dependency to collect all N outputs from a parallel
predecessor, the receiving function must be marked `variadic`:

```rust
#[tomii_export(variadic)]
pub fn aggregate(results: Vec<f64>) -> f64 {   // the LAST parameter must be Vec<T>
    results.iter().sum()
}
```

The runtime collects all predecessor outputs and passes them as a single `Vec<T>`.
The Python graph expresses this fan-in as the node's **last** argument with a Var end:
`node.out(0, n)` (an int end is inclusive: `out(0, 5)` collects six instances).

### Tile-aware kernels (for graph-coarsen Case A)

Tile kernels receive the tile index as an ordinary `usize` parameter fed by an explicit
`$index` argument in the graph:

```rust
#[tomii_export]
pub fn compute_tile(data: &Vec<f64>, step: usize, tile_size: usize, tile_idx: usize) -> Vec<f64> {
    let start = tile_idx * tile_size;
    let end = (start + tile_size).min(data.len());
    data[start..end].iter().map(|x| x * 2.0).collect()
}
```

In the Python graph:
```python
from tomii._types import TypedValue
IDX = TypedValue("$ref", "$index")
node = app.node("compute", func="compute_tile",
                factor=n_tiles,
                args=[data_var, step_var, tm.usize(tile_size), IDX])
```

### Bulk kernels (`#[tomii_export(bulk)]`)

```rust
#[tomii_export(bulk)]
pub fn scale(buf: &SharedBuf, idx: usize) {     // SharedBuf: a $ref buffer with per-cell writes
    buf.set(idx, buf.get(idx) * 2.0);
}
```

Adds a `scale_bulk_cm(start, end, args)` companion that runs instances `start..end` in
one call, extracting the static arguments once. The parameter named `idx`, `index` or `i`
is the per-instance index. The bulk companion discards return values, so bulk kernels
should write their results into a shared `$ref` buffer and return `()`. The runtime uses it when it dispatches a range at once
(`coalesce_barriers`, 1:1 fan-out dispatch; `no_fanout_bulk=True` disables the latter
for verification). Rust only — C plugins have no bulk form.

### Shared buffers written by many instances

`&mut T` gives one call exclusive access to a value. When many instances of one node
write **disjoint** parts of one shared buffer concurrently (each at its `$index`), an
exclusive borrow serializes them. Instead take `&T` of a buffer type with interior
mutability (per-cell `UnsafeCell` / atomics) and write through raw cell pointers — never
create a `&mut` to the whole buffer from several threads (that is undefined behaviour and
shows up as nondeterministic output with more than one worker). The graph's barriers
must guarantee that readers run after writers.

### Cargo.toml template

```toml
[package]
name = "my-plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["dylib", "rlib"]   # REQUIRED: dylib for runtime loading, rlib for macro

[dependencies]
tomii-types = { path = "../../tomii-types" }
tomii-macro = { path = "../../tomii-macro" }
# optional: nalgebra, num-complex, etc.
```

Inside the Τομί repository, plugins live under `examples/<name>/` and join the root
workspace — do not add a `[workspace]` table there. Elsewhere, adjust the paths (or use
git dependencies).

### Dynamic registration (experimental, off by default)

By default `app.build()` regenerates the runtime's wrappers from your source
(`FUNC_PATH`) and rebuilds `tomii-core`, so adding a kernel rebuilds the runtime. With
the `dynamic-registration` feature, the plugin `.so` describes its own kernels in a
`__tomii_exports` table and the runtime loads them at startup, so adding a kernel
rebuilds only the plugin. To opt in (see `examples/radar-pipeline`):

1. Plugin `Cargo.toml`: `[features] dynamic-registration = []` and
   `[build-dependencies] tomii-converter = { path = "../../tomii-converter" }`.
2. Plugin `build.rs`: when `CARGO_FEATURE_DYNAMIC_REGISTRATION` is set, call
   `tomii_converter::generate_self_contained_file(src/lib.rs, $OUT_DIR/tomii_exports.rs)`.
3. Plugin `src/lib.rs`: `#[cfg(feature = "dynamic-registration")] include!(concat!(env!("OUT_DIR"), "/tomii_exports.rs"));`
4. Build the plugin with `--features dynamic-registration` and the runtime
   (`tomii-core`) with `--features dynamic-registration`; run with `--dylib <plugin.so>`.
   The runtime exits with an error if the table is missing or its ABI version differs.

## C plugins (converter)

### Basic pattern

Annotate C function declarations in a header file with `// @tomii_export`:

```c
// @tomii_export
void* fft_planner(size_t buf_size);
```

Set `func_path` in `app.build(...)` to the header file path. The converter generates
Rust FFI wrappers automatically.

### Annotations

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

### Build setup for C plugins

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

## Python kernels

```python
import numpy as np
import tomii

@tomii.export
def compute_fft(v: np.ndarray) -> np.ndarray:
    return np.fft.fft(v).astype(np.complex64)
```

Wire them with `app.py_node("fft", fn=compute_fft, factor=n, args=[gen.out()])` and build
with `app.build(python_plugin=True)` (optionally `python_interpreter="python3.13t"` for the
free-threaded build). Use `@tomii.export(variadic=True)` for fan-in sinks. Kernels that
release the GIL (NumPy, BLAS) run in parallel across workers. See
`examples/matrix-compute-python/`.

## Build and verify

### Build the plugin

```python
build_result = app.build(
    func_path=str(HERE / "src" / "lib.rs"),
    plugin_manifest=str(HERE / "Cargo.toml"),
    env={"CARGO_TARGET_DIR": _TARGET_DIR},
    release=True,
    clean=False,  # True only when changing macro annotations
)
```

### Common errors and fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `last parameter of ... must be Vec<T>` | `variadic` on a function whose last parameter is not `Vec<T>` | Make the collected parameter last and `Vec<T>` |
| `expected &T, found T` | Passing owned where ref expected | Match predecessor's return type |
| `cannot find function in scope` | Missing `pub` | Add `pub fn` |
| `linker error: undefined reference` | Missing `crate-type` | Add `["dylib", "rlib"]` to `[lib]` |
| `wrong number of arguments` | Arg count mismatch with graph JSON | Align Rust params with JSON args array |

### Minimal verification run

After fixing build errors, run with 1 worker and 3 frames to verify the plugin loads and
executes without panics:

```python
proc = app.run(dylib=build_result.dylib, workers=1, slots=1, max_frames=3, max_runtime=60)
assert proc.returncode == 0, proc.stderr
```

If the plugin loads but panics at runtime, add `println!` debug output to the function
body to trace the argument values.

## See also

- [graph-build](graph-build.md) — for deciding what functions to write and their signatures
- [graph-coarsen](graph-coarsen.md) — for tile kernel patterns
- [run-validate](run-validate.md) — after fixing plugin issues
- `tomii-types/src/lib.rs` — CmTypes enum (complete list of supported types)
- `examples/stream-analytics/src/lib.rs` — example with conditions and variadic fan-in
- `examples/matrix-compute-C/include/matcomp.h` — example C plugin with array annotations
- `examples/matrix-compute-python/` — Python kernels
- `examples/radar-pipeline/` — network input, tiled kernels, dynamic registration
