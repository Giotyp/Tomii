---
name: plugin-author
description: Write correct #[tomii_export]-annotated Rust functions (or @tomii_export C functions) that conform to Τομί's type system and FFI requirements
---

# Skill: plugin-author

Write Τομί plugin functions that conform to the `#[tomii_export]` macro convention
and the `CmTypes` type system. Covers Rust plugins, C plugins (via the converter), and
special patterns for mutations, tile kernels, and variadic fan-in.

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
- Parameters from `$res` predecessor results arrive as `&T` (shared reference)
- Parameters from `$ref` initialization variables arrive as `&T`
- Plain constant arguments (inline values in JSON/Python) arrive as owned `T`
- The return type must be owned and must implement `Into<CmTypes>`

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
| Custom structs | Must impl `Into<CmTypes>` + `From<CmTypes>` |

See `tomii-types/src/lib.rs` for the full `CmTypes` enum.

### Variadic fan-in (collecting all predecessor instances)

When a node uses a range `$res` dependency to collect all N outputs from a parallel
predecessor, the receiving function must be marked `variadic`:

```rust
#[tomii_export(variadic)]
pub fn aggregate(results: Vec<f64>) -> f64 {
    results.iter().sum()
}
```

The runtime collects all predecessor outputs and passes them as a single `Vec<T>`.
The Python graph uses `node.out(0, N)` to express this fan-in.

### Tile-aware kernels (for graph-coarsen Case A)

Tile kernels receive the tile index as an ordinary `usize` parameter. The index is the
task's instance number (0-based), passed as the last `usize` arg matching position in `args`:

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
node = app.node("compute", func="compute_tile",
                factor=n_tiles,
                args=[data_var, step_var, ss.usize(tile_size)])
# tile_idx is injected automatically as the instance index
```

### Mutation via raw pointers (advanced)

The `#[tomii_export]` macro does NOT support functions that mutate shared state via
raw pointers. For those, bypass the macro and write the raw FFI function directly:

```rust
use tomii_types::CmTypes;

#[no_mangle]
pub fn my_mutating_fn_cm(args: &[CmTypes]) -> CmTypes {
    // Extract arguments manually
    let matrix = match &args[0] {
        CmTypes::VecF64(v) => v,
        _ => panic!("expected VecF64"),
    };
    // ... mutate via unsafe raw pointer if needed ...
    CmTypes::Bool(true)
}
```

Register this function using its `_cm` suffix — it bypasses the macro's type wrapping.

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

## C plugins (converter)

### Basic pattern

Annotate C function declarations in a header file with `// @tomii_export`:

```c
// @tomii_export
float* compute_fft(const float* input, size_t n, size_t* out_len);
```

Set `func_path` in `app.build(...)` to the header file path. The converter generates
Rust FFI wrappers automatically.

### Annotations for arrays

```c
// @tomii_export (out_len=result_len, free=free_result)
double* process_batch(const double* data, size_t n, size_t tile, size_t* result_len);

// @tomii_export (param: array)      ← input array (read-only)
void analyze(const float* data, size_t n);

// @tomii_export (param: mut_array)  ← input array (mutable)
void normalize_inplace(float* data, size_t n);
```

Memory management: implement `free_result` (or `free_vector`, `free_matrix`, `free_string`)
in the C source to allow the runtime to free returned heap memory:

```c
void free_result(double* ptr) { free(ptr); }
```

### Build setup for C plugins

```python
build_result = app.build(
    func_path=str(HERE / "include" / "mylib.h"),
    plugin_manifest=str(HERE / "Makefile"),   # or CMakeLists.txt
    env={...},
    release=True,
)
```

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
| `the trait Into<CmTypes> is not implemented` | Return type not in CmTypes | Return a supported type (Vec, f64, etc.) |
| `expected &T, found T` | Passing owned where ref expected | Match predecessor's return type |
| `cannot find function in scope` | Missing `pub` | Add `pub fn` |
| `linker error: undefined reference` | Missing `crate-type` | Add `["dylib", "rlib"]` to `[lib]` |
| `wrong number of arguments` | Arg count mismatch with graph JSON | Align Rust params with JSON args array |

### Minimal verification run

After fixing build errors, run with 1 worker and 3 streams to verify the plugin loads and
executes without panics:

```python
app.run(dylib=build_result.dylib, workers=1, slots=1, max_streams=3)
```

If the plugin loads but panics at runtime, add `println!` debug output to the function
body to trace the argument values.

## See also

- [graph-build](graph-build.md) — for deciding what functions to write and their signatures
- [graph-coarsen](graph-coarsen.md) — for tile kernel patterns
- [run-validate](run-validate.md) — after fixing plugin issues
- [AGENT.md](../AGENT.md) — plugin quick-reference with Cargo.toml template
- `tomii-types/src/lib.rs` — CmTypes enum (complete list of supported types)
- `examples/stream-analytics/src/lib.rs` — example with conditions and variadic fan-in
- `examples/matrix-compute-C/include/matcomp.h` — example C plugin with array annotations
