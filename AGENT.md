# SynStream Agent Quick-Reference

## Plugin functions

Annotate pure Rust functions with `#[synstream_export]` — the build system generates all FFI
bridging automatically (no `wrappers.rs` or `reg.rs` required).

```rust
use synstream_macro::synstream_export;

#[synstream_export]
pub fn init_grid(n: usize) -> Vec<f64> { ... }

#[synstream_export]
pub fn compute_cell(grid: &Vec<f64>, n: usize, diag: usize, idx: usize) -> f64 { ... }
```

**When NOT to use `#[synstream_export]`**: functions that mutate shared state via raw pointers
(e.g. `with_any` → `*mut T`). Write those as `#[no_mangle] pub fn foo_cm(args: &[CmTypes]) -> CmTypes`
and extract arguments manually. See `synstream-types/` for `CmTypes` definition.

## Cargo.toml template

```toml
[package]
name = "my-plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["dylib", "rlib"]

[dependencies]
synstream-types = { path = "../../synstream-types" }
synstream-macro = { path = "../../synstream-macro" }
```

## run_wavefront.py skeleton

```python
import synstream as ss
from synstream._builder import find_workspace_root
from pathlib import Path

HERE = Path(__file__).resolve().parent
_TARGET_DIR = str(find_workspace_root() / "target")

graph = ss.Graph()
# ... build graph with graph.var() and graph.node() ...

build_result = graph.build(
    func_path=str(HERE / "src" / "lib.rs"),
    plugin_manifest=str(HERE / "Cargo.toml"),
    env={"CARGO_TARGET_DIR": _TARGET_DIR},
    release=True,
    clean=False,
)

graph.run(
    dylib=build_result.dylib,
    workers=4,
    core_offset=1,
    slots=1,
    max_streams=10,
    timing="timing.csv",
    coalesce_barriers=True,
    inline_continuation=True,
    use_rdtsc=True,
)
```

## Key `graph.run()` flags

| Flag | What it does | When to enable |
|------|-------------|----------------|
| `workers=N` | Rayon worker threads | Match physical cores |
| `core_offset=1` | Pin workers starting at CPU 1 | Always (leaves CPU 0 for OS) |
| `slots=N` | Concurrent in-flight streams | 1 for latency, >1 for throughput |
| `coalesce_barriers=True` | Batch barrier fan-outs into bulk tasks | Fine-grained graphs (wavefront) |
| `inline_continuation=True` | Run single-successor tasks inline | Reduces scheduling overhead |
| `use_rdtsc=True` | Use TSC for sub-µs timing | x86 only; improves timer precision |
| `system_threads=N` | Resolution threads | Default 1; rarely needs changing |

Run `python -m synstream --list-knobs` to see all available `graph.run()` options. Full Python API: `README.md`.
