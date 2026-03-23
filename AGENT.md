# SynStream Agent Quick-Reference

## Plugin functions

Annotate pure Rust functions with `#[synstream_export]` — the build system generates all FFI
bridging automatically (no `wrappers.rs` or `reg.rs` required).

```rust
use synstream_macro::synstream_export;

#[synstream_export]
pub fn init_data(n: usize) -> Vec<f64> { ... }

#[synstream_export]
pub fn process_item(data: &Vec<f64>, idx: usize) -> f64 { ... }
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

## Python script skeleton

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
    max_streams=10,
)
```

## Key `graph.run()` flags

| Flag | What it does |
|------|-------------|
| `workers=N` | Rayon worker threads |
| `core_offset=1` | Pin workers starting at CPU 1 |
| `slots=N` | Concurrent in-flight streams |
| `max_streams=N` | Total streams to process |
| `timing="timing.csv"` | Write per-node timing CSV |
| `report="report.json"` | Write JSON performance report (per-node stats, critical path, bottleneck hints) |

Run `python -m synstream --list-knobs-json` for all `graph.run()` options with search hints (machine-readable JSON).
Run `python -m synstream --schema` for graph construction parameters (node options, arg types).
