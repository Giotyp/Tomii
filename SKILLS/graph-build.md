---
name: graph-build
description: Translate a natural-language or pseudocode computation description into a correct SynStream Python graph definition with appropriate dependency types, factors, and node structure
---

# Skill: graph-build

Translate a computation description into a correct SynStream Python graph. Produces a
complete `run_bench.py` and a `src/lib.rs` plugin skeleton with all required
`#[synstream_export]` stubs.

## Trigger

- User describes a pipeline or computation to express as a SynStream graph
- Migrating an existing streaming pipeline into SynStream
- Adding a new stage to an existing graph

## Steps

### 1. Load the graph construction schema

```bash
python -m synstream --schema
```

Read the full output. Pay attention to:
- The `optimization_hint` field on `factor` — it warns when large factors create too many tasks
- Argument type semantics: `$res` (data dependency), `$barrier` (fan-in sync), `$dep` (ordering only), `$ref` (init variable)
- Available node fields: `factor`, `group_size`, `priority`, `condition`, `loop`, `use_workers`

### 2. Map computation stages to nodes

For each computation stage in the user's description, determine:

| Question | If yes → |
|----------|----------|
| Does it operate on N independent items in parallel? | Set `factor=N` |
| Does it consume a predecessor's output data? | Use `node.out()` (`$res`) |
| Does it need ALL instances of a predecessor to complete first? | Use `node.wait()` (`$barrier`) |
| Does it just need ordering (no data)? | Use `node.dep()` (`$dep`) |
| Does it consume a pre-computed initialization? | Use `$ref` referencing the var name |
| Is it conditional on a runtime predicate? | Use `ss.Condition(...)` |

### 3. Choose the fan-in strategy

When a downstream stage needs results from all N instances of an upstream parallel stage:

| Pattern | Use | When |
|---------|-----|------|
| Synchronize then pass data | `node.out(0, N)` (variadic `$res`) + `#[synstream_export(variadic)]` | Need all N outputs as a `Vec<T>` |
| Synchronize without data | `node.wait(predecessor)` (`$barrier`) | Only need to know all finished |
| Partial synchronization | `node.wait(predecessor, group_by=K)` | Upstream N instances, downstream N/K instances each waiting for K |

### 4. Draft plugin function signatures

For each node, determine the Rust function signature:
- Parameters from `$res` predecessors arrive as `&T` (reference to the predecessor's return value)
- Parameters from `$ref` inits arrive as `&T`
- Plain arguments (constants) arrive as owned values (`usize`, `f64`, `String`, etc.)
- For `#[synstream_export(variadic)]` functions, the collected arg arrives as `Vec<&T>` or `Vec<T>`
- The return type must be a concrete owned type that maps to a `CmTypes` variant (see `synstream-types/`)

### 5. Construct the Python graph

Follow this template (see also [AGENT.md](../AGENT.md) Python skeleton):

```python
import synstream as ss
from synstream._builder import find_workspace_root
from pathlib import Path
import argparse

HERE = Path(__file__).resolve().parent
_TARGET_DIR = str(find_workspace_root() / "target")

parser = argparse.ArgumentParser()
parser.add_argument("--workers", type=int, default=4)
parser.add_argument("--max-streams", type=int, default=20)
args = parser.parse_args()

app = ss.Graph()

# --- Initializations (pre-computed objects, shared across all streams) ---
n = app.var("n", 1024)                              # constant
data = app.var("data", func="init_data", args=[n])  # function-computed

# --- Nodes (streaming computation) ---
stage_a = app.node("stage_a", func="compute_a",
                   factor=n,
                   args=[ss.f64(0.0), data])

stage_b = app.node("stage_b", func="compute_b",
                   factor=n,
                   args=[stage_a.out(0)])  # 1:1 result dependency

result = app.node("result", func="aggregate",
                  args=[stage_a.wait()])   # barrier: wait for all stage_a

build_result = app.build(
    func_path=str(HERE / "src" / "lib.rs"),
    plugin_manifest=str(HERE / "Cargo.toml"),
    env={"CARGO_TARGET_DIR": _TARGET_DIR},
    release=True,
    clean=False,
)

app.run(
    dylib=build_result.dylib,
    workers=args.workers,
    core_offset=1,
    max_streams=args.max_streams,
    exclude_streams=5,
    report="report.json",
    timing="timing.txt",
)
```

For a richer example with conditions, priorities, and grouped barriers, see:
`examples/stream-analytics/run_bench.py`

### 6. Write the plugin skeleton

Create `src/lib.rs` with stubs for all required functions:

```rust
use synstream_macro::synstream_export;

#[synstream_export]
pub fn init_data(n: usize) -> Vec<f64> {
    vec![0.0; n]
}

#[synstream_export]
pub fn compute_a(init: f64, data: &Vec<f64>, idx: usize) -> f64 {
    // TODO: implement
    data[idx] + init
}

#[synstream_export(variadic)]
pub fn aggregate(results: Vec<f64>) -> f64 {
    results.iter().sum()
}
```

And `Cargo.toml`:
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

### 7. Validate graph structure

Export to JSON and visually inspect:

```python
import json
print(json.dumps(json.loads(app.to_json()), indent=2))
```

Verify:
- All predecessor node names referenced in `args` match defined node names
- No cycles: for each `$res` dependency, the predecessor appears earlier in the node list
- Factor values are reasonable (see `optimization_hint` in `--schema`: factors > 256 create many fine-grained tasks)

### 8. Set initial runtime parameters

Start conservative for the first run:
- `workers` = physical core count
- `slots = 1` (minimize latency; increase later for throughput if needed)
- `max_streams = 10`, `exclude_streams = 2`
- `report = "report.json"` (always; needed by [diagnose](diagnose.md))

## Output

- `run_bench.py` — complete Python graph definition
- `src/lib.rs` — plugin skeleton with all `#[synstream_export]` stubs
- `Cargo.toml` — plugin crate manifest

## Common mistakes

| Mistake | Fix |
|---------|-----|
| Returning `&str` from a plugin function | Return `String` (owned) |
| Forgetting `pub` on exported functions | Add `pub fn` |
| Using `$res` to collect all N outputs but forgetting `(variadic)` | Add `#[synstream_export(variadic)]` |
| Factor > 1000 causing high scheduling overhead | Read `optimization_hint` in `--schema`; try `group_size` |
| Predecessor name typo in `args` | Graph will fail at parse time with a diagnostic |

## See also

- [plugin-author](plugin-author.md) — for more complex plugin patterns (C, manual CmTypes, mutations)
- [run-validate](run-validate.md) — next step after building the graph
- [AGENT.md](../AGENT.md) — quick-reference plugin skeleton and Cargo.toml template
