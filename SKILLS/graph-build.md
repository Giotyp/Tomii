---
name: graph-build
description: "Use when translating a natural-language or pseudocode pipeline into a Tomii graph. Produces a complete run_bench.py and src/lib.rs plugin skeleton with all required #[tomii_export] stubs and correct dependency types."
---

# Skill: graph-build

Translate a computation description into a correct Τομί Python graph. Produces a
complete `run_bench.py` and a `src/lib.rs` plugin skeleton with all required
`#[tomii_export]` stubs.

## Trigger

- User describes a pipeline or computation to express as a Τομί graph
- Migrating an existing streaming pipeline into Τομί
- Adding a new stage to an existing graph

## Steps

### 1. Read the API reference

```bash
python -m tomii --schema
```

Use it together with `website/docs/reference/json-graph-format.md` and
`website/docs/guide/graphs/` (nodes-and-vars, control-flow, network-sources). The rules
below are the ones agents most often get wrong.

### 2. Map computation stages to nodes

| Question | If yes → |
|----------|----------|
| Does it operate on N independent items in parallel? | `factor=N` (prefer a `Var`, e.g. `factor=n`) |
| Does it consume a predecessor's output value? | `pred.out(...)` (`$res`) |
| Does it need predecessor instances to finish, without their value? | `pred.wait(...)` (`$barrier`) |
| Does it need ordering only, and the producer's result is otherwise unused? | `pred.dep(...)` (`$dep`; the runtime then skips storing the result) |
| Does it read a pre-computed object (config, buffer, plan)? | pass the `Var` returned by `app.var(...)` (`$ref`) |
| Does an instance need its own index? | pass `TypedValue("$ref", "$index")` explicitly |
| Is it conditional on a runtime predicate? | `condition=tm.Condition(...)` |
| Does it run once after all frames? | `app.post_node(...)` |
| Does it read packets from the network? | `app.network(...)` + the `$network` predecessor (see below) |

### 3. Index selection — the rules that matter

`out` / `wait` / `dep` take `(start=0, end=None, *, group_by=None)` and select predecessor
instances as **offsets relative to the consuming instance's index**:

| Python | JSON `indexes` | Selects |
|--------|----------------|---------|
| `p.out()` / `p.out(0)` | `"0"` | the predecessor instance with the same index (instance 0 for a `factor=1` consumer) |
| `p.out(-1)` | `"-1"` | the previous instance |
| `p.out(0, n)` with `n` a **Var** | `"0-n"` | offsets `0 … n-1` (a Var end is **exclusive**) |
| `p.out(0, 5)` with an **int** | `"0-5"` | offsets `0 … 5` — **six** instances (an int end is **inclusive**) |
| `p.out([0, 2, 4])` | `"0,2,4"` | an exact list |

- Prefer Var-ended ranges (`app.var("n", 64)`), so the range and the factor cannot drift apart.
- If the number of selected instances equals the consumer's `factor`, the edge is 1:1
  (instance `i` gets element `i`). Otherwise every instance collects all selected
  instances, and the kernel's last parameter must be variadic (`Vec<T>`, see step 5).
- A collect-all `$res` must be the **last** argument of the node.
- `p.wait()` with no range waits for one instance only. To wait for **all** instances use
  `p.wait(0, n)`.

### 4. Choose the fan-in strategy

| Pattern | Python | Kernel |
|---------|--------|--------|
| All N outputs as data | `args=[..., p.out(0, n)]` (last arg) | `#[tomii_export(variadic)] fn f(..., xs: Vec<T>)` |
| All N finished, no data | `p.wait(0, n)` | no parameter for the barrier's value is needed |
| Per-group sync | `p.wait(0, n, group_by=k)` on a consumer with `factor = n / k` | consumer `i` fires when `p[i*k .. (i+1)*k]` finish |
| Ordering only | `p.dep(0, n)` | the slot receives no value |
| Different factors, shared data | write into a `$ref` buffer indexed by `$index`; order with `wait(...)` | read the buffer at `$index` |

**Mixing a grouped barrier with a global barrier on one node** (each consumer group waits
for *its own* slice of one producer and for *all* instances of another) requires
`group_size` on the consumer to equal `factor / (pred_count / group_by)` — the number of
consumer instances released by one group:

```python
# load:    factor = n_chunks * parts_per_chunk   (one task per part)
# prepare: factor = n_prep                        (needed by every combine task)
# combine: factor = n_chunks * tasks_per_chunk    (chunk c's tasks need only chunk c's parts)
combine = app.node("combine", func="combine", factor=n_combine,
                   group_size=tasks_per_chunk,   # = n_combine / (n_load / parts_per_chunk)
                   args=[cfg, buffers, IDX,
                         load.wait(0, n_load, group_by=parts_per_chunk),  # one group per chunk
                         prepare.wait(0, n_prep)])                        # all prepare tasks
```

Chunk `c`'s `combine` tasks start as soon as chunk `c` is loaded, while later chunks are
still loading.

If `group_size` does not match, frames either never complete or fire early. Run a short
validation ([run-validate](run-validate.md)) after every barrier change.

### 5. Known traps (check before running)

| Symptom | Cause | Fix |
|---------|-------|-----|
| Frames stall; some consumer instances never run | Consumer has `group_size` < `factor`, an **ungrouped** `$barrier` over several instances of P, **and** a single-index `$res` on the same P (e.g. `p.out()` + `p.wait(0, n)`). Current runtimes reject this graph at load | Any one of: group the barrier (`group_by`, step 4); give the `$res` one index per consumer instance; use `p.dep()` instead of `p.out()` if the value is not needed |
| Frames never complete or fire early | Grouped and global barriers on one node with a `group_size` that does not match the barrier groups | `group_size = factor / (pred_count / group_by)` (step 4) |
| Off-by-one instance count | int range end is inclusive (`out(0, 5)` = 6 instances) | Use a Var end |
| "collect-all must be last" parse error | variadic `$res` not the last arg | Move it last |
| Barrier waits for instance 0 only | `p.wait()` without a range on a `factor=1` consumer | `p.wait(0, n)` |

### 6. Construct the Python graph

```python
import argparse
from pathlib import Path

import tomii as tm
from tomii._builder import find_workspace_root
from tomii._types import TypedValue

HERE = Path(__file__).resolve().parent
_TARGET_DIR = str(find_workspace_root() / "target")
IDX = TypedValue("$ref", "$index")   # the consuming instance's index

parser = argparse.ArgumentParser()
parser.add_argument("--workers", type=int, default=4)
parser.add_argument("--max-frames", type=int, default=20)
args = parser.parse_args()

app = tm.Graph()

# --- Initializations (computed once, shared across all frames) ---
n = app.var("n", 1024)                              # constant (also used as a factor)
data = app.var("data", func="init_data", args=[n])  # function-computed

# --- Nodes (run once per frame) ---
stage_a = app.node("stage_a", func="compute_a", factor=n,
                   args=[tm.f64(0.0), data, IDX])
stage_b = app.node("stage_b", func="compute_b", factor=n,
                   args=[stage_a.out()])             # 1:1: stage_b[i] gets stage_a[i]
result = app.node("result", func="aggregate",
                  args=[stage_b.out(0, n)])          # collect all n (Var end), last arg

build_result = app.build(
    func_path=str(HERE / "src" / "lib.rs"),
    plugin_manifest=str(HERE / "Cargo.toml"),
    env={"CARGO_TARGET_DIR": _TARGET_DIR},
    release=True,
    clean=False,
)

proc = app.run(
    dylib=build_result.dylib,
    workers=args.workers,
    core_offset=1,
    max_frames=args.max_frames,
    exclude_frames=5,
    report="report.json",
    timing="timing.txt",        # required for the report to be written
)
if proc.returncode != 0:
    raise SystemExit(f"tomii run failed (rc={proc.returncode})")
```

More complete graphs (conditions, priorities, grouped barriers, `$dep`, post-nodes,
network input, Python kernels) are in the repository's `examples/` directory.

### 7. Network-input graphs

```python
from tomii._node import Node
net = Node("$network", func="")           # virtual predecessor for packets
app.network(socket_type="udp", num_sockets=n_sockets, packet_length=pkt_len,
            frame_packets=pkts_per_frame, buffer_depth=2000, address=addr_var,
            start_port=port_var, extract_packet_func="process_packet",
            id_function="get_frame_id", index_function=tm.IndexFunc("get_packet_index"))
rx = app.node("rx", func="decode", factor=pkts_per_frame,
              args=[net.out(0, pkts_per_frame), cfg, IDX])   # 1:1 packet → instance
```

Then tune ingest with [network-ingest](network-ingest.md); network graphs need
`receiver_threads >= 1`.

### 8. Write the plugin skeleton

```rust
use tomii_macro::tomii_export;

#[tomii_export]
pub fn init_data(n: usize) -> Vec<f64> {
    vec![0.0; n]
}

#[tomii_export]
pub fn compute_a(init: f64, data: &Vec<f64>, idx: usize) -> f64 {
    data[idx] + init
}

#[tomii_export]
pub fn compute_b(a: f64) -> f64 {
    a * 2.0
}

#[tomii_export(variadic)]
pub fn aggregate(results: Vec<f64>) -> f64 {   // variadic: last parameter must be Vec<T>
    results.iter().sum()
}
```

`Cargo.toml`. Inside the Τομί repository, put the plugin under `examples/<name>/`: it then
joins the root workspace (`examples/*`) — do not add a `[workspace]` table there, or the
runtime build fails with "multiple workspace roots". Outside the repository, adjust the
paths (or use git dependencies):

```toml
[package]
name = "my-plugin"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["dylib", "rlib"]

[dependencies]
tomii-types = { path = "../../tomii-types" }
tomii-macro = { path = "../../tomii-macro" }
```

### 9. Validate graph structure

```python
app.save_json("graph.json")
```
```bash
python -m tomii --visualize graph.json --ascii
```

Verify:
- Every predecessor name exists and every edge points backwards (no cycles)
- Range ends are Vars, or ints that are deliberately inclusive
- Each node with both a grouped and a global barrier has a matching `group_size` (step 4)
- No trap from step 5 applies

### 10. Initial runtime parameters

- `workers` = number of physical cores you may use minus support threads; `core_offset=1`
- `slots=1` (minimize latency; raise later for throughput)
- `max_frames=10`, `exclude_frames=2` for the first run
- `report="report.json"` **and** `timing="timing.txt"` (needed by [diagnose](diagnose.md))

## Output

- `run_bench.py` — complete Python graph definition
- `src/lib.rs` — plugin skeleton with all `#[tomii_export]` stubs
- `Cargo.toml` — plugin crate manifest

## See also

- [plugin-author](plugin-author.md) — kernel patterns (C, Python, `bulk`, `&mut`, dynamic registration)
- [run-validate](run-validate.md) — next step after building the graph
- [network-ingest](network-ingest.md) — runtime flags for network-input graphs
