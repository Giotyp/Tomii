---
title: Nodes and variables
sidebar_label: Nodes and vars
---

A `tomii.Graph` holds two kinds of entries: variables (initializations,
computed once) and nodes (tasks, executed once per frame).

## Variables

```python
buf_size    = app.var("buf_size", 100)                          # constant
fft_planner = app.var("fft_planner", func="fft_planner",        # computed
                      args=[buf_size])
```

A constant variable stores a typed value. A computed variable names a plugin
function; the runtime calls it once at startup and stores the result. Both
serialize into the `initializations` array of the graph JSON. Passing a `Var`
as a node argument emits a `$ref` — a reference to the initialized object,
shared by every node instance that uses it.

Variables also accept `factor=` to create an array of initialized objects.

## Nodes

```python
n = app.node("compute_fft", func="compute_fft", factor=num_nodes,
             args=[fft_planner, gen_vec.out()])
```

The full signature (from `tomii/_graph.py`): `name`, `func`, `args`,
`factor`, `priority`, `use_workers`, `group_size`, `loop`, `loop_args`,
`condition`. `priority`, `loop`, and `condition` are covered in
[Control flow](/docs/guide/graphs/control-flow).

`factor` sets the number of parallel instances of the node per frame. It
takes an `int` or a `Var`, so instance counts can be graph parameters. Where
a factor is accepted, the string `"$workers"` resolves to the worker count at
runtime (`tomii-core/src/json_structs.rs`).

### Built-in references

Three names are reserved and resolved at runtime instead of being defined
with `app.var()`. Pass them as a node argument via `TypedValue('$ref', name)`
(`tomii._types`):

| Name | Resolves to |
|---|---|
| `$index` | The instance index of the consuming node |
| `$workers` | The configured worker count (also usable as a `factor=` string) |
| `$frame_id` | The id of the frame the consuming task belongs to — the same id network admission and frame slots use; for non-network graphs, a monotonically increasing counter starting at 0, one per admitted frame |

```python
from tomii._types import TypedValue

fid = TypedValue('$ref', '$frame_id')
app.node('tagged', func='tag_with_frame', factor=1, args=[fid])
```

Resolving any of these costs nothing for a node that doesn't reference
it — the runtime decides which (if any) apply once per node at
graph-compile time, not once per task.

## Dependencies between nodes

A node handle exposes three dependency constructors
(`tomii/_node.py`):

| Call | JSON arg type | Semantics |
|---|---|---|
| `n.out(i)` | `$res` | Data dependency: the argument receives the result of instance `i` |
| `n.wait(i)` | `$barrier` | Ordering: wait for instance `i`, no value passed |
| `n.dep(i)` | `$dep` | Ordering-only edge; the arg slot receives `None` and the runtime skips result storage |

All three take `(start=0, end=None, *, group_by=None)`:

- `n.out()` — instance `i` of the consumer depends on instance `i` of the
  producer (1:1 mapping).
- `n.out(0, num_nodes)` — a range: the consumer receives all instances
  `0..num_nodes` (fan-in). The consuming plugin function must be variadic.
- `group_by=k` splits a range dependency into groups of `k`; see
  [Control flow](/docs/guide/graphs/control-flow#barriers-and-grouped-barriers).

In the emitted JSON, a dependency becomes a `predecessor` object:

```json
{ "type": "$res", "predecessor": { "name": "gen_vec", "indexes": "0" } }
```

Literal argument values (`100`, `tm.String("x")`) serialize as typed values
instead — see [Types](/docs/guide/graphs/types). The complete JSON grammar is
in the [JSON graph format reference](/docs/reference/json-graph-format).

## Explicit per-instance dependency maps

`n.wait(i)` / `n.dep(i)` express uniform shapes: a single offset, a full
fan-in, or a `group_by` partition. Some graphs need an irregular shape instead
— a reduction tree where branches join different numbers of leaves, or a
neighbourhood (halo) stencil where instance `j` depends on a handful of
specific, non-contiguous neighbours. `map=` declares that explicitly:

```python
# instance j of `fuse` depends on EXACTLY these producer instances —
# an irregular reduction tree where leaf counts per branch differ.
fuse = app.node("fuse", func="combine", factor=3, args=[
    leaves.wait(map=[[0, 1, 2], [3, 4], [5, 6, 7, 8]]),
])
```

`map` is either a list of lists (`map[j]` = the absolute producer instances
consumer instance `j` depends on) or a callable `j -> Iterable[int]`,
resolved once the consumer's factor is known. Two helpers cover the common
shapes:

```python
import tomii as tm

# neighbourhood (halo) stencil: tile j depends on tiles j-1, j, j+1 (wrapping)
halo_step = app.node("halo_step", func="smooth", factor=n, args=[
    tile.wait(map=tm.halo(radius=1, wrap=True)),
])

# strided reduction: coarse[j] depends on fine[j * k] only
coarse = app.node("coarse", func="downsample", factor=n // k, args=[
    fine.wait(map=tm.stride(k)),
])
```

Rules:

- `map` is only valid on `.wait()` / `.dep()` — a mapped edge never delivers a
  value (`.out(map=...)` raises `ValueError`); read values through an
  ordinary `.out()` edge on a different, uniformly-shaped predecessor.
- `map` is mutually exclusive with `group_by`.
- A callable `map` needs both the consumer's and the producer's `factor` to
  be literal ints at build time; if either is a `Var` that cannot be resolved
  yet, pass an explicit list of lists instead.
- `group_size` is not supported on a node with a mapped predecessor.
- `map[j]` may be empty. If instance `j` ends up with zero total
  dependencies (no mapped or coexisting predecessor contributing), it is
  dispatched exactly once per frame at frame admission, like a root/initial
  node — not rejected, and not left waiting on a decrement that would never
  arrive.

See the [JSON graph format reference](/docs/reference/json-graph-format) for
the full `instance_map` grammar and validation rules.

## Post nodes

```python
app.post_node("cleanup", func="cleanup_state", args=[])
```

A post node runs after the frame's compute nodes complete, before the slot
is released. The stream-analytics example uses one for per-frame state
cleanup (`examples/stream-analytics/run_bench.py`). Post nodes take the same
options as `node()` and serialize into a separate `post_nodes` array.

## Name rules

Every variable and node name must be unique within the graph; `Graph` raises
`ValueError` on a duplicate (`tomii/_graph.py`).

## Next

- [Types](/docs/guide/graphs/types) — how literal values are typed.
- [Control flow](/docs/guide/graphs/control-flow) — conditions, loops,
  priorities, grouped barriers.
