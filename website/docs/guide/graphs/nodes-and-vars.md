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
