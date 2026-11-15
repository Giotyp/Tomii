---
title: JSON graph format
sidebar_label: JSON graph format
---

Tomii executes graphs defined in JSON. The
[Python API](/docs/reference/python-api) generates this format for you
(`Graph.to_json`), but you can also write it by hand. The source of truth for
the format is the serde structs in `tomii-core/src/json_structs.rs`; this
page documents every field they define. `python -m tomii --schema` prints a
machine-readable schema for graph construction.

## Top-level structure

```json
{
    "initializations": [ ... ],
    "nodes": [ ... ],
    "post_nodes": [ ... ],
    "network_config": { ... }
}
```

| Field | Required | Description |
|---|---|---|
| `initializations` | no (defaults to empty) | Objects computed once before execution |
| `nodes` | yes | Computation nodes |
| `post_nodes` | no | Nodes run in the post-computation phase |
| `network_config` | no | Network receiver configuration |

## Initializations

Each entry in `initializations` defines a named object that exists before any
node runs. Nodes reference these objects by name with `$ref` arguments, and
`factor` fields can reference them as counts.

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Unique name |
| `factor` | no | [factor](#factor) | Number of instances |
| `args` | yes | array | Typed values: `{"type": ..., "value": ...}` (both fields required) |
| `function` | no | string | Plugin function that computes the object from `args` |

Without `function`, the single arg is parsed as a literal typed value.
With `function`, the plugin function is called at init time with `args`:

```json
{
    "name": "buf_size",
    "args": [ { "type": "usize", "value": "100" } ]
},
{
    "name": "fft_planner",
    "function": "fft_planner",
    "args": [ { "type": "$ref", "value": "buf_size" } ]
}
```

(from `examples/matrix-compute/graph.json`)

Three names are reserved and resolved at runtime rather than defined by you:
`$index` (the instance index of the consuming node), `$workers` (the
worker count), and `$frame_id` (the id of the frame the consuming task
belongs to — the same id network admission and frame slots use; for
non-network graphs, a monotonically increasing counter starting at 0, one
per admitted frame). Use them as `$ref` values, e.g.
`{"type": "$ref", "value": "$index"}` or
`{"type": "$ref", "value": "$frame_id"}`. Resolving `$frame_id` costs
nothing for graphs that never reference it: the decision is made once at
graph-compile time, not per task.

## Nodes

Each entry in `nodes` (and `post_nodes`) defines a task.

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Unique name |
| `function` | yes | string | Plugin function to execute |
| `args` | yes | array | [Arguments](#arguments) passed to the function |
| `factor` | no | [factor](#factor) | Number of parallel instances |
| `loop` | no | object | `{"name": string, "factor": factor?}` — loop configuration |
| `loop_args` | no | array | Arguments for the loop |
| `group_size` | no | [factor](#factor) | Instances grouped into one task |
| `condition` | no | object | [Node-level condition](#conditions) |
| `priority` | no | string | `"high"`, `"normal"` (default), or `"low"` |
| `use_workers` | no | string | Worker index range `"start-end"`, inclusive on both ends (`"0-7"` = workers 0 through 7) |

A node with `factor` creates that many parallel instances of the task, each
with its own instance index. See
[nodes and vars](/docs/guide/graphs/nodes-and-vars).

## Arguments

Each element of `args` is an object with a `type` and, depending on the
type, a `value` or a `predecessor`:

| Field | Required | Description |
|---|---|---|
| `type` | yes | Type string (see below) |
| `value` | no | Literal value or referenced name |
| `predecessor` | no | [Predecessor object](#predecessor-objects) for dependency types |
| `condition` | no | Arg-level guard: `{"operation", "value", "value_type"}` |

### Argument types

| `type` | Uses | Meaning |
|---|---|---|
| `$ref` | `value` | Reference to an initialization object |
| `$res` | `predecessor` | Data dependency: consume a predecessor's result |
| `$dep` | `predecessor` | Ordering-only dependency: wait, but do not fetch the result |
| `$barrier` | `predecessor` | Wait for the selected predecessor instances to complete |
| literal type | `value` | Constant parsed from the string, e.g. `{"type": "usize", "value": "100"}` |

Literal types accepted: `bool`, `char`, `i8`–`i128`, `u8`–`u128`, `usize`,
`isize`, `f32`, `f64`, `String`, `Complex32`, `Complex64`, and `Vec<T>`
where `T` is any of the scalar types (the value is a comma-separated list).
Complex values accept `"3.5+2.5i"`, `"3.5,2.5"`, or
`"{\"re\":3.5,\"im\":2.5}"`. The full runtime type system is described in
[types](/docs/guide/graphs/types).

Network input is consumed by naming the virtual node `$network` as a
predecessor, not by a distinct argument type:

```json
{
    "type": "$res",
    "predecessor": { "name": "$network", "indexes": "0" }
}
```

(from the MIMO benchmark graph generated by `bench/mimo-bench/tomii/build_graph.py`;
the `$network` node exists when `network_config` is present)

## Predecessor objects

| Field | Required | Type | Description |
|---|---|---|---|
| `name` | yes | string | Predecessor node name (or `$network`) |
| `indexes` | one of `indexes`/`instance_map` | string | Instance selection (see below) |
| `group_by` | no | [factor](#factor) | Group this many predecessor completions before firing one successor |
| `instance_map` | one of `indexes`/`instance_map` | array of arrays of integers | Explicit per-consumer-instance dependency map (see below) |

`indexes` selects which predecessor instances the consuming instance depends
on, as relative instance offsets: `0` means the same index, `-1` the
previous. Three syntaxes are accepted:

| Syntax | Example | Meaning |
|---|---|---|
| single | `"0"` | One offset; also accepts an init name that resolves to a number |
| comma list | `"0,1,2"` | Exact list of offsets |
| range | `"0-5"`, `"0-num_nodes"` | Contiguous range of offsets |

In a range, each endpoint may be a number or an init name. A numeric end is
inclusive: `"0-5"` selects offsets 0 through 5. A named end resolves to the
variable's value and is exclusive: `"0-num_nodes"` with `num_nodes = 200`
selects offsets 0 through 199. This matters for barriers that fan in over
all instances of a `factor` node:

```json
{
    "name": "write_res",
    "function": "write_to_file",
    "args": [
        { "type": "$ref", "value": "result_file" },
        {
            "type": "$res",
            "predecessor": { "name": "mat_mul", "indexes": "0-num_nodes" }
        }
    ]
}
```

(from `examples/matrix-compute/graph.json`)

With `group_by`, a barrier over N predecessor instances fires the successor
once per group of `group_by` completions instead of once for all N. See
[control flow](/docs/guide/graphs/control-flow).

### `instance_map`: explicit per-instance dependencies

`indexes`/`group_by` express uniform shapes — every consumer instance reads
the same relative offset or the same fan-in. `instance_map` instead declares,
for EACH consumer instance, the exact (absolute, not relative) set of
producer instances it depends on:

```json
{
    "type": "$barrier",
    "predecessor": {
        "name": "leaves",
        "instance_map": [[0, 1, 2], [3, 4], [5, 6, 7, 8]]
    }
}
```

Here the consumer has factor 3: instance 0 waits for producer instances
`0, 1, 2`; instance 1 for `3, 4`; instance 2 for `5, 6, 7, 8` — an irregular
reduction tree where branches join different numbers of leaves. A
neighbourhood (halo) stencil is the same idea with overlapping, wrapping
lists: `tile[j]` depending on `tile[j-1], tile[j], tile[j+1]` (mod `n`).

Rules, enforced at load time:

- `instance_map` is mutually exclusive with `indexes` and `group_by` on the
  same predecessor object.
- `instance_map` is only valid on `$barrier`/`$dep` arguments, never `$res`
  — a mapped edge never delivers a value in this release.
- The array's length must equal the consumer node's `factor`; each inner
  array lists absolute producer indexes (not relative offsets), each `< `
  the producer's `factor`, with no duplicates within one entry.
- An empty entry (`[]`) means that argument does not gate that consumer
  instance. If NO argument (mapped or coexisting) contributes to a consumer
  instance, its total dependency count is 0 — that instance is dispatched
  exactly once per frame at frame admission, like a root/initial node,
  instead of through the dependency-counter path (nothing would otherwise
  ever decrement it). This can apply to some instances of a node (mixed with
  ordinary nonzero-threshold instances) or to every instance.
- A node with `group_size` set cannot have an `instance_map` predecessor.
- A node with an `instance_map` predecessor may still have an ordinary
  (non-mapped) argument on the SAME predecessor, or on a different one, but
  only in a shape with a uniform or exactly positional per-instance
  contribution: a `$barrier`/`$dep` (optionally filtered to a range), a
  factor-1 producer (broadcasts to every instance), or an exact 1:1
  same-factor single-offset/contiguous-range edge. When the plain argument
  shares the SAME predecessor as a mapped one, its per-instance index set is
  unioned directly into that predecessor's mapped forward map (a producer
  instance reachable through both is still counted, and decremented, exactly
  once) — this is how, for example, an irregular fusion-tree node can
  `$barrier`-wait an explicit, irregular subset of a predecessor's instances
  AND ALSO read a VALUE from it through an ordinary 1:1 `$res` edge on that
  same predecessor. `group_by` on the plain argument, or any other shape (no
  uniform/positional interpretation — the "ordinal fallback"), is rejected
  with a load-time error naming the offending predecessor, whether it
  coexists on the same or a different producer.

The [Python builder guide](/docs/guide/graphs/nodes-and-vars#explicit-per-instance-dependency-maps)
covers the `map=` parameter and the `tomii.halo`/`tomii.stride` helpers that
generate common `instance_map` shapes.

## Factor

A factor is either a literal integer or a string naming an initialization
object that holds a number:

```json
"factor": 200
"factor": "num_nodes"
```

The reserved name `"$workers"` resolves to the worker count. Factors appear
on nodes, initializations, loops, `group_size`, `group_by`, and several
`network_config` fields.

## Conditions

A node-level `condition` gates execution: the node's `function` runs only if
the condition holds, evaluated by calling a plugin function.

| Field | Required | Description |
|---|---|---|
| `operation` | yes | Comparison, e.g. `"Eq"`, `"Neq"` |
| `value` | yes | Comparison value as a string |
| `value_type` | yes | Rust type of `value`, e.g. `"bool"` |
| `function` | yes | Plugin function returning the value to compare |
| `args` | yes | Arguments passed to `function` |

```json
"condition": {
    "operation": "Eq",
    "value": "false",
    "value_type": "bool",
    "function": "is_pilot",
    "args": [
        { "type": "$res", "predecessor": { "name": "$network", "indexes": "0" } },
        { "type": "$ref", "value": "framestats" },
        { "type": "$ref", "value": "$index" }
    ]
}
```

(from the MIMO benchmark graph generated by `bench/mimo-bench/tomii/build_graph.py`)

The arg-level `condition` (inside an argument object) is a guard with only
`operation`, `value`, and `value_type` — the argument is used only if the
guard holds.

## network_config

Present only for graphs that ingest packets from the network. Dedicated
receiver threads (`--receiver-threads`) own the sockets and inject data as
the virtual `$network` node's results. See
[network sources](/docs/guide/graphs/network-sources).

| Field | Required | Type | Description |
|---|---|---|---|
| `socket_type` | yes | string | Only `"udp"` is currently supported |
| `num_sockets` | yes | [factor](#factor) | Number of receive sockets |
| `packet_length` | yes | [factor](#factor) | Packet size in bytes |
| `frame_packets` | yes | [factor](#factor) | Packets per frame (legacy spelling `stream_packets` still accepted) |
| `buffer_depth` | no (default 128) | integer | Receive buffer depth |
| `address` | yes | string | Bind address, literal or the name of a `String` init object |
| `start_port` | yes | [factor](#factor) | First port; socket *i* binds `start_port + i` |
| `extract_packet_func` | yes | string | Plugin function that extracts payload from a packet |
| `id_function` | yes | string | Plugin function that maps a packet to its frame id |
| `index_function` | yes | object | `{"function": string, "args": [...]}` — maps a packet to a `$network` instance index |

```json
"network_config": {
    "socket_type": "udp",
    "num_sockets": "antennas",
    "packet_length": "packet_length",
    "frame_packets": "packets_per_frame",
    "buffer_depth": 2000,
    "address": "server_address",
    "start_port": "base_port",
    "extract_packet_func": "process_packet",
    "id_function": "get_frame_id"
}
```

(from the MIMO benchmark graph generated by `bench/mimo-bench/tomii/build_graph.py`;
`index_function` is required by
the current `json_structs.rs` and must be added to graphs written against
older versions)

## Complete example

`examples/matrix-compute/graph.json`, trimmed to two nodes:

```json
{
    "initializations": [
        { "name": "buf_size",  "args": [ { "type": "usize", "value": "100" } ] },
        { "name": "num_nodes", "args": [ { "type": "usize", "value": "200" } ] },
        {
            "name": "fft_planner",
            "function": "fft_planner",
            "args": [ { "type": "$ref", "value": "buf_size" } ]
        }
    ],
    "nodes": [
        {
            "name": "gen_vec",
            "factor": "num_nodes",
            "function": "generate_vector",
            "args": [ { "type": "$ref", "value": "buf_size" } ]
        },
        {
            "name": "compute_fft",
            "factor": "num_nodes",
            "function": "compute_fft",
            "args": [
                { "type": "$ref", "value": "fft_planner" },
                {
                    "type": "$res",
                    "predecessor": { "name": "gen_vec", "indexes": "0" }
                }
            ]
        }
    ]
}
```

Each of the 200 `compute_fft` instances depends on the `gen_vec` instance at
offset 0 from its own index — a 1:1 edge. More examples are collected in
[examples](/docs/guide/examples).
