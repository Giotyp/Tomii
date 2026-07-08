---
title: Argument types
sidebar_label: Types
---

Kernel functions have typed Rust signatures. When you pass a literal value as
a node argument, the builder must know which Rust type to construct. Plain
Python values are auto-inferred; wrapper functions give explicit control.

## Auto-inference

From `tomii/_types.py`:

| Python value | Rust type |
|---|---|
| `int` | `usize` |
| `float` | `f64` |
| `bool` | `bool` |

Any other unwrapped Python value raises `TypeError`. Strings must always be
wrapped (`tm.String(...)`), because a bare string in an argument position
would be ambiguous with a variable reference.

## Explicit wrappers

| Wrapper | Rust type |
|---|---|
| `tm.i8` `tm.i16` `tm.i32` `tm.i64` `tm.i128` | signed integers |
| `tm.u8` `tm.u16` `tm.u32` `tm.u64` `tm.u128` | unsigned integers |
| `tm.usize` `tm.isize` | pointer-sized integers |
| `tm.f32` `tm.f64` | floats |
| `tm.String(s)` | `String` |
| `tm.bool_(b)` | `bool` |
| `tm.char_(c)` | `char` (exactly one character) |
| `tm.Complex32(re, im)` / `tm.Complex64(re, im)` | complex numbers |
| `tm.Vec(elem_type, values)` | `Vec<T>` |
| `tm.infer_type(v)` | applies the auto-inference rules explicitly |

## Examples

From `examples/stream-analytics/run_bench.py` — a threshold that must be
`f64`, not the default `usize` an `int` would infer to:

```python
anomaly_threshold = app.var("anomaly_threshold", tm.f64(5.0))
```

From `examples/matrix-compute/run_bench.py` — string arguments:

```python
result_file = app.var("result_file", func="get_out_file",
                      args=[tm.String("SCRIPT_DIR"), tm.String("result.txt")])
```

A vector literal:

```python
weights = app.var("weights", tm.Vec("f32", [1.0, 2.0, 3.0]))
```

## What this becomes in JSON

A wrapped value serializes as a `type`/`value` pair:

```json
{ "type": "f64", "value": "5.0" }
```

The type string must match the parameter type of the plugin function that
receives it. Type mismatches surface as errors when the runtime parses the
graph and resolves arguments, not at Python build time. The full set of
argument forms is in the
[JSON graph format reference](/docs/reference/json-graph-format).
