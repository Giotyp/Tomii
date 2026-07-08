---
title: Control flow
sidebar_label: Control flow
---

Beyond plain data dependencies, nodes take conditions, loops, priorities,
grouped barriers, and ordering-only edges. This page uses
`examples/stream-analytics` as the running example — a sensor pipeline that
exercises all of them without external dependencies:

```
generate (factor=32) ─► classify ─► handle_anomaly  (priority: high)
                                 └─► smooth          (priority: low)
                     ─► compute_stats (factor=4, grouped barrier)
                        └─► aggregate ─► report (variadic fan-in)
log_event ($dep on all classify tasks)
```

## Conditions

A `Condition` makes a node instance execute only when a plugin function's
result satisfies a comparison. From `examples/stream-analytics/run_bench.py`:

```python
cond_anomaly = tm.Condition(
    operation="Eq",
    value=True,
    value_type="bool",
    func="check_bool",
    args=[classify.out()],     # $res(classify, i) — 1:1 instance mapping
)
app.node("handle_anomaly", func="amplify_reading",
         factor=total_readings, priority="high", condition=cond_anomaly,
         args=[generate.out(), classify.wait()])
```

At runtime, instance `i` calls `check_bool(classify[i])` and runs only when
the result equals `true`. The `smooth` node uses the same function with
`operation="Neq"`, so exactly one branch fires per reading. Supported
operation strings include `"Eq"` and `"Neq"` (`tomii/_loop.py`).

## Priorities

`priority="high"` / `priority="low"` on a node biases the scheduler.
stream-analytics marks the anomaly branch high and the smoothing branch low,
so anomaly handling is dispatched ahead of routine work when both are ready.

## Barriers and grouped barriers

`n.wait(i)` is a barrier: wait for instance `i`, receive no value. A range
barrier with `group_by` splits the wait into independent groups:

```python
compute_stats = app.node("compute_stats", func="compute_sensor_stats",
    factor=num_sensors,
    args=[generate.wait(0, total_readings, group_by=readings_per_sensor)])
```

With 32 readings and `group_by=8`, `compute_stats[i]` fires when
`generate[i*8 .. (i+1)*8]` complete — four independent groups instead of one
global barrier. In the JSON this emits a `$barrier` arg whose `predecessor`
carries a `group_by` factor.

## Ordering-only edges: `$dep`

`n.dep(...)` orders execution without moving data:

```python
app.node("log_event", func="log_stream_event",
         args=[classify.dep(0, total_readings)])
```

`log_event` fires once after all `classify` instances complete, but no
classify result is fetched — the arg slot receives `None`, and the runtime
skips result storage for producers whose only non-barrier successors are
`$dep` edges (`tomii/_node.py`).

## Loops

A `Loop` re-executes a node's function for a fixed number of iterations
(from the repository `README.md`):

```python
loop_node = app.node("proc", func="process", factor=200,
                     loop=tm.Loop("iter", factor=loop_factor))
```

`Loop(name, factor)` takes an `int` or a `Var` as the iteration count.
Per-iteration arguments go in `loop_args`.

## Index functions

`IndexFunc(function, args)` maps an incoming network packet to the node
instance index that consumes it. It belongs to network configuration, not to
ordinary nodes — see [Network sources](/docs/guide/graphs/network-sources).

## Factor expressions

Any `factor` — on nodes, variables, loops, or `group_by` — is either a
literal integer or the name of an integer-valued variable; `"$workers"`
resolves to the worker count (`tomii-core/src/json_structs.rs`). This is what
makes instance counts tunable without touching code — see
[Runtime knobs](/docs/guide/tuning/knobs).

## Verifying control flow

Run the example and its checker:

```bash
python examples/stream-analytics/run_bench.py
python examples/stream-analytics/verify.py   # prints PASS
```

With the default `anomaly_threshold = 5.0` the anomaly branch fires; raise it
to `10.0` to exercise the smoothing branch instead
(`examples/stream-analytics/README.md`).
