---
title: Your first graph
sidebar_label: First graph
---

This page walks through `examples/matrix-compute`, the canonical starter
example: an FFT + matrix-multiply pipeline with a Rust plugin. The topology
is a linear chain with one fan-out and one fan-in:

```
gen_vec(buf_size) ─────────────────────────────────► vec_to_mat
                  └─► fft_planner ─► compute_fft ──► vec_to_mat
                                                      └─► mat_mul ─► write_res
```

## The builder code

This is the graph definition from
`examples/matrix-compute/run_bench.py`, trimmed to the essentials:

```python
import tomii as tm

app = tm.Graph()

# Initializations — computed once, before any frame runs
buf_size    = app.var("buf_size", 100)
num_nodes   = app.var("num_nodes", 200)
fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])
result_file = app.var("result_file", func="get_out_file",
                      args=[tm.String("SCRIPT_DIR"), tm.String("result.txt")])

# Pipeline — factor=num_nodes creates 200 parallel instances of each node
gen_vec     = app.node("gen_vec",     func="generate_vector",
                       factor=num_nodes, args=[buf_size])
compute_fft = app.node("compute_fft", func="compute_fft",
                       factor=num_nodes, args=[fft_planner, gen_vec.out()])
vec_mat     = app.node("vec_mat",     func="vec_to_mat",
                       factor=num_nodes, args=[gen_vec.out(), compute_fft.wait()])
mat_mul     = app.node("mat_mul",     func="mat_mul",
                       factor=num_nodes, args=[vec_mat.out(), vec_mat.out()])
app.node("write_res", func="write_to_file",
         args=[result_file, mat_mul.out(end=num_nodes)])

app.build(func_path="examples/matrix-compute/src/lib.rs",
          plugin_manifest="examples/matrix-compute/Cargo.toml")
app.run(workers=4, slots=2, timing="timing.csv")
```

Line by line:

- `app.var("buf_size", 100)` defines a constant. `app.var("fft_planner",
  func=..., args=...)` defines a computed initialization: the plugin function
  `fft_planner` runs once at startup and its result is shared by every
  `compute_fft` instance.
- `func="generate_vector"` names a plugin function. The graph never contains
  kernel code — only function names. The kernels live in
  `examples/matrix-compute/src/lib.rs`, annotated with `#[tomii_export]`
  (see [Rust plugins](/docs/guide/plugins/rust)).
- `factor=num_nodes` creates 200 instances of each node per frame. Instance
  `i` of `compute_fft` depends on instance `i` of `gen_vec`.
- `gen_vec.out()` is a data dependency: instance `i` receives the result of
  `gen_vec[i]`. `compute_fft.wait()` is a barrier: `vec_mat[i]` waits for
  `compute_fft[i]` to finish but does not consume its result. The barrier is
  needed because `compute_fft` mutates the vector in place.
- `mat_mul.out(end=num_nodes)` is a fan-in: `write_res` receives all 200
  `mat_mul` results as one variadic argument.

## The JSON it emits

`app.to_json()` produces the same file as
`examples/matrix-compute/graph.json`. One node, as emitted:

```json
{
    "name": "vec_mat",
    "factor": "num_nodes",
    "function": "vec_to_mat",
    "args": [
        { "type": "$res",     "predecessor": { "name": "gen_vec",     "indexes": "0" } },
        { "type": "$barrier", "predecessor": { "name": "compute_fft", "indexes": "0" } }
    ]
}
```

`.out()` became `$res`, `.wait()` became `$barrier`, and the `Var` reference
became a `$ref`. The full mapping is in the
[JSON graph format reference](/docs/reference/json-graph-format).

## Build and run

From the repository root:

```bash
python examples/matrix-compute/run_bench.py --workers 4 --slots 8 --max-frames 100
```

The first run compiles `libmatcomp.so` and regenerates the function registry;
subsequent runs with `--no-clean` skip the rebuild
(`examples/matrix-compute/README.md`). `build()` compiles the plugin from the
annotated Rust source; `run()` writes the graph JSON to a temp file and
launches the runtime binary.

## Verify the output

```bash
bash examples/matrix-compute/verify.sh
```

This builds the `perfval` binary, cross-checks the matrix output against a
NumPy reference, and prints `PASS` on success. Per-element error statistics
go to `validation.txt` (`examples/matrix-compute/README.md`).

## Next

- [Running graphs](/docs/guide/getting-started/running) — what `workers`,
  `slots`, and the other `run()` options mean.
- [Nodes and variables](/docs/guide/graphs/nodes-and-vars) — the builder API
  in full.
- [Rust plugins](/docs/guide/plugins/rust) — how the kernel side works.
