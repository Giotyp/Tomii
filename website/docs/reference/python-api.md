---
title: Python API
sidebar_label: Python API
---

The `tomii` Python package builds graphs, compiles plugins, and runs the
Tomii binary. This page documents every public name the package exports.
For a walkthrough, see [your first graph](/docs/guide/getting-started/first-graph);
for graph-construction concepts, see [nodes and vars](/docs/guide/graphs/nodes-and-vars).

```python
import tomii as tm

app = tm.Graph()

buf_size    = app.var("buf_size", 100)
num_nodes   = app.var("num_nodes", 200)
fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])

gen_vec     = app.node("gen_vec",     func="generate_vector", factor=num_nodes, args=[buf_size])
compute_fft = app.node("compute_fft", func="compute_fft",     factor=num_nodes,
                       args=[fft_planner, gen_vec.out(0)])

app.build(func_path="src/lib.rs", plugin_manifest="Cargo.toml")
app.run(workers=4, slots=2, timing="timing.txt")
```

The snippet above is the usage example from the package docstring
(`tomii/__init__.py`).

## Exported names

| Name | Kind | Purpose |
|---|---|---|
| `Graph` | class | Top-level graph container |
| `export` | decorator | Mark a Python function as a node body |
| `procs` | decorator factory | Process-pool dispatch for pure-Python node bodies |
| `list_knobs` | function | Human-readable runtime knob list |
| `knob_space` | function | Versioned tuning search space |
| `knobs` | module | Knob-space generator and domain helpers |
| `NodeOutput`, `NodeDep` | classes | Dependency handles (returned by node methods) |
| `Loop`, `Condition`, `IndexFunc` | dataclasses | Node loop, conditional execution, network index mapping |
| `usize` … `Complex64`, `Vec`, `infer_type` | type wrappers | Explicitly typed argument values |

## Graph

`Graph` holds all variables, nodes, and configuration. Construct it with no
arguments: `app = tm.Graph()`.

### Graph.var

```python
def var(name, value=None, *, func=None, args=None, factor=None) -> Var
```

Defines an initialization variable. Exactly one of `value` or `func` must be
given; passing both or neither raises `ValueError`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique name within the graph |
| `value` | any | `None` | Literal value (type inferred, see [infer_type](#infer_type)) |
| `func` | `str` | `None` | Plugin function that computes the value at init time |
| `args` | `list` | `None` | Arguments passed to `func` |
| `factor` | `int` or `Var` | `None` | Number of instances |

The returned `Var` serializes as a `$ref` argument when passed to a node.

### Graph.node

```python
def node(name, *, func, args=None, factor=None, priority=None,
         use_workers=None, group_size=None, loop=None, loop_args=None,
         condition=None) -> Node
```

Defines a computation node.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | required | Unique name within the graph |
| `func` | `str` | required | Plugin function name |
| `args` | `list` | `None` | Node arguments: `Var`, `NodeOutput`, `NodeDep`, barrier handles, or typed values |
| `factor` | `int` or `Var` | `None` | Number of parallel instances |
| `priority` | `str` | `None` | Scheduling priority: `"high"`, `"normal"`, or `"low"` |
| `use_workers` | `str` | `None` | Worker index range, e.g. `"0-3"` (inclusive) |
| `group_size` | `int` | `None` | Instances grouped into one task |
| `loop` | `Loop` | `None` | Loop configuration |
| `loop_args` | `list` | `None` | Arguments for the loop |
| `condition` | `Condition` | `None` | Conditional execution |

### Graph.post_node

```python
def post_node(name, *, func, args=None, factor=None, priority=None,
              use_workers=None, group_size=None, loop=None, loop_args=None,
              condition=None) -> Node
```

Same parameters as `node`. Defines a post-computation node, serialized into
the `post_nodes` section of the graph JSON.

### Graph.py_node

```python
def py_node(name, *, fn, args=None, factor=None, priority=None,
            use_workers=None, group_size=None) -> Node
```

Defines a node whose body is a Python function decorated with
`@tomii.export`. Analogous to `node()` but wires to the generic Python bridge
plugin instead of a Rust/C dylib function. See
[Python plugins](/docs/guide/plugins/python).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fn` | callable or `str` | required | Decorated Python callable, or a string `"module.fn_name"` |
| `args` | `list` | `None` | Node arguments (same semantics as `node(args=...)`) |

Barrier dependencies (`predecessor.wait()`) may appear in `args`; the bridge
filters them out before calling the Python function. `py_node` raises
`ValueError` if `fn` is not registered via `@tomii.export`.

### Graph.network

```python
def network(**config) -> None
```

Sets the network receiver configuration. Keys map to the
[`network_config`](/docs/reference/json-graph-format#network_config) JSON
block: `socket_type`, `num_sockets`, `packet_length`, `frame_packets`,
`buffer_depth` (default 128), `address`, `start_port`,
`extract_packet_func`, `id_function`, and `index_function` (an `IndexFunc`
or dict; required). Values that are `Var` objects serialize as name
references. See [network sources](/docs/guide/graphs/network-sources).

### Graph.build

```python
def build(*, func_path=None, wrap_path=None, reg_path=None,
          plugin_manifest=None, release=True, clean=False, env=None,
          python_plugin=False, python_interpreter=None) -> BuildResult
```

Compiles `tomii-core` and the plugin library. Returns a `BuildResult` with
the path to the compiled `.so`.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `func_path` | `str` | `None` | Annotated Rust source; wrappers are auto-generated from `#[tomii_export]` |
| `wrap_path` | `str` | `None` | Pre-written wrapper file (legacy path) |
| `reg_path` | `str` | `None` | Pre-written registry file (legacy path) |
| `plugin_manifest` | `str` | `None` | Plugin `Cargo.toml` |
| `release` | `bool` | `True` | Build with optimizations |
| `clean` | `bool` | `False` | Clean before building |
| `env` | `dict` | `None` | Extra build environment variables |
| `python_plugin` | `bool` | `False` | Compile the bundled PyO3 bridge plugin instead |
| `python_interpreter` | `str` | `None` | Interpreter for the bridge, e.g. `"python3.13t"`; defaults to the current one |

### Graph.run

```python
def run(*, dylib=None, env=None, **kwargs) -> subprocess.CompletedProcess
```

Writes the graph JSON to a temporary file and invokes the Tomii binary. If
`dylib` is omitted and `build()` was called, the built dylib is used;
otherwise `RuntimeError` is raised. `**kwargs` are runtime knobs
(`workers=4`, `slots=2`, `timing="timing.txt"`, …) that map one-to-one to
binary flags — the full set is in the [knob catalog](/docs/reference/knob-catalog).

For Python-bridge runs, `run()` also propagates the interpreter environment
to the subprocess: it sets `TOMII_PARENT_PYTHON`, `PYTHONPATH`, `PYTHONHOME`,
and (on Linux, for binaries built without `--features embed-python`)
`LD_PRELOAD` for `libpython`.

### Graph.build_and_run

```python
def build_and_run(*, func_path=None, wrap_path=None, reg_path=None,
                  plugin_manifest=None, release=True, clean=False, env=None,
                  python_plugin=False, python_interpreter=None,
                  **run_kwargs) -> subprocess.CompletedProcess
```

Calls `build()` then `run()` in sequence.

### Graph.to_json and Graph.save_json

```python
def to_json(indent=4) -> str
def save_json(path) -> None
```

`to_json` serializes the graph to a JSON string in the
[graph format](/docs/reference/json-graph-format); `save_json` writes it to a
file.

### Graph.visualize

```python
def visualize(mode="web", *, editable=False, save_path=None, port=None) -> None
```

Opens the graph in the browser visualizer (`mode="web"`) or prints terminal
box-drawing art (`mode="ascii"`). With `editable=True` the browser view can
modify the graph; `save_path` sets where the edited graph is saved. The same
functionality is available from the command line via
[`python -m tomii --visualize`](/docs/reference/cli).

## Node handles

`Graph.node`, `Graph.post_node`, and `Graph.py_node` return `Node` objects.
A `Node` produces dependency handles that you pass as arguments to downstream
nodes. All three methods share the same signature:

```python
def out(start=0, end=None, *, group_by=None)  -> NodeOutput   # $res
def dep(start=0, end=None, *, group_by=None)  -> NodeDep      # $dep
def wait(start=0, end=None, *, group_by=None) -> NodeBarrier  # $barrier
```

| Parameter | Type | Description |
|---|---|---|
| `start` | `int`, `str`, or `list[int]` | Relative instance offset, list of offsets, or a named reference |
| `end` | `int`, `str`, or `Var` | Range end; combined with `start` as `"start-end"` |
| `group_by` | `int` | Group this many predecessor completions before firing |

`out` is a data dependency: the successor consumes the predecessor's result.
`dep` is an ordering-only dependency: the edge is tracked for scheduling but
the result value is not fetched, and the runtime skips result storage for
nodes whose only non-barrier successors are ordering-only deps. `wait` is a
barrier: the successor waits for the selected predecessor instances to
complete. See [control flow](/docs/guide/graphs/control-flow) for how these
serialize.

`NodeOutput` and `NodeDep` are exported so you can type-annotate or construct
them directly; `NodeBarrier` is returned by `wait()` but is not in the
package's public exports.

## export

```python
def export(fn=None, *, variadic=False, name=None)
```

Marks a Python function as a Tomii-callable node body — the Python analogue
of Rust's `#[tomii_export]`. The decorator returns the original function
untouched; direct Python calls are unaffected.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `variadic` | `bool` | `False` | Collect all trailing result args into a Python list passed as the last positional argument |
| `name` | `str` | `None` | Override the registry key; defaults to `"{module}.{fn_name}"` |

Raises `TomiiExportError` if the function is defined in `__main__`: the
embedded interpreter launched by the Tomii binary cannot import `__main__`,
so the function must live in an importable `.py` module.

```python
@tomii.export
def generate_vector(n: int) -> np.ndarray:
    return np.random.randn(n).astype(np.complex64)

@tomii.export(variadic=True)
def write_to_file(path: str, mats: list) -> None:
    np.savez(path, *mats)
```

## procs

```python
def procs(workers=None) -> decorator
```

Decorator factory that wraps a function with process-pool dispatch. The
decorated function submits work to a shared `ProcessPoolExecutor` and blocks
on `future.result()` with the GIL released, so multiple Tomii workers can
dispatch simultaneously on stock CPython. `workers` sets the pool size and
defaults to `os.cpu_count()`; all `@tomii.procs()`-wrapped functions share
one pool.

Use it for pure-Python loops and custom algorithms without NumPy
vectorization (per-call overhead is roughly 50–200 µs, break-even around
500 µs of compute per call). Skip it for NumPy/SciPy-heavy functions — those
already release the GIL internally. Arguments cross the process boundary via
pickle; for arrays beyond ~50 MB, use `multiprocessing.shared_memory` inside
the function instead.

```python
@tomii.export
@tomii.procs()
def heavy_pure_python(data: list) -> list:
    return [x ** 2 for x in data]
```

## Loop, Condition, IndexFunc

```python
@dataclass
class Loop:
    name: str
    factor: Union[int, Var]
```

Loop configuration for a node: `name` is the loop key in JSON, `factor` the
number of iterations.

```python
@dataclass
class Condition:
    operation: str        # e.g. "Eq", "Neq"
    value: Any
    value_type: str       # explicit Rust type string, e.g. "usize"
    func: str             # plugin function that returns the condition value
    args: List[Any] = []
```

Node-level conditional execution: the node runs only if
`func(*args) <operation> value` holds. See
[control flow](/docs/guide/graphs/control-flow).

```python
@dataclass
class IndexFunc:
    function: str
    args: List[Any] = []
```

Index-mapping function for network nodes; required by `Graph.network()` as
`index_function`.

## Type wrappers

Node and var arguments carry explicit Rust types. Plain Python values are
auto-inferred; wrappers give explicit control.

| Wrapper | Rust type | Call form |
|---|---|---|
| `i8`, `i16`, `i32`, `i64`, `i128` | signed integers | `tm.i32(-5)` |
| `u8`, `u16`, `u32`, `u64`, `u128` | unsigned integers | `tm.u8(255)` |
| `usize`, `isize` | pointer-sized integers | `tm.usize(100)` |
| `f32`, `f64` | floats | `tm.f32(1.5)` |
| `String` | `String` | `tm.String("hello")` |
| `bool_` | `bool` | `tm.bool_(True)` |
| `char_` | `char` | `tm.char_("x")` (single character; else `ValueError`) |
| `Complex32`, `Complex64` | complex numbers | `tm.Complex64(3.5, 2.5)` |
| `Vec` | `Vec<T>` | `tm.Vec("usize", [1, 2, 3])` |

### infer_type

```python
def infer_type(value) -> TypedValue
```

Converts a plain Python value using the auto-inference rules: `bool` →
`bool`, `int` → `usize`, `float` → `f64`. Any other type raises `TypeError`
and requires an explicit wrapper. The full type list on the runtime side is
in [types](/docs/guide/graphs/types).

## Agent and tuning helpers

### list_knobs

```python
def list_knobs() -> str
```

Returns a human-readable list of all `graph.run()` options. The
machine-readable form is `python -m tomii --list-knobs-json`; both are
rendered in the [knob catalog](/docs/reference/knob-catalog).

### knob_space

```python
def knob_space(graph=None, *, workload=None, include_graph_knobs=True) -> dict
```

Generates the versioned knob search space for a workload.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `graph` | `Graph`, dict, or path | `None` | Graph to extract per-graph knobs from; omit for runtime knobs only |
| `workload` | `str` | `None` | Workload label recorded in the space |
| `include_graph_knobs` | `bool` | `True` | Set `False` to restrict to runtime knobs even when a graph is given |

Returns a dict with `version`, `workload`, `knobs` (each entry carries a
`kind` of `"cli"` or `"graph"`, a `domain`, and provenance fields), and
`forbidden` — the invariants no optimizer may violate. Only knobs with role
`perf` and a defined domain enter the space; `receiver_threads` is dropped
for graphs without a `network_config`. See
[agent tuning](/docs/guide/tuning/agent-tuning).

### knobs module

`tomii.knobs` is the module behind `knob_space`. It also exposes
`enumerate_domain(domain)`, which materializes a domain dict into its
concrete search points (`bool` → `[False, True]`, `choice` → its values,
`int` with `pow2` scale → powers of two within `[min, max]`).
