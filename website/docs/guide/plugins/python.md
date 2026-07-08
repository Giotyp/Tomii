---
title: Python plugins
sidebar_label: Python
---

Kernels can be plain Python functions. You decorate them with
`@tomii.export`, reference them with `Graph.py_node()`, and build the bundled
PyO3 bridge plugin instead of a custom dylib. The bridge embeds a Python
interpreter inside the runtime and calls your functions from worker threads.

## Exporting functions

From `examples/matrix-compute-python/matcomp.py`:

```python
import numpy as np
import tomii

@tomii.export
def generate_vector(n: int) -> np.ndarray:
    return (np.random.randn(n) + 1j * np.random.randn(n)).astype(np.complex64)

@tomii.export
def compute_fft(v: np.ndarray) -> np.ndarray:
    return np.fft.fft(v).astype(np.complex64)

@tomii.export(variadic=True)
def write_to_file(path: str, mats: list) -> None:
    np.savez(path, *[np.asarray(m) for m in mats])
```

The decorator is a zero-cost marker: it registers the function and returns it
untouched, so calling it directly from Python is unaffected
(`tomii/_export.py`). `variadic=True` marks fan-in sinks that receive a list
of predecessor results.

## Wiring and building

`py_node` is the Python-kernel analogue of `node()` — you pass the function
object instead of a name string:

```python
import matcomp

gen_vec = app.py_node("gen_vec", fn=matcomp.generate_vector,
                      factor=num_nodes, args=[buf_size])
fft = app.py_node("fft", fn=matcomp.compute_fft,
                  factor=num_nodes, args=[gen_vec.out()])

app.build(python_plugin=True)
app.run(workers=4, slots=2)
```

`build(python_plugin=True)` compiles the bundled bridge plugin. `run()`
propagates your interpreter's `sys.path` to the embedded interpreter, so the
bridge sees the same packages as the building process (`tomii/_graph.py`).

Run the full example with:

```bash
bash examples/matrix-compute-python/run_bench.sh
```

## The GIL

Whether Python kernels run in parallel depends on what they do
(`examples/matrix-compute-python/README.md`):

| Python build | NumPy/BLAS kernels | Pure-Python kernels |
|---|---|---|
| CPython 3.12 (stock) | parallel (GIL released inside BLAS/FFT) | serialized by the GIL |
| CPython 3.13t (free-threaded) | parallel | parallel |

NumPy releases the GIL around BLAS and FFT calls, so the matrix-compute
kernels already run in parallel on stock CPython.

## Pure-Python compute: `@tomii.procs()`

For genuine pure-Python work (loops, comprehensions) that would serialize
under the GIL, stack `@tomii.procs()` under the export decorator:

```python
@tomii.export
@tomii.procs()   # dispatches to a ProcessPoolExecutor; GIL released during wait
def pure_python_example(data: list) -> list:
    return [x * x for x in data]
```

Each worker releases the GIL while waiting for its subprocess result, so N
workers execute concurrently in N separate processes. The dispatch overhead
is roughly 50–200 µs per call, so it only pays when compute dominates
(`examples/matrix-compute-python/matcomp.py`).

## Free-threaded Python

For 3.13t, which removes the GIL entirely, pass the interpreter at build
time:

```python
app.build(python_plugin=True, python_interpreter="python3.13t")
```

or from the example runner:

```bash
bash examples/matrix-compute-python/run_bench.sh --python-interpreter python3.13t
```
