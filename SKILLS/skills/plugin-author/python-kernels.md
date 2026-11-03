# Python kernels

```python
import numpy as np
import tomii

@tomii.export
def compute_fft(v: np.ndarray) -> np.ndarray:
    return np.fft.fft(v).astype(np.complex64)
```

Wire them with `app.py_node("fft", fn=compute_fft, factor=n, args=[gen.out()])` and build
with `app.build(python_plugin=True)` (optionally `python_interpreter="python3.13t"` for the
free-threaded build). Use `@tomii.export(variadic=True)` for fan-in sinks. Kernels that
release the GIL (NumPy, BLAS) run in parallel across workers. See
`examples/matrix-compute-python/`.
