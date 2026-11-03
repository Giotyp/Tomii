# Python kernels

```python
import numpy as np
import tomii

@tomii.export
def normalize(v: np.ndarray) -> np.ndarray:
    return (v / np.linalg.norm(v)).astype(np.float32)
```

Wire them with `app.py_node("norm", fn=normalize, factor=n, args=[gen.out()])` and build
with `app.build(python_plugin=True)` (optionally `python_interpreter="python3.13t"` for the
free-threaded build). Use `@tomii.export(variadic=True)` for fan-in sinks. Kernels that
release the GIL (NumPy, BLAS) run in parallel across workers.
