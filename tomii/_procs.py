"""Process-pool GIL bypass — Ray-style parallelism on stock Python 3.12+.

tomii.procs(fn) wraps a @tomii.export function so it executes in a
subprocess from a shared ProcessPoolExecutor.  The key property:

    Python's future.result() releases the GIL while blocked.

When multiple Tomii Rayon workers call py_call_any_cm simultaneously, each
acquires the GIL briefly to submit the task, then blocks on future.result()
with the GIL released.  The other workers can then submit in turn.  The
subprocess workers execute in parallel (separate processes = separate GILs).

This matches Ray's architecture: worker processes + non-blocking dispatch.
The differences from full Ray are:
  - No Plasma object store: data is passed via pickle (adequate for arrays
    up to ~10 MB; beyond that use shared_memory explicitly).
  - No distributed scheduler: pool is in-process, on the local machine.
  - No fault tolerance: a crashed worker raises an exception in the caller.

Usage::

    import tomii, numpy as np

    @tomii.export
    @tomii.procs()          # wraps with process-pool dispatch
    def heavy_pure_python(data: list) -> list:
        return [x ** 2 for x in data]   # pure Python, benefits from procs()

    # NumPy-heavy functions DON'T need tomii.procs() because NumPy already
    # releases the GIL internally (BLAS/FFTW use Py_BEGIN_ALLOW_THREADS):
    @tomii.export
    def mat_mul(a, b):      # GIL already released by numpy during @
        return a @ b

When to use tomii.procs()
--------------------------
  Use it     — pure-Python loops, comprehensions, custom algorithms without
               NumPy vectorisation. Per-call overhead ~50–200 µs.
  Skip it    — NumPy matmul, FFT, SciPy operations. These already release
               the GIL; adding procs() would add overhead with no benefit.
  Threshold  — break-even at roughly 500 µs per call (compute > overhead).
               Smaller tasks: use python3.13t instead.
"""

from __future__ import annotations

import atexit
import os
from concurrent.futures import ProcessPoolExecutor
from functools import wraps
from typing import Any, Callable, Optional


# Single shared executor for the process — created lazily on first use.
_executor: Optional[ProcessPoolExecutor] = None
_executor_workers: int = 0


def _get_executor(workers: Optional[int] = None) -> ProcessPoolExecutor:
    global _executor, _executor_workers
    n = workers or os.cpu_count() or 4
    if _executor is None or _executor_workers != n:
        if _executor is not None:
            _executor.shutdown(wait=False)
        _executor = ProcessPoolExecutor(max_workers=n)
        _executor_workers = n
    return _executor


@atexit.register
def _shutdown_executor() -> None:
    global _executor
    if _executor is not None:
        _executor.shutdown(wait=False)
        _executor = None


def _call_fn(fn: Callable, args: tuple, kwargs: dict) -> Any:
    """Top-level function executed inside the worker process."""
    return fn(*args, **kwargs)


def procs(workers: Optional[int] = None):
    """Decorator factory that wraps a function with process-pool dispatch.

    The decorated function submits work to a ``ProcessPoolExecutor`` and
    waits for the result.  The GIL is released during the wait, so multiple
    Tomii Rayon workers can dispatch simultaneously even on stock CPython.

    Parameters
    ----------
    workers:
        Size of the process pool.  Defaults to ``os.cpu_count()``.
        All @tomii.procs()-wrapped functions share a single pool.

    Notes
    -----
    The function and its arguments are pickled for IPC.  numpy arrays
    serialize efficiently (~1 µs/MB via pickle protocol 5 out-of-band buffers).
    For very large arrays (> ~50 MB), consider ``multiprocessing.shared_memory``
    inside your function body instead.
    """
    def decorator(fn: Callable) -> Callable:
        executor = _get_executor(workers)

        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Submit to worker process — GIL is released during future.result()
            future = executor.submit(_call_fn, fn, args, kwargs)
            return future.result()  # blocks with GIL released

        return wrapper
    return decorator
