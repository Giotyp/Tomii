"""Python compute functions for the matrix-compute-python Tomii example.

Each function is decorated with @tomii.export so Graph.py_node() can
reference it by object and the bridge knows which entry point to use.
The functions are plain NumPy — no Tomii-specific types or imports needed.

GIL strategy
------------
NumPy/BLAS release the GIL internally during matmul and FFT (they call
Py_BEGIN_ALLOW_THREADS around BLAS/FFTW). With stock Python 3.12 and
multiple Tomii worker threads, mat_mul and compute_fft already run in
parallel — no special GIL bypass needed.

@tomii.procs() is shown on pure_python_example
below to illustrate the pattern for genuine pure-Python compute (loops,
comprehensions) that would otherwise serialize under the GIL. The
overhead (~50-200 µs per call) only pays off when compute >> overhead.

For the functions in this file (NumPy-dominated), @tomii.procs() is omitted
because the GIL is already released by NumPy.
"""

import numpy as np
import tomii


@tomii.export
def generate_vector(n: int) -> np.ndarray:
    """Return a complex64 vector of length n filled with random values."""
    return (np.random.randn(n) + 1j * np.random.randn(n)).astype(np.complex64)


@tomii.export
def compute_fft(v: np.ndarray) -> np.ndarray:
    """Apply an FFT and return the result as complex64.

    np.fft.fft releases the GIL internally — runs in parallel across workers
    without any additional wrapping.
    """
    return np.fft.fft(v).astype(np.complex64)


@tomii.export
def vec_to_mat(v: np.ndarray) -> np.ndarray:
    """Reshape a flat complex vector into a square matrix."""
    n = int(np.sqrt(len(v)))
    return v[:n * n].reshape(n, n)


@tomii.export
def mat_mul(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Multiply two complex matrices.

    BLAS matmul releases the GIL — runs in parallel across workers without
    @tomii.procs(). For large matrices (>100×100) this already saturates the
    CPU cores via BLAS threading.
    """
    return a @ b


@tomii.export(variadic=True)
def write_to_file(path: str, mats: list) -> None:
    """Append all matrices to path as a NumPy .npz archive."""
    np.savez(path, *[np.asarray(m) for m in mats])


# ---------------------------------------------------------------------------
# Example: pure-Python function that DOES need @tomii.procs()
# ---------------------------------------------------------------------------

@tomii.export
@tomii.procs()   # dispatches to a ProcessPoolExecutor; GIL released during wait
def pure_python_example(data: list) -> list:
    """Pure-Python transform — would serialize under the GIL without @tomii.procs().

    With @tomii.procs(), each Tomii worker releases the GIL while waiting for
    its subprocess result, so N workers execute this concurrently in N separate
    Python processes (no shared GIL).
    """
    return [x * x for x in data]
