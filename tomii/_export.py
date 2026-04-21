"""@tomii.export — Python analogue of Rust's #[tomii_export].

Decorating a function with @tomii.export registers it in _TOMII_REGISTRY so
that Graph.py_node() can reference it by object or qualified name, and so the
build step knows which bridge entry point to wire it to.

The decorator is a zero-cost marker: it returns the original function untouched
so calling it directly from Python is completely unaffected.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class ExportMeta:
    fn: Callable
    qualname: str   # e.g. "matcomp.generate_vector"
    module: str     # e.g. "matcomp"
    fn_name: str    # e.g. "generate_vector"
    variadic: bool  # True → bridge uses py_call_void (list-of-results sink)
    bridge: str     # bridge function name: "py_call_any" or "py_call_void"


_TOMII_REGISTRY: dict[str, ExportMeta] = {}


def export(fn: Optional[Callable] = None, *, variadic: bool = False, name: Optional[str] = None) -> Any:
    """Mark a Python function as a Tomii-callable node body.

    Analogous to Rust's #[tomii_export]. The decorated function is registered
    in _TOMII_REGISTRY and gets a __tomii_export__ attribute so Graph.py_node()
    can reference it by object instead of string name.

    Usage::

        @tomii.export
        def generate_vector(n: int) -> np.ndarray:
            return np.random.randn(n).astype(np.complex64)

        @tomii.export(variadic=True)
        def write_to_file(path: str, mats: list) -> None:
            np.savez(path, *mats)

    Parameters
    ----------
    variadic:
        If True, the runtime will collect all trailing result args into a Python
        list and pass them as the last positional argument. Matches
        ``#[tomii_export(variadic)]`` on the Rust side.
    name:
        Override the registry key. Defaults to ``f"{module}.{fn.__name__}"``.
    """
    def _wrap(f: Callable) -> Callable:
        mod = f.__module__
        fn_nm = f.__name__
        qualname = name or f"{mod}.{fn_nm}"
        bridge = "py_call_void" if variadic else "py_call_any"
        meta = ExportMeta(
            fn=f,
            qualname=qualname,
            module=mod,
            fn_name=fn_nm,
            variadic=variadic,
            bridge=bridge,
        )
        _TOMII_REGISTRY[qualname] = meta
        f.__tomii_export__ = meta  # type: ignore[attr-defined]
        return f

    if fn is not None:
        # Called as @tomii.export (no parentheses)
        return _wrap(fn)
    # Called as @tomii.export(...) (with keyword arguments)
    return _wrap
