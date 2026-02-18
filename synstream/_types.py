"""Type wrappers for SynStream argument values.

Auto-inference rules:
    Python int   → usize
    Python float → f64
    Python bool  → bool

For explicit control use the wrapper functions: ss.i32(-5), ss.String("hello"), etc.
"""

from __future__ import annotations


class TypedValue:
    """A value with an explicit SynStream type name."""

    def __init__(self, type_name: str, value_str: str) -> None:
        self.type_name = type_name
        self.value_str = value_str

    def __repr__(self) -> str:
        return f"{self.type_name}({self.value_str!r})"


# --------------------------------------------------------------------------- #
# Scalar type wrappers
# --------------------------------------------------------------------------- #

def _scalar(type_name: str):
    def factory(value) -> TypedValue:
        return TypedValue(type_name, str(value))
    factory.__name__ = type_name
    factory.__qualname__ = type_name
    return factory


usize   = _scalar("usize")
isize   = _scalar("isize")
i8      = _scalar("i8")
i16     = _scalar("i16")
i32     = _scalar("i32")
i64     = _scalar("i64")
i128    = _scalar("i128")
u8      = _scalar("u8")
u16     = _scalar("u16")
u32     = _scalar("u32")
u64     = _scalar("u64")
u128    = _scalar("u128")
f32     = _scalar("f32")
f64     = _scalar("f64")


def String(value: str) -> TypedValue:  # noqa: N802
    return TypedValue("String", str(value))


def bool_(value: bool) -> TypedValue:
    return TypedValue("bool", "true" if value else "false")


def char_(value: str) -> TypedValue:
    if len(value) != 1:
        raise ValueError(f"char_ expects a single character, got {value!r}")
    return TypedValue("char", value)


# --------------------------------------------------------------------------- #
# Complex types
# --------------------------------------------------------------------------- #

def Complex32(real: float, imag: float) -> TypedValue:
    return TypedValue("Complex32", f"{real},{imag}")


def Complex64(real: float, imag: float) -> TypedValue:
    return TypedValue("Complex64", f"{real},{imag}")


# --------------------------------------------------------------------------- #
# Vec type
# --------------------------------------------------------------------------- #

def Vec(element_type: str, values: list) -> TypedValue:  # noqa: N802
    return TypedValue(f"Vec<{element_type}>", ",".join(str(v) for v in values))


# --------------------------------------------------------------------------- #
# Auto-inference
# --------------------------------------------------------------------------- #

def infer_type(value) -> TypedValue:
    """Convert a plain Python value to a TypedValue using auto-inference rules."""
    if isinstance(value, bool):
        # Must check bool before int — bool is a subclass of int in Python
        return bool_(value)
    if isinstance(value, int):
        return usize(value)
    if isinstance(value, float):
        return f64(value)
    if isinstance(value, TypedValue):
        return value
    raise TypeError(
        f"Cannot auto-infer type for {value!r} (type {type(value).__name__}). "
        "Use an explicit wrapper like ss.String(...), ss.i32(...), etc."
    )
