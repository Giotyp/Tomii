"""Loop, Condition, and IndexFunc helper objects."""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, List, Union


@dataclass
class Loop:
    """Loop configuration for a node.

    Args:
        name:   Loop name (used as the loop key in JSON).
        factor: Number of loop iterations (int or Var).
    """
    name: str
    factor: Union[int, Any]  # int or Var


@dataclass
class Condition:
    """Node-level conditional execution.

    Args:
        operation:  Comparison operation string, e.g. ``"Eq"``, ``"Neq"``.
        value:      Comparison value (Python int/float/bool or TypedValue).
        value_type: Explicit Rust type string, e.g. ``"usize"``.
        func:       Plugin function that returns the condition value.
        args:       Arguments passed to ``func``.
    """
    operation: str
    value: Any
    value_type: str
    func: str
    args: List[Any] = field(default_factory=list)


@dataclass
class IndexFunc:
    """Index-mapping function for network nodes.

    Args:
        function: Plugin function name.
        args:     Arguments passed to the function.
    """
    function: str
    args: List[Any] = field(default_factory=list)
