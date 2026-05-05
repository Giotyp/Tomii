"""Var — an initialization variable in the Τομί graph."""

from __future__ import annotations
from typing import Any, List, Optional, Union


class Var:
    """Represents a named initialization object.

    When passed as an argument to a node, it serializes as a ``$ref``.
    """

    def __init__(
        self,
        name: str,
        value: Any = None,
        *,
        func: Optional[str] = None,
        args: Optional[List[Any]] = None,
        factor: Optional[Union[int, "Var"]] = None,
    ) -> None:
        if value is None and func is None:
            raise ValueError(f"Var '{name}': must provide either 'value' or 'func'.")
        if value is not None and func is not None:
            raise ValueError(f"Var '{name}': cannot specify both 'value' and 'func'.")
        self.name = name
        self.value = value
        self.func = func
        self.args: List[Any] = args or []
        self.factor = factor

    def __repr__(self) -> str:
        return f"Var({self.name!r})"
