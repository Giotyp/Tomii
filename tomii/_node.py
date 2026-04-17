"""Node, NodeOutput, and NodeBarrier — computation graph nodes."""

from __future__ import annotations
from typing import Any, List, Optional, Union


class NodeOutput:
    """A result dependency on a predecessor node (serializes as ``$res``)."""

    def __init__(
        self,
        node: "Node",
        start: Union[int, str, list],
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
    ) -> None:
        self.node = node
        self.start = start
        self.end = end
        self.group_by = group_by

    def __repr__(self) -> str:
        idx = _format_indexes(self.start, self.end)
        return f"NodeOutput({self.node.name!r}, {idx!r})"


class NodeDep(NodeOutput):
    """An ordering-only dependency on a predecessor node (serializes as ``$dep``).

    Like ``NodeOutput`` but signals to the runtime that the result value is not
    needed — only the completion ordering matters.  The runtime skips result
    storage for nodes whose only non-barrier successors are ordering-only deps.
    """

    def __repr__(self) -> str:
        idx = _format_indexes(self.start, self.end)
        return f"NodeDep({self.node.name!r}, {idx!r})"


class NodeBarrier:
    """A barrier dependency on a predecessor node (serializes as ``$barrier``)."""

    def __init__(
        self,
        node: "Node",
        start: Union[int, str, list],
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
    ) -> None:
        self.node = node
        self.start = start
        self.end = end
        self.group_by = group_by

    def __repr__(self) -> str:
        idx = _format_indexes(self.start, self.end)
        return f"NodeBarrier({self.node.name!r}, {idx!r})"


def _format_indexes(start, end) -> str:
    if isinstance(start, list):
        return ",".join(str(i) for i in start)
    if end is None:
        return str(start)
    return f"{start}-{end}"


class Node:
    """A computation node in the Τομί task graph."""

    def __init__(
        self,
        name: str,
        *,
        func: str,
        args: Optional[List[Any]] = None,
        factor: Optional[Union[int, "Var"]] = None,
        priority: Optional[str] = None,
        use_workers: Optional[str] = None,
        group_size: Optional[int] = None,
        loop: Optional[Any] = None,
        loop_args: Optional[List[Any]] = None,
        condition: Optional[Any] = None,
        is_post: bool = False,
    ) -> None:
        self.name = name
        self.func = func
        self.args: List[Any] = args or []
        self.factor = factor
        self.priority = priority
        self.use_workers = use_workers
        self.group_size = group_size
        self.loop = loop
        self.loop_args: List[Any] = loop_args or []
        self.condition = condition
        self.is_post = is_post

    def out(
        self,
        start: Union[int, str, list] = 0,
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
    ) -> NodeOutput:
        """Return a result dependency on this node."""
        return NodeOutput(self, start, end, group_by=group_by)

    def dep(
        self,
        start: Union[int, str, list] = 0,
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
    ) -> NodeDep:
        """Return an ordering-only dependency on this node (serializes as ``$dep``).

        The predecessor edge is tracked for scheduling but the result value is not
        fetched from the result buffer — the arg slot receives ``None`` instead.
        """
        return NodeDep(self, start, end, group_by=group_by)

    def wait(
        self,
        start: Union[int, str, list] = 0,
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
    ) -> NodeBarrier:
        """Return a barrier dependency on this node."""
        return NodeBarrier(self, start, end, group_by=group_by)

    def __repr__(self) -> str:
        return f"Node({self.name!r})"
