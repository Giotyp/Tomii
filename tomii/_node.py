"""Node, NodeOutput, and NodeBarrier — computation graph nodes."""

from __future__ import annotations
from typing import Any, Callable, Iterable, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    from ._var import Var

# A per-consumer-instance dependency map: either an explicit list of lists
# (`map[j]` = absolute producer instances consumer instance `j` depends on),
# or a callable `j -> Iterable[int]` evaluated at build time once the
# consumer's factor is known (see `tomii.halo`, `tomii.stride`).
InstanceMap = Union["List[List[int]]", Callable[[int], Iterable[int]]]


def _validate_map_args(map: Optional[InstanceMap], group_by: Optional[int]) -> None:
    """`map` is mutually exclusive with `group_by` — mirrors the Rust-side
    validation (`instance_map` vs. `indexes`/`group_by`) but fails at graph-build
    time, closer to the mistake, with a Python-native error."""
    if map is not None and group_by is not None:
        raise ValueError(
            "`map` is mutually exclusive with `group_by` — a predecessor edge is "
            "either an explicit per-instance instance_map or a group_by-partitioned "
            "range, not both."
        )


class NodeOutput:
    """A result dependency on a predecessor node (serializes as ``$res``).

    ``map`` (an explicit per-consumer-instance ``instance_map``) is supported
    here: consumer instance ``j`` reads the producer instances listed in
    ``map[j]`` (ascending absolute-index order, after the Rust-side union with
    any other mapped/coexisting argument on the same predecessor — see
    ``tomii_core::instance_map``'s module docs). A single-entry list delivers
    one value; any other length (including 0) delivers a list, matching an
    ordinary multi-index ``.out(...)`` edge's existing behaviour — there is no
    way to check the receiving function's arity from the graph alone, so a
    non-uniform-length ``map`` must be paired with a variadic plugin
    parameter, exactly as for an ordinary multi-value ``.out(...)`` edge today.
    """

    def __init__(
        self,
        node: "Node",
        start: Union[int, str, "list[int]"],
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
        map: Optional[InstanceMap] = None,
    ) -> None:
        _validate_map_args(map, group_by)
        self.node = node
        self.start = start
        self.end = end
        self.group_by = group_by
        self.map = map

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
        start: Union[int, str, "list[int]"],
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
        map: Optional[InstanceMap] = None,
    ) -> None:
        _validate_map_args(map, group_by)
        self.node = node
        self.start = start
        self.end = end
        self.group_by = group_by
        self.map = map

    def __repr__(self) -> str:
        idx = _format_indexes(self.start, self.end)
        return f"NodeBarrier({self.node.name!r}, {idx!r})"


def _format_indexes(
    start: Union[int, str, "list[int]"], end: Optional[Union[int, str, "Var"]]
) -> str:
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
        start: Union[int, str, "list[int]"] = 0,
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
        map: Optional[InstanceMap] = None,
    ) -> NodeOutput:
        """Return a result dependency on this node.

        ``map`` declares an explicit per-consumer-instance ``instance_map``:
        either a list of lists (``map[j]`` = absolute producer instances
        consumer instance ``j`` depends on) or a callable ``j -> Iterable[int]``
        evaluated once the consumer node's factor is known (a literal int, or a
        resolvable ``Var``; an unresolvable factor requires passing a list
        instead). Consumer instance ``j`` reads the producer values listed in
        ``map[j]``, in ascending absolute-index order — one value if the list
        has length 1, a list otherwise (see ``NodeOutput``'s class docstring).
        See ``tomii.halo`` and ``tomii.stride`` for common shapes.
        """
        return NodeOutput(self, start, end, group_by=group_by, map=map)

    def dep(
        self,
        start: Union[int, str, "list[int]"] = 0,
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
        map: Optional[InstanceMap] = None,
    ) -> NodeDep:
        """Return an ordering-only dependency on this node (serializes as ``$dep``).

        The predecessor edge is tracked for scheduling but the result value is not
        fetched from the result buffer — the arg slot receives ``None`` instead.

        ``map`` declares an explicit per-consumer-instance ``instance_map``:
        either a list of lists (``map[j]`` = absolute producer instances
        consumer instance ``j`` depends on) or a callable ``j -> Iterable[int]``
        evaluated once the consumer node's factor is known (a literal int, or a
        resolvable ``Var``; an unresolvable factor requires passing a list
        instead). See ``tomii.halo`` and ``tomii.stride`` for common shapes.
        """
        return NodeDep(self, start, end, group_by=group_by, map=map)

    def wait(
        self,
        start: Union[int, str, "list[int]"] = 0,
        end: Optional[Union[int, str, "Var"]] = None,
        *,
        group_by: Optional[int] = None,
        map: Optional[InstanceMap] = None,
    ) -> NodeBarrier:
        """Return a barrier dependency on this node.

        ``map`` declares an explicit per-consumer-instance ``instance_map`` —
        see ``.dep(map=...)`` above.
        """
        return NodeBarrier(self, start, end, group_by=group_by, map=map)

    def __repr__(self) -> str:
        return f"Node({self.name!r})"
