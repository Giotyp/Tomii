"""Serialize a Graph to the Τομί JSON DSL format.

Serialization is implemented by constructing Pydantic models from _generated.py
(which mirrors json_structs.rs) and calling model_dump(). This means json_structs.rs
is the single source of truth — run `make schema` after changing Rust types.
"""

from __future__ import annotations

import json
from typing import Any, Optional

from ._generated import (
    ArgInit,
    ArgJson,
    ConditionJson,
    Factor,
    GraphFile,
    IndexFunctionJson,
    InitJson,
    LoopJson,
    NetworkConfigJson,
    NodeConditionJson,
    NodeJson,
    PredJson,
)
from ._loop import Condition, IndexFunc, Loop
from ._node import Node, NodeBarrier, NodeDep, NodeOutput
from ._types import TypedValue, infer_type
from ._var import Var


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #


def _factor(f: Any) -> Optional[Factor]:
    """Convert int or Var to a Factor (Union[int, str])."""
    if f is None:
        return None
    return f.name if isinstance(f, Var) else f


def _indexes(start: Any, end: Any) -> str:
    """Build the indexes string from start / end values."""
    if isinstance(start, list):
        return ",".join(str(i) for i in start)
    if end is None:
        return str(start)
    end_str = end.name if isinstance(end, Var) else str(end)
    return f"{start}-{end_str}"


def _arg(a: Any) -> ArgJson:
    """Convert a node argument to an ArgJson model."""
    if isinstance(a, Var):
        return ArgJson(type_="$ref", value=a.name)

    if isinstance(a, NodeDep):
        pred = PredJson(
            name=a.node.name,
            indexes=_indexes(a.start, a.end),
            group_by=_factor(a.group_by),
        )
        return ArgJson(type_="$dep", predecessor=pred)

    if isinstance(a, NodeOutput):
        pred = PredJson(
            name=a.node.name,
            indexes=_indexes(a.start, a.end),
            group_by=_factor(a.group_by),
        )
        return ArgJson(type_="$res", predecessor=pred)

    if isinstance(a, NodeBarrier):
        pred = PredJson(
            name=a.node.name,
            indexes=_indexes(a.start, a.end),
            group_by=_factor(a.group_by),
        )
        return ArgJson(type_="$barrier", predecessor=pred)

    if isinstance(a, TypedValue):
        return ArgJson(type_=a.type_name, value=a.value_str)

    tv = infer_type(a)
    return ArgJson(type_=tv.type_name, value=tv.value_str)


def _arg_init(a: Any) -> ArgInit:
    """Convert an initialization argument to an ArgInit model.

    Init args always have a required value — no predecessors allowed.
    """
    if isinstance(a, Var):
        return ArgInit(type_="$ref", value=a.name)
    if isinstance(a, TypedValue):
        return ArgInit(type_=a.type_name, value=a.value_str)
    tv = infer_type(a)
    return ArgInit(type_=tv.type_name, value=tv.value_str)


def _condition_value(v: Any) -> str:
    """Serialize a Condition.value to its string representation."""
    if isinstance(v, TypedValue):
        return v.value_str
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _node(n: Node) -> NodeJson:
    """Convert a Node to a NodeJson model."""
    loop: Optional[LoopJson] = None
    if n.loop is not None:
        lp: Loop = n.loop
        loop = LoopJson(name=lp.name, factor=_factor(lp.factor))

    condition: Optional[NodeConditionJson] = None
    if n.condition is not None:
        cond: Condition = n.condition
        condition = NodeConditionJson(
            operation=cond.operation,
            value=_condition_value(cond.value),
            value_type=cond.value_type,
            function=cond.func,
            args=[_arg(a) for a in cond.args],
        )

    return NodeJson(
        name=n.name,
        factor=_factor(n.factor),
        function=n.func,
        loop_=loop,
        loop_args=[_arg(a) for a in n.loop_args] if n.loop_args else None,
        args=[_arg(a) for a in n.args],
        group_size=_factor(n.group_size) if n.group_size is not None else None,
        condition=condition,
        priority=n.priority,
        use_workers=n.use_workers,
    )


def _init(v: Var) -> InitJson:
    """Convert a Var to an InitJson model."""
    args: list[ArgInit] = []
    if v.value is not None:
        args.append(_arg_init(v.value))
    for a in v.args:
        args.append(_arg_init(a))

    return InitJson(
        name=v.name,
        factor=_factor(v.factor),
        args=args,
        function=v.func,
    )


def _network(config: dict) -> NetworkConfigJson:
    """Convert the network config dict to a NetworkConfigJson model."""
    index_func_cfg = config.get("index_function")
    index_function: Optional[IndexFunctionJson] = None
    if index_func_cfg is not None:
        if isinstance(index_func_cfg, IndexFunc):
            index_function = IndexFunctionJson(
                function=index_func_cfg.function,
                args=[_arg(a) for a in index_func_cfg.args],
            )
        elif isinstance(index_func_cfg, dict):
            index_function = IndexFunctionJson(
                function=index_func_cfg["function"],
                args=[_arg(a) for a in index_func_cfg.get("args", [])],
            )

    def _factor_net(v: Any) -> Any:
        """Network config values: Var → name string, else pass-through."""
        return v.name if isinstance(v, Var) else v

    return NetworkConfigJson(
        socket_type=config["socket_type"],
        num_sockets=_factor_net(config["num_sockets"]),
        packet_length=_factor_net(config["packet_length"]),
        stream_packets=_factor_net(config["stream_packets"]),
        buffer_depth=config.get("buffer_depth", 128),
        address=_factor_net(config["address"]),
        start_port=_factor_net(config["start_port"]),
        extract_packet_func=config["extract_packet_func"],
        id_function=config["id_function"],
        index_function=index_function,
    )


# --------------------------------------------------------------------------- #
# Public API (preserved for test compatibility)
# --------------------------------------------------------------------------- #


def serialize_arg(arg: Any) -> dict:
    """Convert a single argument to its JSON DSL dict representation."""
    return _arg(arg).model_dump(by_alias=True, exclude_none=True)


def serialize_var(var: Var) -> dict:
    """Convert a Var to an initialization entry dict."""
    return _init(var).model_dump(by_alias=True, exclude_none=True)


def serialize_node(node: Node) -> dict:
    """Convert a Node to a node entry dict."""
    return _node(node).model_dump(by_alias=True, exclude_none=True)


def serialize_graph(graph: Any) -> dict:
    """Convert a Graph object to the full JSON DSL dict."""
    return GraphFile(
        initializations=[_init(v) for v in graph._vars],
        nodes=[_node(n) for n in graph._nodes],
        post_nodes=[_node(n) for n in graph._post_nodes] or None,
        network_config=_network(graph._network) if graph._network else None,
    ).model_dump(by_alias=True, exclude_none=True)


def to_json(graph: Any, indent: int = 4) -> str:
    """Serialize a Graph to a JSON string."""
    return json.dumps(serialize_graph(graph), indent=indent)
