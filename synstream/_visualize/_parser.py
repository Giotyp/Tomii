"""Parse a SynStream graph (JSON file or live Graph object) into a VizGraph topology."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional


@dataclass
class VizNode:
    id: str
    label: str          # display label shown in renderers
    kind: str           # "compute" | "post" | "conditional"
    function: str
    factor: Optional[str] = None
    priority: Optional[str] = None
    group_size: Optional[str] = None
    has_loop: bool = False
    condition_summary: Optional[str] = None  # e.g. "check_bool == true"


@dataclass
class VizEdge:
    source: str         # predecessor node name
    target: str         # successor node name
    edge_type: str      # "res" | "dep" | "barrier"
    indexes: str = "0"
    group_by: Optional[str] = None
    label: str = ""     # rendered label (edge_type + indexes)


@dataclass
class VizInitVar:
    name: str
    value: Optional[str] = None
    function: Optional[str] = None


@dataclass
class VizGraph:
    nodes: List[VizNode] = field(default_factory=list)
    edges: List[VizEdge] = field(default_factory=list)
    init_vars: List[VizInitVar] = field(default_factory=list)
    has_post_nodes: bool = False
    has_network: bool = False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _factor_str(f: Any) -> Optional[str]:
    if f is None:
        return None
    return str(f)


def _edges_from_args(args: list, target_name: str) -> List[VizEdge]:
    """Extract VizEdge list from a list of ArgJson dicts."""
    edges: List[VizEdge] = []
    seen: set = set()  # deduplicate (source, type) pairs

    for arg in args:
        type_ = arg.get("type", "")
        pred = arg.get("predecessor")
        if pred is None or type_ not in ("$res", "$dep", "$barrier"):
            continue

        source = pred.get("name", "")
        indexes = pred.get("indexes", "0")
        group_by = pred.get("group_by")
        edge_type = type_[1:]  # strip leading "$"

        key = (source, target_name, edge_type, indexes)
        if key in seen:
            continue
        seen.add(key)

        label_parts = [edge_type]
        if indexes and indexes != "0":
            label_parts.append(f"[{indexes}]")
        if group_by:
            label_parts.append(f"grp={group_by}")
        label = " ".join(label_parts)

        edges.append(VizEdge(
            source=source,
            target=target_name,
            edge_type=edge_type,
            indexes=str(indexes),
            group_by=str(group_by) if group_by is not None else None,
            label=label,
        ))

    return edges


def _condition_summary(cond: dict) -> str:
    op = cond.get("operation", "?")
    val = cond.get("value", "?")
    func = cond.get("function", "")
    return f"{func} {op} {val}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_json_file(path: str | Path) -> VizGraph:
    """Parse a SynStream JSON graph file into a VizGraph."""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return _parse_dict(data)


def parse_graph(graph: Any) -> VizGraph:
    """Parse a live synstream.Graph object into a VizGraph."""
    from .._serialize import serialize_graph
    data = serialize_graph(graph)
    return _parse_dict(data)


def _parse_dict(data: dict) -> VizGraph:
    viz = VizGraph()

    # Init vars
    for init in data.get("initializations", []):
        val = None
        if init.get("args"):
            val = init["args"][0].get("value")
        viz.init_vars.append(VizInitVar(
            name=init["name"],
            value=val,
            function=init.get("function"),
        ))

    # Compute nodes
    node_names: set[str] = set()
    for node in data.get("nodes", []):
        name = node["name"]
        node_names.add(name)
        cond = node.get("condition")
        has_cond = cond is not None
        factor = _factor_str(node.get("factor"))
        priority = node.get("priority")

        label_parts = [name]
        if factor:
            label_parts.append(f"f={factor}")
        if priority:
            label_parts.append(f"[{priority}]")

        viz.nodes.append(VizNode(
            id=name,
            label=" | ".join(label_parts),
            kind="conditional" if has_cond else "compute",
            function=node["function"],
            factor=factor,
            priority=priority,
            group_size=_factor_str(node.get("group_size")),
            has_loop=node.get("loop") is not None,
            condition_summary=_condition_summary(cond) if cond else None,
        ))

        # Edges from main args
        viz.edges.extend(_edges_from_args(node.get("args", []), name))

        # Edges from condition args (condition function has its own data deps)
        if cond:
            viz.edges.extend(_edges_from_args(cond.get("args", []), name))

    # Post-nodes
    for node in data.get("post_nodes", []) or []:
        name = node["name"]
        node_names.add(name)
        factor = _factor_str(node.get("factor"))

        label_parts = [name, "(post)"]
        if factor:
            label_parts.append(f"f={factor}")

        viz.nodes.append(VizNode(
            id=name,
            label=" ".join(label_parts),
            kind="post",
            function=node["function"],
            factor=factor,
        ))
        viz.edges.extend(_edges_from_args(node.get("args", []), name))
        viz.has_post_nodes = True

    # Network
    if data.get("network_config"):
        viz.has_network = True

    return viz
