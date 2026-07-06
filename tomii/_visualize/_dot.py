"""GraphViz DOT emitter for Tomii graphs (`python -m tomii --dump`)."""

from __future__ import annotations

from ._parser import VizGraph


def _esc(s: str) -> str:
    return s.replace('"', '\\"')


_EDGE_STYLE = {
    "res": "solid",
    "dep": "dashed",
    "barrier": "bold",
}


def to_dot(viz: VizGraph, title: str = "tomii") -> str:
    """Render a parsed graph as GraphViz DOT.

    Node shape encodes kind (box = compute, component = post-node,
    diamond decoration for conditional); edge style encodes dependency type
    (solid = $res, dashed = $dep, bold = $barrier).
    """
    lines = [
        f'digraph "{_esc(title)}" {{',
        "  rankdir=TB;",
        '  node [shape=box, fontname="monospace"];',
        '  edge [fontname="monospace", fontsize=10];',
    ]

    for node in viz.nodes:
        attrs = []
        label = node.label  # parser label already includes "| f=<factor>"
        if node.function and node.function != node.id:
            label += f"\\n{node.function}"
        if node.condition_summary:
            label += f"\\nif {node.condition_summary}"
        attrs.append(f'label="{_esc(label)}"')
        if node.kind == "post":
            attrs.append("shape=component")
        if node.kind == "conditional" or node.condition_summary:
            attrs.append("style=diagonals")
        if node.has_loop:
            attrs.append("peripheries=2")
        lines.append(f'  "{_esc(node.id)}" [{", ".join(attrs)}];')

    for edge in viz.edges:
        style = _EDGE_STYLE.get(edge.edge_type, "solid")
        label = edge.label or edge.edge_type
        lines.append(
            f'  "{_esc(edge.source)}" -> "{_esc(edge.target)}" '
            f'[style={style}, label="{_esc(label)}"];'
        )

    if viz.init_vars:
        init_names = ", ".join(v.name for v in viz.init_vars)
        lines.append(
            f'  _inits [shape=note, label="initializations:\\n{_esc(init_names)}"];'
        )

    lines.append("}")
    return "\n".join(lines) + "\n"
