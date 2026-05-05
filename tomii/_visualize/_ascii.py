"""ASCII art renderer for Τομί graph topology."""

from __future__ import annotations

import shutil
from collections import defaultdict, deque
from typing import Dict, List, Optional, Set, Tuple

from ._parser import VizEdge, VizGraph, VizNode

# Box-drawing characters
_TL, _TR, _BL, _BR = "┌", "┐", "└", "┘"
_H, _V = "─", "│"
_LARROW = "◄"

# Node box width (internal content width, not counting borders)
_BOX_W = 28

_EDGE_SYMBOLS = {
    "res": "→",
    "dep": "⇢",  # dashed feel
    "barrier": "⟹",
}

_KIND_BADGE = {
    "conditional": "[cond]",
    "post": "[post]",
    "compute": "",
}


def _wrap(text: str, width: int) -> List[str]:
    """Simple word-wrap a string to given width, no hyphenation."""
    if len(text) <= width:
        return [text]
    lines: List[str] = []
    while len(text) > width:
        cut = text.rfind(" ", 0, width)
        if cut == -1:
            cut = width
        lines.append(text[:cut])
        text = text[cut:].lstrip()
    if text:
        lines.append(text)
    return lines


def _box(node: VizNode, width: int = _BOX_W) -> List[str]:
    """Render a node as a box. Returns list of strings (lines)."""
    lines: List[str] = []

    # Line 1: name + kind badge
    badge = _KIND_BADGE.get(node.kind, "")
    name_line = node.id + (" " + badge if badge else "")
    for wl in _wrap(name_line, width):
        lines.append(wl)

    # Line 2: function
    fn_line = f"fn:{node.function}"
    for wl in _wrap(fn_line, width):
        lines.append(wl)

    # Line 3: attributes
    attrs: List[str] = []
    if node.factor:
        attrs.append(f"f={node.factor}")
    if node.priority:
        attrs.append(node.priority)
    if node.group_size:
        attrs.append(f"gs={node.group_size}")
    if node.has_loop:
        attrs.append("loop")
    if attrs:
        attr_line = " ".join(attrs)
        for wl in _wrap(attr_line, width):
            lines.append(wl)

    # Line 4: condition summary
    if node.condition_summary:
        cond_line = f"if: {node.condition_summary}"
        for wl in _wrap(cond_line, width):
            lines.append(wl)

    # Build box
    inner_w = max(width, max(len(l) for l in lines))
    top = _TL + _H * (inner_w + 2) + _TR
    bottom = _BL + _H * (inner_w + 2) + _BR
    rows = [top]
    for l in lines:
        rows.append(_V + " " + l.ljust(inner_w) + " " + _V)
    rows.append(bottom)
    return rows


def _topo_layers(nodes: List[VizNode], edges: List[VizEdge]) -> List[List[str]]:
    """Assign nodes to topological layers (BFS from roots). Returns list of layers."""
    node_ids = {n.id for n in nodes}
    in_degree: Dict[str, int] = {n.id: 0 for n in nodes}
    successors: Dict[str, List[str]] = defaultdict(list)

    for e in edges:
        if e.source in node_ids and e.target in node_ids:
            in_degree[e.target] = in_degree.get(e.target, 0) + 1
            successors[e.source].append(e.target)

    # Deduplicate in_degree increments — track unique predecessors per node
    pred_sets: Dict[str, Set[str]] = defaultdict(set)
    for e in edges:
        if e.source in node_ids and e.target in node_ids:
            pred_sets[e.target].add(e.source)

    in_degree = {n.id: len(pred_sets[n.id]) for n in nodes}

    queue: deque[str] = deque(n.id for n in nodes if in_degree[n.id] == 0)
    layer_of: Dict[str, int] = {}
    order: List[str] = []

    while queue:
        nid = queue.popleft()
        order.append(nid)
        for succ in successors[nid]:
            in_degree[succ] -= 1
            if in_degree[succ] == 0:
                queue.append(succ)
            # Assign layer = max(pred_layer) + 1
            pred_layer = max((layer_of.get(p, 0) for p in pred_sets[succ]), default=-1)
            layer_of[succ] = max(layer_of.get(succ, 0), pred_layer + 1)

    for nid in order:
        if nid not in layer_of:
            layer_of[nid] = 0

    # Group by layer
    max_layer = max(layer_of.values(), default=0)
    layers: List[List[str]] = [[] for _ in range(max_layer + 1)]
    for n in nodes:
        layers[layer_of.get(n.id, 0)].append(n.id)

    return layers


def _edge_summary(edges: List[VizEdge], target: str) -> str:
    """Build a compact incoming-edge summary string for a node."""
    parts: List[str] = []
    for e in edges:
        if e.target == target:
            sym = _EDGE_SYMBOLS.get(e.edge_type, "→")
            parts.append(f"{e.source}{sym}")
    if not parts:
        return ""
    return "from: " + ", ".join(parts)


def render(viz: VizGraph) -> str:
    """Render the VizGraph as ASCII art. Returns a multi-line string."""
    term_w = shutil.get_terminal_size((100, 40)).columns
    box_w = min(_BOX_W, term_w // 3 - 4)

    node_map: Dict[str, VizNode] = {n.id: n for n in viz.nodes}
    layers = _topo_layers(viz.nodes, viz.edges)

    output_lines: List[str] = []

    # Header
    output_lines.append("")
    output_lines.append("  Τομί Graph")
    n_compute = sum(1 for n in viz.nodes if n.kind != "post")
    n_post = sum(1 for n in viz.nodes if n.kind == "post")
    summary = f"  {n_compute} node(s)"
    if n_post:
        summary += f" + {n_post} post-node(s)"
    if viz.has_network:
        summary += " + network"
    output_lines.append(summary)
    output_lines.append("")

    # Legend
    legend_parts = ["  Legend: "]
    for etype, sym in _EDGE_SYMBOLS.items():
        legend_parts.append(f"{sym}={etype}")
    output_lines.append("  " + "  ".join(legend_parts))
    output_lines.append("")
    output_lines.append("  " + "─" * (term_w - 4))
    output_lines.append("")

    for layer_idx, layer in enumerate(layers):
        if not layer:
            continue

        # Render all boxes in this layer side-by-side
        boxes: List[List[str]] = []
        for nid in layer:
            node = node_map.get(nid)
            if node is None:
                continue
            edge_hint = _edge_summary(viz.edges, nid)
            b = _box(node, box_w)
            # Prepend edge summary above the box if there are predecessors
            if edge_hint:
                padding = " " * (box_w + 4)
                b = [padding, f"  {edge_hint}"] + b
            boxes.append(b)

        if not boxes:
            continue

        # Pad all boxes to same height
        max_h = max(len(b) for b in boxes)
        boxes = [b + [""] * (max_h - len(b)) for b in boxes]

        # Interleave columns with 4-space separator
        sep = "    "
        for row_i in range(max_h):
            row = sep.join(b[row_i] for b in boxes)
            output_lines.append("  " + row)

        # Arrow row between layers
        if layer_idx < len(layers) - 1:
            arrow_line = ""
            for nid in layer:
                node = node_map.get(nid)
                if node is None:
                    continue
                # Find outgoing edges
                out_edges = [e for e in viz.edges if e.source == nid]
                if out_edges:
                    inner_w = box_w + 2
                    center_pos = inner_w // 2
                    arrow_line += (
                        " " * (center_pos) + "│" + " " * (inner_w - center_pos) + "    "
                    )
                else:
                    arrow_line += " " * (box_w + 6)
            if arrow_line.strip():
                output_lines.append("  " + arrow_line)
                output_lines.append("  " + arrow_line.replace("│", "▼"))
        output_lines.append("")

    # Init vars section
    if viz.init_vars:
        output_lines.append("  " + "─" * (term_w - 4))
        output_lines.append("  Init variables:")
        for iv in viz.init_vars:
            parts = [f"    {iv.name}"]
            if iv.function:
                parts.append(f"= {iv.function}(...)")
            elif iv.value is not None:
                parts.append(f"= {iv.value}")
            output_lines.append("".join(parts))
        output_lines.append("")

    return "\n".join(output_lines)


def print_graph(viz: VizGraph) -> None:
    """Print the ASCII representation to stdout."""
    print(render(viz))
