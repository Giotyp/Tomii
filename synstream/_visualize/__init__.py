"""SynStream graph visualization toolkit.

Two output modes:
    - "web"   (default) — interactive Cytoscape.js in browser (localhost)
    - "ascii" — box-drawing art printed to terminal

Usage::

    # From Python
    import synstream as ss
    app = ss.Graph()
    # ... build graph ...
    app.visualize()          # web mode (opens browser)
    app.visualize("ascii")   # terminal

    # From CLI
    python -m synstream --visualize graph.json
    python -m synstream --visualize graph.json --ascii
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional

from ._parser import VizGraph, parse_graph, parse_json_file


def visualize(
    source: "str | Path | Any",
    mode: str = "web",
    *,
    port: Optional[int] = None,
    open_browser: bool = True,
) -> None:
    """Visualize a SynStream graph.

    Parameters
    ----------
    source:
        A JSON file path (str or Path) or a live ``synstream.Graph`` object.
    mode:
        ``"web"`` (default) or ``"ascii"``.
    port:
        TCP port for the web server. Auto-selected if None.
    open_browser:
        Whether to auto-open the browser in web mode (default True).
    """
    # Parse the source into a VizGraph
    if isinstance(source, (str, Path)):
        viz: VizGraph = parse_json_file(source)
    else:
        viz = parse_graph(source)

    if mode == "ascii":
        from ._ascii import print_graph
        print_graph(viz)

    elif mode == "web":
        from ._server import serve
        serve(viz, port=port, open_browser=open_browser)

    else:
        raise ValueError(f"Unknown mode {mode!r}. Choose 'web' or 'ascii'.")


__all__ = ["visualize", "parse_json_file", "parse_graph", "VizGraph"]
