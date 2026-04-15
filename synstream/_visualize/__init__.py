"""SynStream graph visualization and editing toolkit.

Two output modes:
    - "web"   (default) — interactive Cytoscape.js in browser (localhost)
    - "ascii" — box-drawing art printed to terminal

Three editor modes (web only):
    - "view"   — read-only visualization (default when file exists)
    - "edit"   — load graph and allow modifications, save back
    - "create" — empty canvas, build a graph from scratch

Usage::

    # From Python
    import synstream as ss
    app = ss.Graph()
    # ... build graph ...
    app.visualize()                        # view mode
    app.visualize("ascii")                 # terminal
    app.visualize(editable=True)           # edit mode

    # From CLI
    python -m synstream --visualize graph.json
    python -m synstream --visualize graph.json --ascii
    python -m synstream --visualize graph.json --edit
    python -m synstream --visualize new_graph.json     # create mode (file missing)
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
    editable: bool = False,
    save_path: Optional[str] = None,
) -> None:
    """Visualize (or edit) a SynStream graph.

    Parameters
    ----------
    source:
        A JSON file path (str or Path) or a live ``synstream.Graph`` object.
        Pass a non-existent path to start in create mode.
    mode:
        ``"web"`` (default) or ``"ascii"``.
    port:
        TCP port for the web server. Auto-selected if None.
    open_browser:
        Whether to auto-open the browser in web mode (default True).
    editable:
        If True, open in edit mode (modifiable, saves back to source path).
    save_path:
        Override the save location (defaults to the source path when editable).
    """
    source_path: Optional[str] = None
    if isinstance(source, (str, Path)):
        source_path = str(source)
        if Path(source).exists():
            viz: VizGraph = parse_json_file(source)
            editor_mode = "edit" if editable else "view"
        else:
            # File doesn't exist → create mode
            viz = VizGraph()
            editor_mode = "create"
            if save_path is None:
                save_path = source_path
    else:
        viz = parse_graph(source)
        editor_mode = "edit" if editable else "view"

    if save_path is None and source_path is not None and editable:
        save_path = source_path

    if mode == "ascii":
        from ._ascii import print_graph
        print_graph(viz)

    elif mode == "web":
        from ._server import serve
        serve(
            viz,
            port=port,
            open_browser=open_browser,
            editor_mode=editor_mode,
            save_path=save_path,
        )

    else:
        raise ValueError(f"Unknown mode {mode!r}. Choose 'web' or 'ascii'.")


__all__ = ["visualize", "parse_json_file", "parse_graph", "VizGraph"]
