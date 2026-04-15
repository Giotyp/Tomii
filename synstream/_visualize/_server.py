"""Minimal localhost HTTP server for the SynStream graph visualizer."""

from __future__ import annotations

import importlib.resources
import json
import socket
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

from ._parser import VizGraph


def _html_template() -> str:
    """Return the index.html content from the _web package directory."""
    html_path = Path(__file__).parent / "_web" / "index.html"
    return html_path.read_text(encoding="utf-8")


def _viz_to_dict(viz: VizGraph) -> dict:
    """Convert a VizGraph to a plain dict for JSON serialization."""
    return {
        "nodes": [
            {
                "id": n.id,
                "label": n.label,
                "kind": n.kind,
                "function": n.function,
                "factor": n.factor,
                "priority": n.priority,
                "group_size": n.group_size,
                "has_loop": n.has_loop,
                "condition_summary": n.condition_summary,
            }
            for n in viz.nodes
        ],
        "edges": [
            {
                "source": e.source,
                "target": e.target,
                "edge_type": e.edge_type,
                "indexes": e.indexes,
                "group_by": e.group_by,
                "label": e.label,
            }
            for e in viz.edges
        ],
        "init_vars": [
            {
                "name": iv.name,
                "value": iv.value,
                "function": iv.function,
            }
            for iv in viz.init_vars
        ],
        "has_post_nodes": viz.has_post_nodes,
        "has_network": viz.has_network,
    }


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def serve(viz: VizGraph, port: Optional[int] = None, open_browser: bool = True) -> None:
    """Serve the interactive graph visualizer on localhost.

    Blocks until Ctrl+C is pressed.
    """
    if port is None:
        port = _find_free_port()

    graph_json = json.dumps(_viz_to_dict(viz))
    template = _html_template()
    html = template.replace("{{GRAPH_DATA}}", graph_json)
    html_bytes = html.encode("utf-8")

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            pass  # silence request logging

        def do_GET(self) -> None:
            if self.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(html_bytes)))
                self.end_headers()
                self.wfile.write(html_bytes)
            else:
                self.send_response(404)
                self.end_headers()

    server = HTTPServer(("127.0.0.1", port), _Handler)
    url = f"http://localhost:{port}/"

    print(f"  SynStream Graph Visualizer  →  {url}")
    print("  Press Ctrl+C to stop.\n")

    if open_browser:
        # Open browser after a short delay so the server is ready
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.")
    finally:
        server.server_close()
