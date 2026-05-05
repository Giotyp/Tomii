"""Localhost HTTP server for the Τομί graph visualizer/editor."""

from __future__ import annotations

import json
import socket
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

from ._parser import VizGraph


def _html_template() -> str:
    html_path = Path(__file__).parent / "_web" / "index.html"
    return html_path.read_text(encoding="utf-8")


def _viz_to_dict(viz: VizGraph) -> dict:
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
                "raw": n.raw,
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
                "raw": iv.raw,
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


def serve(
    viz: VizGraph,
    port: Optional[int] = None,
    open_browser: bool = True,
    editor_mode: str = "view",  # "view" | "edit" | "create"
    save_path: Optional[str] = None,
) -> None:
    """Serve the graph visualizer/editor on localhost.

    Parameters
    ----------
    viz:
        The parsed graph to display (empty VizGraph for create mode).
    port:
        TCP port to bind to. Auto-selected if None.
    open_browser:
        Whether to auto-open the browser.
    editor_mode:
        ``"view"`` — read-only (default),
        ``"edit"`` — load graph and allow modifications,
        ``"create"`` — empty canvas, build from scratch.
    save_path:
        File path to write to when the user clicks Save. Required for edit/create modes.
    """
    if port is None:
        port = _find_free_port()

    graph_json = json.dumps(_viz_to_dict(viz))
    template = _html_template()
    html = (
        template.replace("{{GRAPH_DATA}}", graph_json)
        .replace("{{EDITOR_MODE}}", editor_mode)
        .replace("{{SAVE_PATH}}", save_path or "")
    )
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

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)

            if self.path == "/api/save-json":
                self._handle_save_json(body)
            elif self.path == "/api/export-python":
                self._handle_export_python(body)
            else:
                self.send_response(404)
                self.end_headers()

        def _handle_save_json(self, body: bytes) -> None:
            if not save_path:
                self._json_error(400, "No save path configured on server.")
                return
            try:
                data = json.loads(body)
                # Pretty-print with 4-space indent
                text = json.dumps(data, indent=4)
                Path(save_path).write_text(text, encoding="utf-8")
                self._json_ok({"status": "saved", "path": save_path})
                print(f"  Saved → {save_path}")
            except Exception as exc:
                self._json_error(500, str(exc))

        def _handle_export_python(self, body: bytes) -> None:
            try:
                from ._codegen import generate_python

                data = json.loads(body)
                code = generate_python(data)
                code_bytes = code.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/x-python; charset=utf-8")
                self.send_header("Content-Length", str(len(code_bytes)))
                self.end_headers()
                self.wfile.write(code_bytes)
            except Exception as exc:
                self._json_error(500, str(exc))

        def _json_ok(self, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _json_error(self, code: int, msg: str) -> None:
            body = json.dumps({"error": msg}).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = HTTPServer(("127.0.0.1", port), _Handler)
    url = f"http://localhost:{port}/"

    mode_label = {"view": "View", "edit": "Edit", "create": "Create"}[editor_mode]
    print(f"  Τομί Graph Visualizer [{mode_label}]  →  {url}")
    if save_path:
        print(f"  Save path: {save_path}")
    print("  Press Ctrl+C to stop.\n")

    if open_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopped.")
    finally:
        server.server_close()
