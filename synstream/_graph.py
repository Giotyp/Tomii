"""Graph — the top-level container for a SynStream application graph."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ._node import Node
from ._serialize import to_json, serialize_graph
from ._var import Var


class Graph:
    """Top-level container. Holds all variables, nodes, and configuration."""

    def __init__(self) -> None:
        self._vars: List[Var] = []
        self._nodes: List[Node] = []
        self._post_nodes: List[Node] = []
        self._network: Dict[str, Any] = {}
        self._names: set = set()
        self._build_result: Optional[Any] = None  # BuildResult, set after build()

    # ---------------------------------------------------------------------- #
    # Graph construction
    # ---------------------------------------------------------------------- #

    def var(
        self,
        name: str,
        value: Any = None,
        *,
        func: Optional[str] = None,
        args: Optional[List[Any]] = None,
        factor: Optional[Union[int, Var]] = None,
    ) -> Var:
        """Define an initialization variable. Returns the Var object."""
        self._check_name(name)
        v = Var(name, value, func=func, args=args, factor=factor)
        self._vars.append(v)
        self._names.add(name)
        return v

    def node(
        self,
        name: str,
        *,
        func: str,
        args: Optional[List[Any]] = None,
        factor: Optional[Union[int, Var]] = None,
        priority: Optional[str] = None,
        use_workers: Optional[str] = None,
        group_size: Optional[int] = None,
        loop: Optional[Any] = None,
        loop_args: Optional[List[Any]] = None,
        condition: Optional[Any] = None,
    ) -> Node:
        """Define a computation node. Returns the Node object."""
        self._check_name(name)
        n = Node(
            name,
            func=func,
            args=args,
            factor=factor,
            priority=priority,
            use_workers=use_workers,
            group_size=group_size,
            loop=loop,
            loop_args=loop_args,
            condition=condition,
        )
        self._nodes.append(n)
        self._names.add(name)
        return n

    def post_node(
        self,
        name: str,
        *,
        func: str,
        args: Optional[List[Any]] = None,
        factor: Optional[Union[int, Var]] = None,
        priority: Optional[str] = None,
        use_workers: Optional[str] = None,
        group_size: Optional[int] = None,
        loop: Optional[Any] = None,
        loop_args: Optional[List[Any]] = None,
        condition: Optional[Any] = None,
    ) -> Node:
        """Define a post-computation node. Returns the Node object."""
        self._check_name(name)
        n = Node(
            name,
            func=func,
            args=args,
            factor=factor,
            priority=priority,
            use_workers=use_workers,
            group_size=group_size,
            loop=loop,
            loop_args=loop_args,
            condition=condition,
            is_post=True,
        )
        self._post_nodes.append(n)
        self._names.add(name)
        return n

    def network(self, **config: Any) -> None:
        """Define network receiver configuration."""
        self._network.update(config)

    # ---------------------------------------------------------------------- #
    # Export
    # ---------------------------------------------------------------------- #

    def to_json(self, indent: int = 4) -> str:
        """Serialize the graph to a JSON string."""
        return to_json(self, indent=indent)

    def save_json(self, path: Union[str, Path]) -> None:
        """Write the graph JSON to a file."""
        Path(path).write_text(self.to_json(), encoding="utf-8")

    # ---------------------------------------------------------------------- #
    # Build & Run
    # ---------------------------------------------------------------------- #

    def build(
        self,
        *,
        func_path: Optional[str] = None,
        wrap_path: Optional[str] = None,
        reg_path: Optional[str] = None,
        plugin_manifest: Optional[str] = None,
        release: bool = True,
        clean: bool = False,
        env: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Compile synstream-core and the plugin library.

        Recommended: use ``func_path`` pointing at your Rust source file.
        The build system auto-generates FFI wrappers from ``#[synstream_export]``
        annotations — no need to write wrappers.rs or reg.rs by hand.

        Legacy: ``wrap_path`` + ``reg_path`` for pre-written wrapper files.

        Returns a BuildResult with the path to the compiled .so.
        """
        from ._builder import BuildConfig, build as _build
        cfg = BuildConfig(
            func_path=func_path,
            wrap_path=wrap_path,
            reg_path=reg_path,
            plugin_manifest=plugin_manifest,
            release=release,
            clean=clean,
            env=env or {},
        )
        result = _build(cfg)
        self._build_result = result
        return result

    def run(
        self,
        *,
        dylib: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> Any:
        """Write graph JSON to a temp file and invoke the SynStream binary.

        If ``dylib`` is omitted and ``build()`` was called, uses that dylib.
        Returns subprocess.CompletedProcess.
        """
        from ._runner import run as _run
        if dylib is None:
            if self._build_result is None:
                raise RuntimeError(
                    "No dylib specified and build() has not been called. "
                    "Provide dylib= or call build() first."
                )
            dylib = self._build_result.dylib
        return _run(self, dylib=dylib, env=env, **kwargs)

    def build_and_run(
        self,
        *,
        func_path: Optional[str] = None,
        wrap_path: Optional[str] = None,
        reg_path: Optional[str] = None,
        plugin_manifest: Optional[str] = None,
        release: bool = True,
        clean: bool = False,
        env: Optional[Dict[str, str]] = None,
        **run_kwargs: Any,
    ) -> Any:
        """Build then run in sequence. Returns subprocess.CompletedProcess."""
        self.build(
            func_path=func_path,
            wrap_path=wrap_path,
            reg_path=reg_path,
            plugin_manifest=plugin_manifest,
            release=release,
            clean=clean,
            env=env,
        )
        return self.run(env=env, **run_kwargs)

    # ---------------------------------------------------------------------- #
    # Internals
    # ---------------------------------------------------------------------- #

    def visualize(
        self,
        mode: str = "web",
        *,
        editable: bool = False,
        save_path: Optional[str] = None,
        port: Optional[int] = None,
    ) -> None:
        """Visualize (or edit) this graph.

        Parameters
        ----------
        mode:
            ``"web"`` (default) — interactive browser visualization,
            ``"ascii"`` — terminal box-drawing art.
        editable:
            If True, open in edit mode so the graph can be modified in the browser.
        save_path:
            Where to save the graph when the user clicks Save (web edit mode).
        port:
            TCP port for the web server.
        """
        from ._visualize import visualize
        visualize(self, mode=mode, editable=editable, save_path=save_path, port=port)

    def _check_name(self, name: str) -> None:
        if name in self._names:
            raise ValueError(f"Duplicate name {name!r} in graph.")
