"""Graph — the top-level container for a Τομί application graph."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ._node import Node
from ._serialize import to_json, serialize_graph
from ._types import String
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
        self._py_callable_vars: Dict[str, Var] = {}  # qualname → Var for dedup
        self._py_module_dirs: List[str] = []        # dirs to prepend to PYTHONPATH at run()

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

    def py_node(
        self,
        name: str,
        *,
        fn: Any,
        args: Optional[List[Any]] = None,
        factor: Optional[Union[int, Var]] = None,
        priority: Optional[str] = None,
        use_workers: Optional[str] = None,
        group_size: Optional[int] = None,
    ) -> Node:
        """Define a node whose body is a Python function decorated with @tomii.export.

        Analogous to ``node()`` but wires to the generic Python bridge plugin
        instead of a Rust/C dylib function.  The Python function must be decorated
        with ``@tomii.export`` (or ``@tomii.export(variadic=True)`` for sinks).

        Parameters
        ----------
        fn:
            Decorated Python callable **or** a string ``"module.fn_name"``.
        args:
            Node arguments (same semantics as ``node(args=...)``).
            Barrier dependencies (``predecessor.wait()``) may appear here and are
            passed transparently through the bridge; the bridge filters them out
            before calling the Python function.

        Example::

            import matcomp

            gen_vec = app.py_node("gen_vec", fn=matcomp.generate_vector,
                                  factor=num_nodes, args=[buf_size])
            fft = app.py_node("fft", fn=matcomp.compute_fft,
                              factor=num_nodes, args=[gen_vec.out()])
        """
        from ._export import ExportMeta, _TOMII_REGISTRY

        # Resolve fn to ExportMeta ------------------------------------------ #
        if isinstance(fn, str):
            if fn not in _TOMII_REGISTRY:
                # Try to import the module to trigger decorator registration
                parts = fn.rsplit(".", 1)
                if len(parts) == 2:
                    try:
                        importlib.import_module(parts[0])
                    except ImportError:
                        pass
            meta = _TOMII_REGISTRY.get(fn)
            if meta is None:
                raise ValueError(
                    f"py_node: '{fn}' not found in @tomii.export registry. "
                    "Is the function decorated with @tomii.export?"
                )
        elif hasattr(fn, "__tomii_export__"):
            meta: ExportMeta = fn.__tomii_export__
        else:
            raise ValueError(
                f"py_node: {fn!r} is not decorated with @tomii.export. "
                "Add @tomii.export above the function definition."
            )

        # Record the module's directory so run() can prepend it to PYTHONPATH -- #
        import sys as _sys
        module_obj = _sys.modules.get(meta.module)
        if module_obj and getattr(module_obj, "__file__", None):
            module_dir = str(Path(module_obj.__file__).resolve().parent)
            if module_dir not in self._py_module_dirs:
                self._py_module_dirs.append(module_dir)

        # Auto-register callable init (deduped by qualname) ------------------- #
        init_name = f"_py_{meta.qualname.replace('.', '_').replace('-', '_')}"
        if init_name not in self._py_callable_vars:
            callable_var = self.var(
                init_name,
                func="py_load_callable",
                args=[String(meta.module), String(meta.fn_name)],
            )
            self._py_callable_vars[meta.qualname] = callable_var
        else:
            callable_var = self._py_callable_vars[meta.qualname]

        # Build node args: [callable_ref, *user_args] ------------------------- #
        node_args: List[Any] = [callable_var] + (args or [])

        return self.node(
            name,
            func=meta.bridge,
            args=node_args,
            factor=factor,
            priority=priority,
            use_workers=use_workers,
            group_size=group_size,
        )

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
        python_plugin: bool = False,
        python_interpreter: Optional[str] = None,
    ) -> Any:
        """Compile tomii-core and the plugin library.

        Recommended: use ``func_path`` pointing at your Rust source file.
        The build system auto-generates FFI wrappers from ``#[tomii_export]``
        annotations — no need to write wrappers.rs or reg.rs by hand.

        Legacy: ``wrap_path`` + ``reg_path`` for pre-written wrapper files.

        Python bridge: pass ``python_plugin=True`` to compile the bundled PyO3
        bridge plugin instead of a custom Rust/C plugin.  Optionally set
        ``python_interpreter`` to e.g. ``"python3.13t"`` for the free-threaded
        build (Tier 3, no GIL).  Defaults to the current interpreter.

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
            python_plugin=python_plugin,
            python_interpreter=python_interpreter,
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
        """Write graph JSON to a temp file and invoke the Τομί binary.

        If ``dylib`` is omitted and ``build()`` was called, uses that dylib.
        Returns subprocess.CompletedProcess.
        """
        import os as _os
        from ._runner import run as _run
        if dylib is None:
            if self._build_result is None:
                raise RuntimeError(
                    "No dylib specified and build() has not been called. "
                    "Provide dylib= or call build() first."
                )
            dylib = self._build_result.dylib

        # Build environment for the bridge subprocess.
        # The Rust binary embeds Python via PyO3 without activating the running venv,
        # so we explicitly propagate the packages the current interpreter sees.
        import sys as _sys
        merged_env: Dict[str, str] = dict(env or {})

        # PYTHONPATH: prepend (1) dirs containing @tomii.export modules (e.g. matcomp.py)
        # and (2) the active venv's site-packages so the bridge finds the same packages
        # (numpy, etc.) as this process.  Without (2) the bridge falls back to system
        # dist-packages which may have incompatible versions.
        existing = merged_env.get("PYTHONPATH") or _os.environ.get("PYTHONPATH", "")
        extra_parts: list = list(self._py_module_dirs)
        extra_parts += [p for p in _sys.path if p and "site-packages" in p]
        if extra_parts:
            extra = _os.pathsep.join(extra_parts)
            merged_env["PYTHONPATH"] = f"{extra}{_os.pathsep}{existing}" if existing else extra

        # LD_PRELOAD libpython with RTLD_GLOBAL so Python API symbols (e.g. PyObject_SelfIter)
        # are globally visible when numpy's C extension (_multiarray_umath.so) is dlopen'd by
        # the embedded interpreter.  Without this, the bridge dylib loads libpython RTLD_LOCAL
        # and numpy's extension can't resolve Python symbols at import time.
        import ctypes.util as _cu
        _libpython = _cu.find_library(
            f"python{_sys.version_info.major}.{_sys.version_info.minor}"
        )
        if _libpython:
            _existing_preload = merged_env.get("LD_PRELOAD") or _os.environ.get("LD_PRELOAD", "")
            merged_env["LD_PRELOAD"] = (
                f"{_libpython}{_os.pathsep}{_existing_preload}"
                if _existing_preload
                else _libpython
            )

        return _run(self, dylib=dylib, env=merged_env, **kwargs)

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
        python_plugin: bool = False,
        python_interpreter: Optional[str] = None,
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
            python_plugin=python_plugin,
            python_interpreter=python_interpreter,
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
