"""Tests for Phase 2 bridge-guard changes.

Covers:
  - @tomii.export raises TomiiExportError for __main__ functions
  - @tomii.export succeeds for importable-module functions
  - Graph.run() env block sets TOMII_PARENT_PYTHON, PYTHONHOME, and a
    widened PYTHONPATH that includes non-site-packages sys.path entries
"""

from __future__ import annotations

import sys
import types
import unittest.mock as mock
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import tomii
from tomii._export import TomiiExportError, _TOMII_REGISTRY


# --------------------------------------------------------------------------- #
# @tomii.export guards
# --------------------------------------------------------------------------- #


def test_export_main_raises():
    """Functions defined in __main__ must raise TomiiExportError."""

    def fake_main_fn():
        pass

    fake_main_fn.__module__ = "__main__"
    fake_main_fn.__name__ = "fake_main_fn"
    fake_main_fn.__qualname__ = "fake_main_fn"

    with pytest.raises(TomiiExportError, match="__main__"):
        tomii.export(fake_main_fn)


def test_export_main_error_message_includes_qualname():
    """Error message should mention the function's qualified name."""

    def my_func():
        pass

    my_func.__module__ = "__main__"
    my_func.__name__ = "my_func"
    my_func.__qualname__ = "MyClass.my_func"

    with pytest.raises(TomiiExportError, match="MyClass.my_func"):
        tomii.export(my_func)


def test_export_module_function_succeeds():
    """Functions from a proper module (not __main__) register without error."""

    def real_fn():
        pass

    real_fn.__module__ = "mymodule"
    real_fn.__name__ = "real_fn"
    real_fn.__qualname__ = "real_fn"

    result = tomii.export(real_fn)
    assert result is real_fn
    assert hasattr(real_fn, "__tomii_export__")
    meta = real_fn.__tomii_export__
    assert meta.module == "mymodule"
    assert meta.fn_name == "real_fn"
    assert meta.py_qualname == "real_fn"

    # Clean up registry
    _TOMII_REGISTRY.pop("mymodule.real_fn", None)


def test_export_stores_py_qualname():
    """ExportMeta.py_qualname holds f.__qualname__, not the registry key."""

    def nested():
        pass

    nested.__module__ = "somemod"
    nested.__name__ = "nested"
    nested.__qualname__ = "Outer.nested"

    tomii.export(nested)
    meta = nested.__tomii_export__
    assert meta.py_qualname == "Outer.nested"
    assert meta.fn_name == "nested"

    _TOMII_REGISTRY.pop("somemod.nested", None)


# --------------------------------------------------------------------------- #
# Graph.run() environment block
# --------------------------------------------------------------------------- #


def _make_minimal_graph_with_build_result(
    dylib: str = "/fake/bridge.so",
) -> tomii.Graph:
    """Return a Graph with a fake build result pointing at a given dylib."""
    from tomii._builder import BuildResult

    app = tomii.Graph()
    app._build_result = BuildResult(
        dylib=dylib,
        binary="/fake/main",
        python_interpreter=sys.executable,
    )
    return app


def _capture_run_env(app: tomii.Graph) -> dict:
    """Invoke app.run() with a mocked _runner.run and return the merged env."""
    captured = {}

    def fake_run(graph, *, dylib, env, **kwargs):
        captured.update(env)
        # Return a mock CompletedProcess so callers don't crash
        return mock.MagicMock()

    with (
        mock.patch(
            "tomii._graph._run"
            if hasattr(tomii._graph, "_run")
            else "tomii._runner.run",
            side_effect=fake_run,
        ),
        mock.patch("tomii._runner.run", side_effect=fake_run),
    ):
        try:
            app.run()
        except Exception:
            pass  # we only care about the env
    return captured


def test_run_env_sets_tomii_parent_python():
    app = _make_minimal_graph_with_build_result()
    env = _capture_run_env(app)
    assert env.get("TOMII_PARENT_PYTHON") == sys.executable


def test_run_env_sets_pythonhome():
    app = _make_minimal_graph_with_build_result()
    env = _capture_run_env(app)
    assert "PYTHONHOME" in env
    assert sys.prefix in env["PYTHONHOME"]


def test_run_env_pythonpath_includes_non_site_packages():
    """PYTHONPATH must include sys.path entries beyond just site-packages."""
    # Add a custom path entry that doesn't contain "site-packages"
    custom_path = "/tmp/my_custom_lib"
    original_path = sys.path[:]
    sys.path.insert(0, custom_path)

    try:
        app = _make_minimal_graph_with_build_result()
        env = _capture_run_env(app)
        pythonpath = env.get("PYTHONPATH", "")
        assert custom_path in pythonpath, (
            f"{custom_path!r} not found in PYTHONPATH={pythonpath!r}. "
            "PYTHONPATH must be a full sys.path copy, not just site-packages."
        )
    finally:
        sys.path[:] = original_path


def test_run_env_pythonpath_deduplicates():
    """Duplicate entries in sys.path must appear only once in PYTHONPATH."""
    dup_path = "/tmp/dup_entry"
    original_path = sys.path[:]
    sys.path.insert(0, dup_path)
    sys.path.insert(0, dup_path)

    try:
        app = _make_minimal_graph_with_build_result()
        env = _capture_run_env(app)
        parts = [p for p in env.get("PYTHONPATH", "").split(":") if p == dup_path]
        assert len(parts) == 1, f"Expected exactly one {dup_path!r}, got {len(parts)}"
    finally:
        sys.path[:] = original_path
