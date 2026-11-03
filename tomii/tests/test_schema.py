"""Tests for the `python -m tomii --schema` document (tomii/_schema.py).

The schema is meant to teach an agent or user the real graph-construction
API. Every code sample under an "example" key must therefore actually run
against that API and produce a buildable graph — otherwise the schema rots
out of sync with tomii/_graph.py, tomii/_node.py, tomii/_types.py the first
time one of them changes shape (exactly what happened before this test
existed: stale `('dep', node)` tuple syntax and an int `priority`).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm
from tomii._schema import graph_schema


def _collect_examples(node: Any, path: str = "") -> "list[tuple[str, str]]":
    """Recursively collect every (path, code) pair stored under an "example" key.

    Only string values are treated as executable code samples — non-string
    "example" values (e.g. the plain int `8` under node.factor.example) are
    illustrative scalars, not snippets, and are skipped.
    """
    found: "list[tuple[str, str]]" = []
    if isinstance(node, dict):
        for key, value in node.items():
            sub_path = f"{path}.{key}" if path else key
            if key == "example" and isinstance(value, str):
                found.append((sub_path, value))
            else:
                found.extend(_collect_examples(value, sub_path))
    elif isinstance(node, list):
        for i, value in enumerate(node):
            found.extend(_collect_examples(value, f"{path}[{i}]"))
    return found


SCHEMA = graph_schema()
EXAMPLES = _collect_examples(SCHEMA)


def test_schema_has_at_least_one_example() -> None:
    assert len(EXAMPLES) >= 5, "expected several executable examples in the schema"


@pytest.mark.parametrize(
    "path,code", EXAMPLES, ids=[p for p, _ in EXAMPLES] or ["none"]
)
def test_example_builds_and_serializes(path: str, code: str) -> None:
    """Every schema example must exec cleanly and produce a Graph that serializes."""
    namespace: "dict[str, Any]" = {}
    exec(compile(code, f"<schema:{path}>", "exec"), namespace)  # noqa: S102

    assert "app" in namespace, (
        f"example at {path!r} must build a Graph named 'app':\n{code}"
    )
    app = namespace["app"]
    assert isinstance(app, tm.Graph), f"example at {path!r}: 'app' is not a tomii.Graph"

    # Must serialize without raising, and produce valid, non-trivial JSON.
    parsed = json.loads(app.to_json())
    assert "nodes" in parsed


def test_schema_is_json_serializable() -> None:
    """The schema itself (not just its examples) must round-trip through JSON,
    since `python -m tomii --schema` does exactly that."""
    json.loads(json.dumps(SCHEMA))


def test_schema_documents_dependency_methods_not_tuples() -> None:
    """Regression: the schema used to teach ('dep', node) tuple syntax, which
    raises TypeError against the real API (see tomii/_types.py infer_type)."""
    dep_methods = SCHEMA["dependency_methods"]
    for key in ("out", "dep", "wait"):
        assert key in dep_methods
    overview = SCHEMA["overview"]
    assert "('dep'" not in overview
    assert "('barrier'" not in overview


def test_schema_priority_is_a_string() -> None:
    priority = SCHEMA["graph_construction"]["node"]["parameters"]["priority"]
    assert priority["type"] == "str"
    assert "high" in priority["description"]


def test_schema_documents_post_node() -> None:
    assert "post_node" in SCHEMA["graph_construction"]


def test_schema_documents_range_end_rule() -> None:
    indexing = SCHEMA["dependency_methods"]["indexing"]["description"]
    assert "inclusive" in indexing.lower()
    assert "exclusive" in indexing.lower()


def test_schema_documents_collect_all_must_be_last() -> None:
    assert "collect_all_must_be_last_arg" in SCHEMA["dependency_methods"]
