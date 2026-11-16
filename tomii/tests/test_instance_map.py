"""Tests for the Python builder's `instance_map` support: `n.out/wait/dep(map=...)`,
`tomii.halo`/`tomii.stride`, and the validation errors they raise."""

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm
from tomii._instance_map import resolve_map
from tomii._serialize import serialize_graph, serialize_node


# --------------------------------------------------------------------------- #
# Explicit list-of-lists map
# --------------------------------------------------------------------------- #


def test_explicit_map_emits_instance_map_no_indexes() -> None:
    app = tm.Graph()
    leaves = app.node("leaves", func="noop", factor=4)
    fuse = app.node(
        "fuse",
        func="noop",
        factor=1,
        args=[leaves.wait(map=[[0, 1, 2, 3]])],
    )
    emitted = serialize_node(fuse)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[0, 1, 2, 3]]
    assert "indexes" not in pred
    assert "group_by" not in pred


def test_explicit_map_on_dep_arg() -> None:
    app = tm.Graph()
    p = app.node("p", func="noop", factor=4)
    c = app.node("c", func="noop", factor=2, args=[p.dep(map=[[0, 1], [2, 3]])])
    emitted = serialize_node(c)
    pred = emitted["args"][0]["predecessor"]
    assert emitted["args"][0]["type"] == "$dep"
    assert pred["instance_map"] == [[0, 1], [2, 3]]


def test_map_mutually_exclusive_with_group_by() -> None:
    app = tm.Graph()
    p = app.node("p", func="noop", factor=4)
    with pytest.raises(ValueError, match="mutually exclusive"):
        p.wait(map=[[0]], group_by=2)


def test_map_on_res_dependency_serializes_as_instance_map() -> None:
    # $res + instance_map is supported (value-carrying mapped edges): `.out(map=...)`
    # serializes to a `$res` arg with an `instance_map` predecessor, same shape as
    # `.wait`/`.dep`, no `indexes`/`group_by`.
    app = tm.Graph()
    p = app.node("p", func="noop", factor=4)
    c = app.node("c", func="noop", factor=2, args=[p.out(map=[[0, 1], [2, 3]])])
    emitted = serialize_node(c)
    assert emitted["args"][0]["type"] == "$res"
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[0, 1], [2, 3]]
    assert "indexes" not in pred
    assert "group_by" not in pred


def test_map_still_mutually_exclusive_with_group_by_on_res() -> None:
    app = tm.Graph()
    p = app.node("p", func="noop", factor=4)
    with pytest.raises(ValueError, match="mutually exclusive"):
        p.out(map=[[0]], group_by=2)


def test_map_on_res_with_halo_callable_resolves_before_serializing() -> None:
    # The callable-map path (`tomii.halo`/`tomii.stride`, resolved once the
    # consumer's factor is known — see `_resolved_instance_map`) must work for
    # `.out(map=...)` exactly as it already does for `.wait`/`.dep`.
    app = tm.Graph()
    p = app.node("p", func="noop", factor=8)
    c = app.node("c", func="noop", factor=8, args=[p.out(map=tm.halo(1, wrap=True))])
    emitted = serialize_node(c)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"][0] == [7, 0, 1]
    assert pred["instance_map"][7] == [6, 7, 0]


def test_map_on_res_non_uniform_length_serializes_unconditionally() -> None:
    # Length 0 and length > 1 entries both serialize cleanly for `$res` — the
    # Rust side accepts any length (see `instance_map.rs`'s module docs); the
    # Python builder never validated map-entry length either.
    app = tm.Graph()
    p = app.node("p", func="noop", factor=4)
    c = app.node("c", func="noop", factor=3, args=[p.out(map=[[], [0, 1], [2, 3]])])
    emitted = serialize_node(c)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[], [0, 1], [2, 3]]


# --------------------------------------------------------------------------- #
# tomii.halo / tomii.stride
# --------------------------------------------------------------------------- #


def test_halo_resolves_wrapped_neighbourhood() -> None:
    assert resolve_map(tm.halo(1, wrap=True), consumer_factor=8, producer_factor=8) == [
        [7, 0, 1],
        [0, 1, 2],
        [1, 2, 3],
        [2, 3, 4],
        [3, 4, 5],
        [4, 5, 6],
        [5, 6, 7],
        [6, 7, 0],
    ]


def test_halo_clamped_without_wrap() -> None:
    out = resolve_map(tm.halo(1, wrap=False), consumer_factor=4, producer_factor=4)
    assert out[0] == [0, 1]  # no left neighbour
    assert out[-1] == [2, 3]  # no right neighbour
    assert out[1] == [0, 1, 2]


def test_halo_radius_zero_is_self_only() -> None:
    out = resolve_map(tm.halo(0, wrap=True), consumer_factor=5, producer_factor=5)
    assert out == [[0], [1], [2], [3], [4]]


def test_halo_negative_radius_rejected() -> None:
    with pytest.raises(ValueError):
        tm.halo(-1)


def test_stride_resolves_single_producer_per_consumer() -> None:
    assert resolve_map(tm.stride(4), consumer_factor=4, producer_factor=16) == [
        [0],
        [4],
        [8],
        [12],
    ]


def test_stride_rejects_non_positive_k() -> None:
    with pytest.raises(ValueError):
        tm.stride(0)


def test_halo_end_to_end_json_emission() -> None:
    app = tm.Graph()
    n = 8
    tile = app.node("tile", func="noop", factor=n)
    halo_step = app.node(
        "halo_step",
        func="noop",
        factor=n,
        args=[tile.wait(map=tm.halo(1, wrap=True))],
    )
    emitted = serialize_node(halo_step)
    pred = emitted["args"][0]["predecessor"]
    instance_map = pred["instance_map"]
    assert len(instance_map) == n
    assert instance_map[0] == [n - 1, 0, 1]
    assert instance_map[-1] == [n - 2, n - 1, 0]


def test_stride_end_to_end_json_emission() -> None:
    app = tm.Graph()
    fine = app.node("fine", func="noop", factor=16)
    coarse = app.node(
        "coarse",
        func="noop",
        factor=4,
        args=[fine.wait(map=tm.stride(4))],
    )
    emitted = serialize_node(coarse)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[0], [4], [8], [12]]


# --------------------------------------------------------------------------- #
# Unresolvable (Var) factor requires an explicit list
# --------------------------------------------------------------------------- #


def test_callable_map_with_var_consumer_factor_requires_explicit_list() -> None:
    app = tm.Graph()
    n = app.var("n", 8)
    tile = app.node("tile", func="noop", factor=8)
    halo_step = app.node(
        "halo_step",
        func="noop",
        factor=n,  # Var, not a literal int — unresolvable at build time
        args=[tile.wait(map=tm.halo(1))],
    )
    with pytest.raises(ValueError, match="literal int"):
        serialize_node(halo_step)


def test_callable_map_with_var_producer_factor_requires_explicit_list() -> None:
    app = tm.Graph()
    n = app.var("n", 8)
    tile = app.node("tile", func="noop", factor=n)  # producer factor is a Var
    halo_step = app.node(
        "halo_step",
        func="noop",
        factor=8,
        args=[tile.wait(map=tm.halo(1))],
    )
    with pytest.raises(ValueError, match="literal int"):
        serialize_node(halo_step)


def test_explicit_list_map_works_even_with_var_factor() -> None:
    # A literal list of lists never needs to resolve a callable, so a Var
    # factor on either side is fine.
    app = tm.Graph()
    n = app.var("n", 4)
    p = app.node("p", func="noop", factor=n)
    c = app.node("c", func="noop", factor=n, args=[p.wait(map=[[0], [1], [2], [3]])])
    emitted = serialize_node(c)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[0], [1], [2], [3]]


# --------------------------------------------------------------------------- #
# Full-graph round trip (serialize_graph) sanity check
# --------------------------------------------------------------------------- #


def test_irregular_tree_graph_serializes_cleanly() -> None:
    app = tm.Graph()
    leaves = app.node("leaves", func="noop", factor=8)
    level1 = app.node(
        "level1",
        func="noop",
        factor=2,
        args=[leaves.wait(map=[[0, 1, 2], [3, 4]])],
    )
    fuse_mixed = app.node(
        "fuse_mixed",
        func="noop",
        factor=1,
        args=[
            leaves.dep(map=[[5]]),
            level1.wait(map=[[0]]),
        ],
    )
    root = app.node(
        "root",
        func="noop",
        factor=1,
        args=[
            level1.dep(map=[[1]]),
            fuse_mixed.wait(map=[[0]]),
        ],
    )
    graph = serialize_graph(app)
    names = {n["name"] for n in graph["nodes"]}
    assert names == {"leaves", "level1", "fuse_mixed", "root"}
    fuse_json = next(n for n in graph["nodes"] if n["name"] == "fuse_mixed")
    assert fuse_json["args"][0]["predecessor"]["instance_map"] == [[5]]
    assert fuse_json["args"][1]["predecessor"]["instance_map"] == [[0]]


# --------------------------------------------------------------------------- #
# Item 2: mixed mapped + plain args on the SAME predecessor. The Python
# builder places no restriction here either (each `.wait/.dep/.out(...)` call
# is serialized independently — see `_serialize._arg`); the two args just
# happen to name the same predecessor node. The Rust runtime's graph
# validation (`tomii_core::instance_map::validate_and_build`) is what unions
# the plain arg's per-instance contribution into the mapped forward map — see
# tomii-core/tests/instance_map.rs's `mixed_same_pred_*` tests for the
# end-to-end runtime behaviour this round-trips into.
# --------------------------------------------------------------------------- #


def test_mixed_mapped_and_plain_dep_on_same_predecessor_serializes_cleanly() -> None:
    """The exact QEC-like shape: `level` reads `leaves` through a mapped
    `$barrier` (irregular groups) AND a plain `$dep` (a single "halo" leaf
    every level instance also needs)."""
    app = tm.Graph()
    leaves = app.node("leaves", func="noop", factor=6)
    level = app.node(
        "level",
        func="noop",
        factor=2,
        args=[
            leaves.wait(map=[[0, 1, 2], [3, 4]]),
            leaves.dep(5),
        ],
    )
    emitted = serialize_node(level)
    mapped_pred = emitted["args"][0]["predecessor"]
    plain_pred = emitted["args"][1]["predecessor"]
    assert emitted["args"][0]["type"] == "$barrier"
    assert mapped_pred["name"] == "leaves"
    assert mapped_pred["instance_map"] == [[0, 1, 2], [3, 4]]
    assert emitted["args"][1]["type"] == "$dep"
    assert plain_pred["name"] == "leaves"
    assert plain_pred["indexes"] == "5"
    assert "instance_map" not in plain_pred


def test_mixed_mapped_and_plain_res_on_same_predecessor_serializes_cleanly() -> None:
    """A mapped `$barrier` (irregular per-instance gating) coexisting with a
    plain 1:1 `$res` on the SAME predecessor — the plain arg still delivers a
    VALUE (unaffected by routing this predecessor through `instance_map`'s
    per-instance mechanism), while also contributing to the gating."""
    app = tm.Graph()
    leaves = app.node("leaves", func="noop", factor=4)
    level = app.node(
        "level",
        func="noop",
        factor=4,
        args=[
            leaves.wait(map=[[1], [], [0], [3]]),
            leaves.out(0),
        ],
    )
    emitted = serialize_node(level)
    assert emitted["args"][0]["type"] == "$barrier"
    assert emitted["args"][0]["predecessor"]["instance_map"] == [[1], [], [0], [3]]
    assert emitted["args"][1]["type"] == "$res"
    assert emitted["args"][1]["predecessor"]["indexes"] == "0"
    graph = serialize_graph(app)
    assert {n["name"] for n in graph["nodes"]} == {"leaves", "level"}


# --------------------------------------------------------------------------- #
# Zero-threshold instances: an empty map entry (no other predecessor
# contributing) gives that consumer instance a total dependency threshold of
# 0. The Python builder places no restriction on this — it is accepted at the
# Rust runtime's graph-validation step (`tomii_core::instance_map`), where the
# instance is dispatched once per frame at admission, like a root. See
# tomii-core/tests/instance_map.rs's `mixed_zero_threshold_*`/
# `all_zero_threshold_*` tests for the end-to-end runtime behaviour.
# --------------------------------------------------------------------------- #


def test_empty_map_entry_serializes_cleanly_as_a_zero_threshold_instance() -> None:
    app = tm.Graph()
    leaves = app.node("leaves", func="noop", factor=4)
    # cons[1]'s map entry is empty — a zero-threshold instance; cons[0] and
    # cons[2] keep ordinary nonzero thresholds.
    cons = app.node(
        "cons",
        func="noop",
        factor=3,
        args=[leaves.wait(map=[[0, 1], [], [2, 3]])],
    )
    emitted = serialize_node(cons)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[0, 1], [], [2, 3]]

    graph = serialize_graph(app)
    assert {n["name"] for n in graph["nodes"]} == {"leaves", "cons"}


def test_all_empty_map_entries_serialize_cleanly() -> None:
    app = tm.Graph()
    leaves = app.node("leaves", func="noop", factor=1)
    # Every instance of `orphan` has an empty map entry — the whole node is
    # zero-threshold, behaving like a root despite declaring a predecessor.
    orphan = app.node(
        "orphan",
        func="noop",
        factor=2,
        args=[leaves.dep(map=[[], []])],
    )
    emitted = serialize_node(orphan)
    pred = emitted["args"][0]["predecessor"]
    assert pred["instance_map"] == [[], []]
