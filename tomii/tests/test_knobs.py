"""Tests for the knob-space generator and its optimizer adapters."""

from __future__ import annotations

import random
from typing import Any

import pytest

from tomii._knobs import (
    apply_graph_edits,
    enumerate_domain,
    grid_cells,
    grid_size,
    knob_space,
    render_prompt,
    sample,
    split,
    suggest_optuna,
)
from tomii._runner import list_knobs_json


# A minimal graph exercising all three graph-knob shapes: a Var-named factor
# (shared init knob), a literal int factor (per-node knob), and a literal
# group_by on a predecessor arg (divisor-domain knob).
TOY_GRAPH: dict[str, Any] = {
    "initializations": [
        {"name": "n_items", "args": [{"type": "usize", "value": "16"}]},
        {"name": "threshold", "args": [{"type": "f64", "value": "5.0"}]},
    ],
    "nodes": [
        {
            "name": "produce",
            "factor": "n_items",
            "function": "produce",
            "args": [],
        },
        {
            "name": "transform",
            "factor": 8,
            "function": "transform",
            "args": [
                {
                    "type": "$res",
                    "predecessor": {"name": "produce", "indexes": "0", "group_by": 4},
                },
                {"type": "$ref", "value": "threshold"},
            ],
        },
    ],
    "post_nodes": [],
}


# --------------------------------------------------------------------------- #
# Catalog
# --------------------------------------------------------------------------- #


def test_catalog_is_versioned_and_carries_domains() -> None:
    catalog = list_knobs_json()
    assert catalog["version"] == 3
    by_name = {k["name"]: k for k in catalog["knobs"]}
    for name in ("workers", "slots", "batching_size"):
        assert by_name[name]["role"] == "perf"
        assert by_name[name]["domain"]["kind"] == "int"
    assert by_name["fifo"]["domain"] == {"kind": "bool"}
    assert by_name["max_frames"]["role"] == "measurement"
    assert by_name["output"]["role"] == "io"
    assert by_name["workers"]["domain"]["max"] >= 1


# --------------------------------------------------------------------------- #
# Space generation
# --------------------------------------------------------------------------- #


def test_cli_only_space_has_no_graph_knobs() -> None:
    space = knob_space()
    assert space["version"] == 3
    assert all(k["kind"] == "cli" for k in space["knobs"])
    assert all(k["role"] == "perf" for k in space["knobs"])
    assert space["forbidden"]


def test_graph_knob_extraction() -> None:
    space = knob_space(TOY_GRAPH, workload="toy")
    assert space["workload"] == "toy"
    by_name = {k["name"]: k for k in space["knobs"]}

    # Var-named factor -> shared init knob with multiplier ladder around 16.
    init_knob = by_name["graph:init.n_items"]
    assert init_knob["kind"] == "graph"
    assert init_knob["base"] == 16
    assert init_knob["domain"]["values"] == [4, 8, 16, 32, 64]
    assert init_knob["used_by"] == ["produce"]

    # Non-factor init must NOT become a knob.
    assert "graph:init.threshold" not in by_name

    # Literal factor -> per-node knob, ladder around 8.
    factor_knob = by_name["graph:node.transform.factor"]
    assert factor_knob["domain"]["values"] == [2, 4, 8, 16, 32]

    # Literal group_by -> divisors of the predecessor's factor (16).
    gb_knob = by_name["graph:node.transform.arg0.group_by"]
    assert gb_knob["domain"]["values"] == [1, 2, 4, 8, 16]
    assert gb_knob["base"] == 4


def test_graph_knobs_can_be_disabled() -> None:
    space = knob_space(TOY_GRAPH, include_graph_knobs=False)
    assert all(k["kind"] == "cli" for k in space["knobs"])


def test_receiver_threads_excluded_without_network() -> None:
    space = knob_space(TOY_GRAPH)
    assert "receiver_threads" not in {k["name"] for k in space["knobs"]}
    # CLI-only space (no graph) keeps it — caller may target a network graph.
    cli_space = knob_space()
    assert "receiver_threads" in {k["name"] for k in cli_space["knobs"]}


# --------------------------------------------------------------------------- #
# Domains and adapters
# --------------------------------------------------------------------------- #


def test_enumerate_domain_shapes() -> None:
    assert enumerate_domain({"kind": "bool"}) == [False, True]
    assert enumerate_domain({"kind": "choice", "values": [3, 5]}) == [3, 5]
    assert enumerate_domain({"kind": "int", "min": 1, "max": 64, "scale": "pow2"}) == [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
    ]
    assert enumerate_domain({"kind": "int", "min": 0, "max": 4, "scale": "pow2"}) == [
        0,
        1,
        2,
        4,
    ]
    assert enumerate_domain({"kind": "int", "min": 1, "max": 4, "scale": "linear"}) == [
        1,
        2,
        3,
        4,
    ]


def test_sample_stays_in_domain_and_is_deterministic() -> None:
    space = knob_space(TOY_GRAPH)
    domains = {k["name"]: set(enumerate_domain(k["domain"])) for k in space["knobs"]}
    values_a = sample(space, random.Random(7))
    values_b = sample(space, random.Random(7))
    assert values_a == values_b
    assert set(values_a) == set(domains)
    for _ in range(100):
        for name, value in sample(space, random.Random()).items():
            assert value in domains[name]


def test_split_and_apply_graph_edits() -> None:
    space = knob_space(TOY_GRAPH)
    values = sample(space, random.Random(0))
    values["graph:init.n_items"] = 32
    values["graph:node.transform.factor"] = 16
    values["graph:node.transform.arg0.group_by"] = 8
    values["workers"] = 4

    cli_kwargs, graph_edits = split(space, values)
    assert cli_kwargs["workers"] == 4
    assert all(not name.startswith("graph:") for name in cli_kwargs)
    assert len(graph_edits) == 3

    patched = apply_graph_edits(TOY_GRAPH, graph_edits)
    assert patched["initializations"][0]["args"][0]["value"] == "32"
    assert patched["nodes"][1]["factor"] == 16
    assert patched["nodes"][1]["args"][0]["predecessor"]["group_by"] == 8
    # Original untouched.
    assert TOY_GRAPH["initializations"][0]["args"][0]["value"] == "16"
    assert TOY_GRAPH["nodes"][1]["factor"] == 8


def test_split_rejects_unknown_knob() -> None:
    space = knob_space()
    with pytest.raises(KeyError):
        split(space, {"not_a_knob": 1})


def test_grid_cells_cover_the_space() -> None:
    space = knob_space(TOY_GRAPH, include_graph_knobs=False)
    size = grid_size(space)
    cells = list(grid_cells(space))
    assert len(cells) == size
    assert len({tuple(sorted(c.items())) for c in cells}) == size


def test_render_prompt_lists_all_knobs_and_forbidden() -> None:
    space = knob_space(TOY_GRAPH, workload="toy")
    text = render_prompt(space)
    for knob in space["knobs"]:
        assert knob["name"] in text
    for item in space["forbidden"]:
        assert item in text


def test_suggest_optuna_matches_domains() -> None:
    optuna = pytest.importorskip("optuna")
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    space = knob_space(TOY_GRAPH)
    domains = {k["name"]: set(enumerate_domain(k["domain"])) for k in space["knobs"]}
    seen = {}

    def objective(trial: Any) -> float:
        values = suggest_optuna(space, trial)
        seen.update(values)
        for name, value in values.items():
            assert value in domains[name]
        return 0.0

    study = optuna.create_study(sampler=optuna.samplers.RandomSampler(seed=1))
    study.optimize(objective, n_trials=3)
    assert set(seen) == set(domains)
