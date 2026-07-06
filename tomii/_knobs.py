"""Knob-space generation: a versioned, machine-readable search space for any graph.

This module turns two knob sources into ONE self-describing search space:

1. **Runtime knobs** — the CLI flag catalog from `list_knobs_json()`
   (`_runner.py`), filtered to `role == "perf"` knobs that carry a value domain.
2. **Graph knobs** — extracted from the graph itself:
   - an initialization referenced by a node's string `factor` becomes one
     shared knob on the init's value (all referencing nodes stay consistent);
   - a literal integer node `factor` becomes a per-node knob;
   - a literal integer `group_by` on a predecessor arg becomes a per-arg knob
     whose domain is the divisors of the predecessor's factor.

Any optimizer (random, grid, Optuna, LLM) can consume the space through the
adapters below — no hand-written sweep spec per workload.  Graph knobs may
violate workload invariants the generator cannot know; that is by design:
every trial must be gated by the workload's verifier, and invalid edits are
logged as rejections, never silently accepted.

Typical use::

    import tomii
    space = tomii.knob_space("graph.json", workload="my-workload")
    values = tomii.knobs.sample(space, random.Random(42))
    cli_kwargs, graph_edits = tomii.knobs.split(space, values)
    patched = tomii.knobs.apply_graph_edits(graph_dict, graph_edits)
"""

from __future__ import annotations

import copy
import itertools
import json
import random
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from ._runner import list_knobs_json

SPACE_VERSION = 2

# Multiplier ladder used for factor-like graph knobs: base/4 … base*4,
# keeping only integer values >= 1.
_FACTOR_MULTIPLIERS = (0.25, 0.5, 1.0, 2.0, 4.0)

# Invariants no optimizer may violate regardless of knob values.  Shipped in
# every generated space so LLM-driven arms see them alongside the domains.
_FORBIDDEN = [
    "removing $barrier args",
    "removing $dep/$res args",
    "changing node function names",
    "adding/removing nodes",
    "editing anything not named by a knob in this space",
]


# --------------------------------------------------------------------------- #
# Space generation
# --------------------------------------------------------------------------- #


def _load_graph(graph: Any) -> Optional[Dict[str, Any]]:
    """Normalize a Graph object / dict / JSON path into a parsed graph dict."""
    if graph is None:
        return None
    if isinstance(graph, dict):
        return graph
    if isinstance(graph, (str, Path)):
        return dict(json.loads(Path(graph).read_text(encoding="utf-8")))
    to_json = getattr(graph, "to_json", None)
    if callable(to_json):
        return dict(json.loads(to_json()))
    raise TypeError(
        f"graph must be a Graph, dict, or path to a graph JSON; got {type(graph)!r}"
    )


def _factor_ladder(base: int) -> List[int]:
    """Integer multiplier ladder around `base` (base/4 … base*4, >= 1)."""
    values = set()
    for mult in _FACTOR_MULTIPLIERS:
        scaled = base * mult
        if scaled >= 1 and float(scaled).is_integer():
            values.add(int(scaled))
    return sorted(values)


def _divisors(n: int) -> List[int]:
    return [d for d in range(1, n + 1) if n % d == 0]


def _init_int_value(init: Dict[str, Any]) -> Optional[int]:
    """Return the integer value of a literal single-arg initialization, if any."""
    args = init.get("args", [])
    if init.get("function") is not None or len(args) != 1:
        return None
    arg = args[0]
    if arg.get("type") not in ("usize", "u32", "u64", "i32", "i64", "int"):
        return None
    try:
        return int(arg["value"])
    except (KeyError, TypeError, ValueError):
        return None


def _resolve_factor_base(
    node: Dict[str, Any], init_values: Dict[str, int]
) -> Optional[int]:
    """Resolve a node's factor to its integer base (literal or via init name)."""
    factor = node.get("factor")
    if factor is None:
        return None
    if isinstance(factor, int):
        return factor
    if isinstance(factor, str):
        return init_values.get(factor)
    return None


def _graph_knobs(graph_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract factor / group_by knobs from a parsed graph dict."""
    knobs: List[Dict[str, Any]] = []
    nodes = graph_dict.get("nodes", []) + graph_dict.get("post_nodes", [])

    init_values: Dict[str, int] = {}
    for init in graph_dict.get("initializations", []):
        value = _init_int_value(init)
        if value is not None:
            init_values[init["name"]] = value

    # Initializations referenced as a factor by >= 1 node: one shared knob each.
    factor_inits: Dict[str, List[str]] = {}
    for node in nodes:
        factor = node.get("factor")
        if isinstance(factor, str) and factor in init_values:
            factor_inits.setdefault(factor, []).append(node.get("name", "?"))

    for init_name, used_by in sorted(factor_inits.items()):
        base = init_values[init_name]
        knobs.append(
            {
                "name": f"graph:init.{init_name}",
                "kind": "graph",
                "type": "int",
                "path": ["initializations", init_name, "value"],
                "domain": {"kind": "choice", "values": _factor_ladder(base)},
                "base": base,
                "used_by": sorted(set(used_by)),
                "risk": "may violate workload invariants the generator cannot "
                "know (e.g. derived sizes); every trial must be verifier-gated",
                "description": f"shared factor variable '{init_name}' "
                f"(parallel instances of {len(set(used_by))} node(s))",
            }
        )

    # Literal integer factors: one per-node knob each.
    for node in nodes:
        factor = node.get("factor")
        node_name = node.get("name", "?")
        if isinstance(factor, int):
            knobs.append(
                {
                    "name": f"graph:node.{node_name}.factor",
                    "kind": "graph",
                    "type": "int",
                    "path": ["node", node_name, "factor"],
                    "domain": {"kind": "choice", "values": _factor_ladder(factor)},
                    "base": factor,
                    "risk": "changes this node's data partitioning; verifier-gated",
                    "description": f"parallel instances of node '{node_name}'",
                }
            )

        # Literal group_by on predecessor args: domain = divisors of the
        # predecessor's factor (grouping only makes sense in whole divisions).
        for arg_idx, arg in enumerate(node.get("args", [])):
            pred = arg.get("predecessor") if isinstance(arg, dict) else None
            if not isinstance(pred, dict):
                continue
            group_by = pred.get("group_by")
            if not isinstance(group_by, int):
                continue
            pred_node = next(
                (n for n in nodes if n.get("name") == pred.get("name")), None
            )
            pred_base = (
                _resolve_factor_base(pred_node, init_values) if pred_node else None
            )
            domain_values = _divisors(pred_base) if pred_base else [group_by]
            knobs.append(
                {
                    "name": f"graph:node.{node_name}.arg{arg_idx}.group_by",
                    "kind": "graph",
                    "type": "int",
                    "path": ["node", node_name, "args", arg_idx, "group_by"],
                    "domain": {"kind": "choice", "values": domain_values},
                    "base": group_by,
                    "risk": "changes result grouping width; verifier-gated",
                    "description": f"group width of '{node_name}' arg {arg_idx} "
                    f"(predecessor '{pred.get('name')}')",
                }
            )

    return knobs


def knob_space(
    graph: Any = None,
    *,
    workload: Optional[str] = None,
    include_graph_knobs: bool = True,
) -> Dict[str, Any]:
    """Generate the versioned knob search space for a workload.

    Args:
        graph: Graph object, parsed graph dict, or path to a graph JSON.
               When omitted, the space contains runtime (CLI) knobs only.
        workload: Optional workload label recorded in the space.
        include_graph_knobs: Set False to restrict to runtime knobs even when
               a graph is provided.

    Returns:
        A dict with `version`, `workload`, `knobs` (each entry carries `kind`
        ("cli" or "graph"), a `domain`, and provenance fields), and
        `forbidden` — the invariants no optimizer may violate.
    """
    graph_dict = _load_graph(graph)

    has_network = bool(graph_dict.get("network_config")) if graph_dict else True
    knobs: List[Dict[str, Any]] = []
    for entry in list_knobs_json()["knobs"]:
        if entry.get("role") != "perf" or "domain" not in entry:
            continue
        if entry["name"] == "receiver_threads" and not has_network:
            continue
        knobs.append({**entry, "kind": "cli"})

    if graph_dict is not None and include_graph_knobs:
        knobs.extend(_graph_knobs(graph_dict))

    return {
        "version": SPACE_VERSION,
        "generated_by": "tomii.knob_space",
        "workload": workload,
        "knobs": knobs,
        "forbidden": list(_FORBIDDEN),
    }


# --------------------------------------------------------------------------- #
# Domain helpers and optimizer adapters
# --------------------------------------------------------------------------- #


def enumerate_domain(domain: Dict[str, Any]) -> List[Any]:
    """Materialize a domain into its concrete search points."""
    kind = domain["kind"]
    if kind == "bool":
        return [False, True]
    if kind == "choice":
        return list(domain["values"])
    if kind == "int":
        lo, hi = int(domain["min"]), int(domain["max"])
        if domain.get("scale") == "pow2":
            values = []
            v = max(lo, 1)
            while v <= hi:
                values.append(v)
                v *= 2
            if lo == 0:
                values.insert(0, 0)
            return values
        return list(range(lo, hi + 1))
    raise ValueError(f"unknown domain kind: {kind!r}")


def sample(space: Dict[str, Any], rng: random.Random) -> Dict[str, Any]:
    """Draw one uniform random configuration from the space."""
    return {
        knob["name"]: rng.choice(enumerate_domain(knob["domain"]))
        for knob in space["knobs"]
    }


def suggest_optuna(space: Dict[str, Any], trial: Any) -> Dict[str, Any]:
    """Draw one configuration from an Optuna trial (categorical per domain).

    Domains are materialized to their concrete points so the TPE sampler sees
    the same search space as the random and grid arms.
    """
    return {
        knob["name"]: trial.suggest_categorical(
            knob["name"], enumerate_domain(knob["domain"])
        )
        for knob in space["knobs"]
    }


def grid_cells(space: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    """Iterate the full cross-product grid of the space (can be large)."""
    names = [knob["name"] for knob in space["knobs"]]
    domains = [enumerate_domain(knob["domain"]) for knob in space["knobs"]]
    for combo in itertools.product(*domains):
        yield dict(zip(names, combo))


def grid_size(space: Dict[str, Any]) -> int:
    """Number of cells in the full grid."""
    size = 1
    for knob in space["knobs"]:
        size *= len(enumerate_domain(knob["domain"]))
    return size


def split(
    space: Dict[str, Any], values: Dict[str, Any]
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Split a configuration into (cli_kwargs, graph_edits).

    cli_kwargs feed `build_command` / `graph.run(**kwargs)`; graph_edits are
    `{"path": [...], "value": v}` records for `apply_graph_edits`.
    """
    by_name = {knob["name"]: knob for knob in space["knobs"]}
    cli_kwargs: Dict[str, Any] = {}
    graph_edits: List[Dict[str, Any]] = []
    for name, value in values.items():
        knob = by_name.get(name)
        if knob is None:
            raise KeyError(f"value for unknown knob {name!r}")
        if knob["kind"] == "cli":
            cli_kwargs[name] = value
        else:
            graph_edits.append({"path": knob["path"], "value": value})
    return cli_kwargs, graph_edits


def apply_graph_edits(
    graph: Union[Dict[str, Any], str, Path],
    edits: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Return a deep-copied graph dict with the given knob edits applied.

    Supported paths (as emitted by `knob_space`):
      ["initializations", <name>, "value"]        — literal init value
      ["node", <name>, "factor"]                  — literal node factor
      ["node", <name>, "args", <idx>, "group_by"] — literal arg group width
    """
    graph_dict = copy.deepcopy(_load_graph(graph))
    assert graph_dict is not None
    all_nodes = graph_dict.get("nodes", []) + graph_dict.get("post_nodes", [])

    for edit in edits:
        path, value = edit["path"], edit["value"]
        if path[0] == "initializations":
            init = next(
                (
                    i
                    for i in graph_dict.get("initializations", [])
                    if i.get("name") == path[1]
                ),
                None,
            )
            if init is None:
                raise KeyError(f"initialization {path[1]!r} not found")
            # Literal init values serialize as strings in the graph JSON.
            init["args"][0]["value"] = str(value)
        elif path[0] == "node":
            node = next((n for n in all_nodes if n.get("name") == path[1]), None)
            if node is None:
                raise KeyError(f"node {path[1]!r} not found")
            if path[2] == "factor":
                node["factor"] = int(value)
            elif path[2] == "args":
                node["args"][path[3]]["predecessor"]["group_by"] = int(value)
            else:
                raise ValueError(f"unsupported node edit path: {path}")
        else:
            raise ValueError(f"unsupported edit path: {path}")

    return graph_dict


def render_prompt(space: Dict[str, Any]) -> str:
    """Render the space as a text block for LLM-driven optimizers."""
    lines = [
        f"## Knob space (version {space['version']}"
        + (f", workload: {space['workload']}" if space.get("workload") else "")
        + ")",
        "",
        "Use ONLY the listed knobs and values.",
        "",
    ]
    for knob in space["knobs"]:
        values = enumerate_domain(knob["domain"])
        lines.append(f"- {knob['name']}: {json.dumps(values)}")
        if knob.get("description"):
            lines.append(f"    {knob['description']}")
        if knob.get("search_hint"):
            lines.append(f"    hint: {knob['search_hint']}")
        if knob.get("risk"):
            lines.append(f"    risk: {knob['risk']}")
    lines += ["", "## Forbidden edits", ""]
    lines += [f"- {item}" for item in space["forbidden"]]
    return "\n".join(lines)
