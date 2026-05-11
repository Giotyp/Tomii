#!/usr/bin/env python3
"""
Classify the dominant performance bottleneck from a report.json.

Decision tree follows SKILLS/diagnose.md:
  overhead_pct > 60%  → scheduling-dominated (graph-coarsen)
  overhead_pct < 20%  → compute-dominated (kernel optimisation)
  20-60%              → mixed (knob-search first)
  max(worker_busy) < 50%        → underutilisation
  spread(worker_busy) > 30pp    → load imbalance

Returns a dict with:
  bottleneck_class: str
  overhead_pct: float
  worker_busy_min/max/mean/spread: float
  critical_path_nodes: int
  critical_path_latency_us: float
  max_node_factor: int
  top_nodes: list[dict]
  optimization_suggestions: list[dict]
  bottleneck_hints: list[str]
  actions: list[str]   (human-readable)
"""

import json
import statistics
import sys
from pathlib import Path


def diagnose(report_path: str) -> dict:
    with open(report_path) as f:
        report = json.load(f)

    summary = report.get("summary", {})
    diag = summary.get("scheduling_overhead_diagnostic", {})
    cp = report.get("critical_path", {})
    util = report.get("resource_utilization", {})
    per_node = report.get("per_node", [])
    suggestions = report.get("optimization_suggestions", [])
    hints = report.get("bottleneck_hints", [])

    overhead_pct = diag.get("overhead_pct", 0.0) or 0.0
    worker_busy = util.get("worker_busy_pct", []) or []

    w_min = min(worker_busy) if worker_busy else 0.0
    w_max = max(worker_busy) if worker_busy else 0.0
    w_mean = statistics.mean(worker_busy) if worker_busy else 0.0
    w_spread = w_max - w_min

    cp_nodes = cp.get("length_nodes", 0) or 0
    cp_latency = cp.get("estimated_latency_us", 0.0) or 0.0
    max_factor = cp.get("max_node_factor", 1) or 1

    top_nodes = sorted(per_node, key=lambda n: n.get("total_exec_us", 0), reverse=True)[:3]

    # Decision tree
    actions = []
    if overhead_pct > 60.0:
        bottleneck = "scheduling"
        actions.append("[graph-coarsen] overhead_pct={:.1f}% > 60% — reduce task count by coarsening nodes".format(overhead_pct))
    elif overhead_pct < 20.0:
        bottleneck = "compute"
        actions.append("[kernel-opt] overhead_pct={:.1f}% < 20% — scheduling is negligible; optimise compute kernels".format(overhead_pct))
    else:
        bottleneck = "mixed"
        actions.append("[knob-search] overhead_pct={:.1f}% in 20-60% — try knob-search first".format(overhead_pct))

    if w_max < 50.0:
        bottleneck = "underutilisation"
        actions.append("[graph-restructure] max worker_busy={:.1f}% < 50% — critical path serialises execution".format(w_max))

    if w_spread > 30.0:
        if bottleneck not in ("scheduling", "underutilisation"):
            bottleneck = "load_imbalance"
        actions.append("[group-rebalance] worker_busy spread={:.1f}pp > 30pp — review group_size/barrier placement".format(w_spread))

    # Append priority-1 suggestions from report
    for s in sorted(suggestions, key=lambda x: x.get("priority", 99)):
        p = s.get("priority", "?")
        desc = s.get("description", "")
        knob = s.get("knob", "")
        val = s.get("suggested_value", "")
        speedup = s.get("estimated_speedup", "")
        actions.append(f"[priority={p}] {desc}  knob={knob} → {val}  est. speedup={speedup}")
        if p == 1:
            break  # only highest priority here; rest go in full suggestions list

    return {
        "bottleneck_class": bottleneck,
        "overhead_pct": overhead_pct,
        "worker_busy_min": w_min,
        "worker_busy_max": w_max,
        "worker_busy_mean": w_mean,
        "worker_busy_spread": w_spread,
        "critical_path_nodes": cp_nodes,
        "critical_path_latency_us": cp_latency,
        "max_node_factor": max_factor,
        "top_nodes": [{"name": n.get("name"), "total_exec_us": n.get("total_exec_us"), "on_critical_path": n.get("on_critical_path")} for n in top_nodes],
        "optimization_suggestions": suggestions,
        "bottleneck_hints": hints,
        "actions": actions,
    }


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "report.json"
    result = diagnose(path)
    print(f"Bottleneck class: {result['bottleneck_class']}")
    print(f"overhead_pct={result['overhead_pct']:.1f}%  worker_busy: min={result['worker_busy_min']:.1f}% max={result['worker_busy_max']:.1f}% spread={result['worker_busy_spread']:.1f}pp")
    print(f"critical_path: {result['critical_path_nodes']} nodes, {result['critical_path_latency_us']:.1f}us, max_factor={result['max_node_factor']}")
    print("Actions:")
    for a in result["actions"]:
        print(f"  {a}")
