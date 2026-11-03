#!/usr/bin/env python3
"""Check a Τομί report.json and print the fields the skills use.

Usage: check_report.py report.json [--newer-than FILE_OR_EPOCH] [--expect-frames N]

Exits 1 (and says why) if the report is missing, older than the run it should describe,
reports dropped tasks, or covers fewer frames than expected. Otherwise prints a compact
JSON summary: latency percentiles, overhead diagnostic, critical path, worker spread and
the top optimization suggestions.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("report", type=Path)
    ap.add_argument("--newer-than", help="file path or UNIX time the report must be newer than")
    ap.add_argument("--expect-frames", type=int,
                    help="frames the report must cover (max_frames minus exclude_frames)")
    a = ap.parse_args()

    problems = []
    if not a.report.exists():
        print(f"INVALID: {a.report} not found (was the run started with timing= as well as report=?)")
        return 1
    if a.newer_than:
        ref = os.path.getmtime(a.newer_than) if os.path.exists(a.newer_than) else float(a.newer_than)
        if a.report.stat().st_mtime <= ref:
            problems.append("report is older than the reference: it describes an earlier run")

    r = json.loads(a.report.read_text())
    s = r.get("summary", {})
    diag = s.get("scheduling_overhead_diagnostic", {}) or {}
    cp = r.get("critical_path", {}) or {}
    busy = (r.get("resource_utilization", {}) or {}).get("worker_busy_pct") or []

    drops = s.get("stale_task_drops", 0) or 0
    if drops:
        problems.append(f"stale_task_drops={drops}: tasks were dropped, the run is not valid")
    if a.expect_frames is not None and s.get("total_frames") is not None \
            and s["total_frames"] < a.expect_frames:
        problems.append(f"total_frames={s['total_frames']} < expected {a.expect_frames}")

    out = {
        "latency_us": {k: s.get(f"{k}_latency_us") for k in ("avg", "p50", "p99", "p999")},
        "throughput_frames_per_sec": s.get("throughput_frames_per_sec"),
        "total_frames": s.get("total_frames"),
        "total_tasks_per_frame": s.get("total_tasks_per_frame"),
        "stale_task_drops": drops,
        "overhead_pct": diag.get("overhead_pct"),
        "interpretation": diag.get("interpretation"),
        "critical_path": {k: cp.get(k) for k in ("length_nodes", "max_node_factor", "estimated_latency_us")},
        "worker_busy_pct": ({"min": min(busy), "max": max(busy), "mean": round(sum(busy) / len(busy), 1),
                             "spread": round(max(busy) - min(busy), 1)} if busy else None),
        "top_nodes": [{k: n.get(k) for k in ("name", "total_exec_us", "on_critical_path")}
                      for n in (r.get("per_node") or [])[:3]],
        "suggestions": [{k: g.get(k) for k in ("priority", "category", "knob", "suggested_value", "action")}
                        for g in sorted(r.get("optimization_suggestions") or [],
                                        key=lambda g: g.get("priority", 99))[:3]],
    }
    print(("INVALID: " + "; ".join(problems)) if problems else "VALID")
    print(json.dumps(out, indent=1))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
