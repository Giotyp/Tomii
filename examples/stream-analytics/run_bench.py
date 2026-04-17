"""Stream-analytics benchmark — Python API equivalent of graph.json.

This file builds and runs the same graph as graph.json using the tomii
Python API, demonstrating conditional nodes, grouped barriers, $dep ordering
edges, priority levels, and post-stream cleanup — all without the heavy
dependencies of a real MIMO pipeline.

Usage:
    # From repo root (activate venv first: source .venv/bin/activate)
    uv run python examples/stream-analytics/run_bench.py

    # Override key parameters without editing the file
    uv run python examples/stream-analytics/run_bench.py --workers 4 --no-clean
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent        # examples/stream-analytics/
REPO_ROOT = HERE.parents[1]                   # workspace root
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm

# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="stream-analytics Τομί benchmark")
    p.add_argument("--workers",        type=int, default=2)
    p.add_argument("--system-threads", type=int, default=3)
    p.add_argument("--slots",          type=int, default=2)
    p.add_argument("--max-streams",    type=int, default=1)
    p.add_argument("--max-runtime",    type=int, default=60)
    p.add_argument("--batching-size",  type=int, default=1)
    p.add_argument("--batching-limit", type=int, default=10)
    p.add_argument("--exclude-streams",type=int, default=0)
    p.add_argument("--no-clean",  dest="clean",  action="store_false", default=True)
    p.add_argument("--no-record", dest="record", action="store_false", default=True)
    p.add_argument("--no-inits",  dest="inits",  action="store_false", default=True)
    p.add_argument("--slot-priority",  action="store_true", default=False)
    p.add_argument("--debug",          action="store_true", default=False)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph definition (equivalent to graph.json)
# ---------------------------------------------------------------------------


def build_graph() -> tm.Graph:
    app = tm.Graph()

    # --- Initializations ---
    num_sensors         = app.var("num_sensors",         4)
    readings_per_sensor = app.var("readings_per_sensor", 8)
    total_readings      = app.var("total_readings",      32)
    anomaly_threshold   = app.var("anomaly_threshold",   tm.f64(5.0))
    result_file         = app.var(
        "result_file",
        func="get_out_file",
        args=[tm.String("SCRIPT_DIR"), tm.String("result.txt")],
    )

    # --- Stage 1: generate one reading per slot (factor = total_readings = 32) ---
    generate = app.node(
        "generate",
        func="generate_reading",
        factor=total_readings,
        args=[anomaly_threshold],
    )

    # --- Stage 2: classify each reading as anomaly (True) or normal (False) ---
    classify = app.node(
        "classify",
        func="classify_reading",
        factor=total_readings,
        args=[generate.out(), anomaly_threshold],
    )

    # --- Stage 3a: anomaly branch (high priority, conditional on classify == True) ---
    cond_anomaly = tm.Condition(
        operation="Eq",
        value=True,
        value_type="bool",
        func="check_bool",
        args=[classify.out()],        # $res(classify, 0) — 1:1 instance mapping
    )
    app.node(
        "handle_anomaly",
        func="amplify_reading",
        factor=total_readings,
        priority="high",
        condition=cond_anomaly,
        args=[
            generate.out(),           # $res(generate, 0) — reading value
            classify.wait(),          # $barrier(classify, 0) — ensure classify[i] done
        ],
    )

    # --- Stage 3b: normal branch (low priority, conditional on classify != True) ---
    cond_normal = tm.Condition(
        operation="Neq",
        value=True,
        value_type="bool",
        func="check_bool",
        args=[classify.out()],
    )
    app.node(
        "smooth",
        func="smooth_reading",
        factor=total_readings,
        priority="low",
        condition=cond_normal,
        args=[
            generate.out(),           # $res(generate, 0)
            classify.wait(),          # $barrier(classify, 0)
        ],
    )

    # --- Stage 4a: per-sensor statistics (grouped barrier on generate) ---
    # group_by=readings_per_sensor splits the total_readings barrier into
    # num_sensors independent groups — compute_stats[i] fires when
    # generate[i*8 .. (i+1)*8] all complete.
    compute_stats = app.node(
        "compute_stats",
        func="compute_sensor_stats",
        factor=num_sensors,
        args=[
            generate.wait(0, total_readings, group_by=readings_per_sensor),
        ],
    )

    # --- Stage 4b: ordering-only log ($dep — no data consumed) ---
    # log_event fires once after ALL classify instances complete, but
    # receives no classify results (CmTypes::None in each dep slot).
    app.node(
        "log_event",
        func="log_stream_event",
        args=[classify.dep(0, total_readings)],
    )

    # --- Stage 5: aggregate per-sensor results (1:1 from compute_stats + grouped barrier on classify) ---
    aggregate = app.node(
        "aggregate",
        func="aggregate_results",
        factor=num_sensors,
        args=[
            compute_stats.out(),      # $res(compute_stats, 0) — 1:1 stats value
            classify.wait(0, total_readings, group_by=readings_per_sensor),
        ],
    )

    # --- Stage 6: variadic fan-in — collect all num_sensors aggregate results ---
    app.node(
        "report",
        func="write_report",
        args=[result_file, aggregate.out(0, num_sensors)],
    )

    # --- Post-stream cleanup ---
    app.post_node("cleanup", func="cleanup_state", args=[])

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    out_file    = HERE / "out.txt"
    timing_file = HERE / "timing.txt"
    report_file = HERE / "report.txt"
    out_file.unlink(missing_ok=True)
    timing_file.unlink(missing_ok=True)
    report_file.unlink(missing_ok=True)

    app = build_graph()

    env = {"SCRIPT_DIR": str(HERE)}

    app.build(
        func_path=str(HERE / "src" / "lib.rs"),
        plugin_manifest=str(HERE / "Cargo.toml"),
        release=True,
        clean=args.clean,
        env=env,
    )

    app.run(
        env=env,
        workers=args.workers,
        system_threads=args.system_threads,
        slots=args.slots,
        max_streams=args.max_streams,
        max_runtime=args.max_runtime,
        batching_size=args.batching_size,
        batching_limit=args.batching_limit,
        exclude_streams=args.exclude_streams,
        output=str(out_file),
        timing=str(timing_file),
        record=args.record,
        inits=args.inits,
        slot_priority=args.slot_priority,
        debug=args.debug,
        report=str(report_file),
    )


if __name__ == "__main__":
    main()
