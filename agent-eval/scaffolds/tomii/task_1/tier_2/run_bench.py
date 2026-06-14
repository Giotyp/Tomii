"""Sensor pipeline — slow baseline for optimization.

Current settings: workers=1, batching_size=1. Your job is to make this faster.
See TASK.md for the full optimization guide.
"""

from __future__ import annotations
import argparse
import signal
from pathlib import Path

import tomii as tm  # installed via `pip install -e .` at repo root

HERE = Path(__file__).resolve().parent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=1)  # <-- slow baseline
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--max-streams", type=int, default=5)
    p.add_argument("--exclude-streams", type=int, default=2)
    p.add_argument("--batching-size", type=int, default=1)  # <-- slow baseline
    p.add_argument("--batching-limit", type=int, default=1)
    p.add_argument("--coalesce-barriers", action="store_true", default=False)
    p.add_argument("--inline-continuation", action="store_true", default=False)
    p.add_argument("--slot-priority", action="store_true", default=False)
    p.add_argument("--clean", dest="clean", action="store_true", default=False)
    p.add_argument("--build-only", action="store_true", default=False)
    p.add_argument("--report", default="report.json")
    p.add_argument("--timing", default="timing.txt")
    return p.parse_args()


NUM_SENSORS = 4  # fixed workload — do not modify
READINGS_PER_SENSOR = 64  # fixed workload — do not modify
TOTAL_READINGS = 256  # fixed workload — do not modify
ANOMALY_THRESHOLD = 5.0  # fixed workload — do not modify


def build_graph() -> tm.Graph:
    app = tm.Graph()

    num_sensors = NUM_SENSORS
    readings_per_sensor = READINGS_PER_SENSOR
    total_readings = TOTAL_READINGS
    anomaly_threshold = app.var("anomaly_threshold", tm.f64(ANOMALY_THRESHOLD))
    result_file = app.var(
        "result_file",
        func="get_out_file",
        args=[tm.String("SCRIPT_DIR"), tm.String("result.txt")],
    )

    generate = app.node(
        "generate",
        func="generate_reading",
        factor=total_readings,
        args=[anomaly_threshold],
    )

    classify = app.node(
        "classify",
        func="classify_reading",
        factor=total_readings,
        args=[generate.out(), anomaly_threshold],
    )

    cond_anomaly = tm.Condition(
        operation="Eq",
        value=True,
        value_type="bool",
        func="check_bool",
        args=[classify.out()],
    )
    app.node(
        "handle_anomaly",
        func="amplify_reading",
        factor=total_readings,
        priority="high",
        condition=cond_anomaly,
        args=[generate.out(), classify.wait()],
    )

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
        args=[generate.out(), classify.wait()],
    )

    compute_stats = app.node(
        "compute_stats",
        func="compute_sensor_stats",
        factor=num_sensors,
        args=[generate.wait(0, total_readings - 1, group_by=readings_per_sensor)],
    )

    app.node(
        "log_event",
        func="log_stream_event",
        args=[classify.dep(0, total_readings - 1)],
    )

    aggregate = app.node(
        "aggregate",
        func="aggregate_results",
        factor=num_sensors,
        args=[
            compute_stats.out(),
            classify.wait(0, total_readings - 1, group_by=readings_per_sensor),
        ],
    )

    app.node(
        "report",
        func="write_report",
        args=[result_file, aggregate.out(0, num_sensors - 1)],
    )

    app.post_node("cleanup", func="cleanup_state", args=[])

    return app


def main() -> None:
    args = _parse_args()

    for f in ["result.txt", "out.txt", args.report, args.timing]:
        Path(f).unlink(missing_ok=True)

    # result.txt must exist for the plugin's append-mode open
    (HERE / "result.txt").touch()

    app = build_graph()

    # CARGO_TARGET_DIR: route plugin output to the Tomii workspace target so
    # Tomii's _find_dylib picks up libsensor_pipeline.so (not a stale .so).
    from tomii._builder import find_workspace_root

    bench_target = str(find_workspace_root() / "target")
    env = {"SCRIPT_DIR": str(HERE), "CARGO_TARGET_DIR": bench_target}

    app.build(
        func_path=str(HERE / "src" / "lib.rs"),
        plugin_manifest=str(HERE / "Cargo.toml"),
        release=True,
        clean=args.clean,
        env=env,
    )

    if args.build_only:
        return

    # Kill the process if app.run() hangs (e.g. bad flag combination).
    def _alarm(signum, frame):
        raise SystemExit("pipeline timed out after 120s — try different knobs")

    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(120)
    try:
        app.run(
            env=env,
            workers=args.workers,
            slots=args.slots,
            max_streams=args.max_streams,
            exclude_streams=args.exclude_streams,
            batching_size=args.batching_size,
            batching_limit=args.batching_limit,
            coalesce_barriers=args.coalesce_barriers,
            inline_continuation=args.inline_continuation,
            slot_priority=args.slot_priority,
            output=str(HERE / "out.txt"),  # runtime log goes here, NOT result.txt
            report=str(HERE / args.report),
            timing=str(HERE / args.timing),
        )
    finally:
        signal.alarm(0)


if __name__ == "__main__":
    main()
