"""Matrix-compute benchmark — Python equivalent of run_bench.sh.

Replaces the shell script + graph.json pair with a single Python file using
the tomii API. All runtime parameters match run_bench.sh defaults.

Usage:
    # From repo root (activate venv first: source .venv/bin/activate)
    python examples/matrix-compute/run_bench.py

    # Override key parameters without editing the file
    python examples/matrix-compute/run_bench.py --workers 4 --no-clean
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent  # examples/matrix-compute/
REPO_ROOT = HERE.parents[1]  # workspace root
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm

# ---------------------------------------------------------------------------
# CLI arguments (mirrors the config block at the top of run_bench.sh)
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="matrix-compute Τομί benchmark")
    p.add_argument("--workers", type=int, default=2, help="worker threads (default: 2)")
    p.add_argument(
        "--system-threads", type=int, default=3, help="system threads (default: 3)"
    )
    p.add_argument(
        "--slots", type=int, default=2, help="concurrent stream slots (default: 2)"
    )
    p.add_argument(
        "--max-streams", type=int, default=1, help="max concurrent streams (default: 1)"
    )
    p.add_argument(
        "--max-runtime",
        type=int,
        default=60,
        help="max runtime in seconds (default: 60)",
    )
    p.add_argument(
        "--batching-size", type=int, default=1, help="batch size (default: 1)"
    )
    p.add_argument(
        "--batching-limit", type=int, default=10, help="batch limit (default: 10)"
    )
    p.add_argument(
        "--exclude-streams",
        type=int,
        default=0,
        help="streams excluded from timing stats (default: 0)",
    )
    p.add_argument(
        "--no-clean",
        dest="clean",
        action="store_false",
        default=True,
        help="skip cargo clean before build (faster rebuilds)",
    )
    p.add_argument(
        "--no-record",
        dest="record",
        action="store_false",
        default=True,
        help="disable scheduler recording",
    )
    p.add_argument(
        "--no-inits",
        dest="inits",
        action="store_false",
        default=True,
        help="disable initialization printing",
    )
    p.add_argument(
        "--slot-priority",
        action="store_true",
        default=False,
        help="enable slot-priority scheduler",
    )
    p.add_argument(
        "--debug", action="store_true", default=False, help="enable debug output"
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph definition (equivalent to graph.json)
# ---------------------------------------------------------------------------


def build_graph() -> tm.Graph:
    app = tm.Graph()

    # --- Initializations ---
    buf_size = app.var("buf_size", 100)
    num_nodes = app.var("num_nodes", 200)
    fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])
    result_file = app.var(
        "result_file",
        func="get_out_file",
        # SCRIPT_DIR is resolved at runtime from the env var passed to run()
        args=[tm.String("SCRIPT_DIR"), tm.String("result.txt")],
    )

    # --- Pipeline ---
    gen_vec = app.node(
        "gen_vec",
        func="generate_vector",
        factor=num_nodes,
        args=[buf_size],
    )
    compute_fft = app.node(
        "compute_fft",
        func="compute_fft",
        factor=num_nodes,
        args=[fft_planner, gen_vec.out()],
    )
    vec_mat = app.node(
        "vec_mat",
        func="vec_to_mat",
        factor=num_nodes,
        args=[gen_vec.out(), compute_fft.wait()],
    )
    mat_mul = app.node(
        "mat_mul",
        func="mat_mul",
        factor=num_nodes,
        args=[vec_mat.out(), vec_mat.out()],
    )
    app.node(
        "write_res",
        func="write_to_file",
        args=[result_file, mat_mul.out(end=num_nodes)],
    )

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    # Clean up previous output files (mirrors: rm -f $OUTPUT $TIMING_FILE)
    out_file = HERE / "out.txt"
    timing_file = HERE / "timing.txt"
    out_file.unlink(missing_ok=True)
    timing_file.unlink(missing_ok=True)

    report_file = HERE / "report.txt"
    report_file.unlink(missing_ok=True)

    app = build_graph()

    # SCRIPT_DIR is read by get_out_file() to locate the result file
    env = {"SCRIPT_DIR": str(HERE)}

    # Step 1: build Τομί + plugin library (mirrors: cargo build + cargo build --manifest-path)
    app.build(
        func_path=str(HERE / "src" / "lib.rs"),
        plugin_manifest=str(HERE / "Cargo.toml"),
        release=True,
        clean=args.clean,
        env=env,
    )

    # Step 2: run (mirrors: cargo run --bin main -- ...)
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
        report=str(report_file)
    )


if __name__ == "__main__":
    main()
