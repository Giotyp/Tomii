"""SynStream Wavefront benchmark — agent-bench harness interface.

Runs the N×N anti-diagonal wavefront sweep for a fixed (n, workers, iterations)
configuration and writes a structured JSON report.

This script is the reference implementation seed for the optimize_synstream
experiment.  Agents modify it to improve performance.

Usage (as invoked by the harness):
    python run_wavefront.py \\
        --n 64 --workers 4 --iterations 10 \\
        --report report.json \\
        [--timing timing.csv] \\
        [--dylib /path/to/libwavefront_bench.so]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent

import synstream as ss
from synstream._builder import find_workspace_root
from synstream._types import TypedValue

# Ensure the wavefront dylib lands in the main workspace target dir so that
# _builder._find_dylib() always locates it alongside other SynStream artifacts.
_REPO_ROOT = find_workspace_root()
_TARGET_DIR = str(_REPO_ROOT / "target")


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

def build_wavefront_graph(n: int, tile_size: int = 1) -> ss.Graph:
    """Build the anti-diagonal wavefront graph for an N×N grid."""
    app = ss.Graph()

    n_var = app.var("n", ss.usize(n))
    grid  = app.var("grid", func="init_grid", args=[n_var])

    _index = TypedValue("$ref", "$index")

    prev        = None
    prev_factor = 0

    for d in range(2 * n - 1):
        width = min(d + 1, n, 2 * n - 1 - d)

        if tile_size > 1:
            factor = math.ceil(width / tile_size)
            args   = [grid, n_var, ss.usize(d), _index, ss.usize(tile_size)]
            func   = "wf_tile"
        else:
            factor = width
            args   = [grid, n_var, ss.usize(d), _index]
            func   = "wf_cell"

        if prev is not None:
            args.append(prev.wait(0, prev_factor))

        cur         = app.node(f"diag_{d}", func=func, factor=factor, args=args)
        prev        = cur
        prev_factor = factor

    return app


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def _parse_synstream_timing(timing_file: Path):
    """Return (total_s, s_per_iter, iterations) from a SynStream timing CSV."""
    import re
    text = timing_file.read_text()
    total_m = re.search(r"Total Runtime:\s+([\d.]+)(ms|µs|us|s)", text)
    avg_m   = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|µs|us|s)", text)
    iters_m = re.search(r"Total Streams Processed:\s+(\d+)", text)

    def to_seconds(val: float, unit: str) -> float:
        if unit in ("ms",):      return val / 1e3
        if unit in ("µs", "us"): return val / 1e6
        return val

    total_s    = to_seconds(float(total_m.group(1)), total_m.group(2)) if total_m else 0.0
    s_per_iter = to_seconds(float(avg_m.group(1)),   avg_m.group(2))   if avg_m   else 0.0
    iterations = int(iters_m.group(1)) if iters_m else 0
    return total_s, s_per_iter, iterations


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SynStream Wavefront benchmark")
    p.add_argument("--n",          type=int, required=True,  help="grid size")
    p.add_argument("--workers",    type=int, required=True,  help="worker thread count")
    p.add_argument("--iterations", type=int, default=10,     help="timed sweeps")
    p.add_argument("--warmup",     type=int, default=3,      help="untimed warm-up sweeps")
    p.add_argument("--report",     type=Path, default=None,  help="output JSON report path")
    p.add_argument("--timing",     type=Path, default=None,  help="output timing CSV path")
    p.add_argument("--dylib",      type=str,  default=None,  help="prebuilt plugin dylib path")
    p.add_argument("--tile-size",  type=int,  default=1,     help="tile coarsening factor (default 1)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    # -------------------------------------------------------------------
    # Build plugin
    # -------------------------------------------------------------------
    if args.dylib:
        dylib = args.dylib
    else:
        build_app = ss.Graph()
        build_app.var("_dummy", ss.usize(0))
        build_result = build_app.build(
            func_path=str(HERE / "src" / "lib.rs"),
            plugin_manifest=str(HERE / "Cargo.toml"),
            # Route output into the main workspace target dir so the dylib is
            # found by _find_dylib() and synstream-core uses the same cache.
            env={"CARGO_TARGET_DIR": _TARGET_DIR},
            release=True,
            clean=False,
        )
        dylib = build_result.dylib

    # -------------------------------------------------------------------
    # Run benchmark
    # -------------------------------------------------------------------
    n          = args.n
    workers    = args.workers
    tile_size  = args.tile_size
    warmup     = args.warmup
    iterations = args.iterations
    total_streams = warmup + iterations

    timing_path = args.timing or (HERE / "_timing.csv")

    graph = build_wavefront_graph(n, tile_size=tile_size)
    graph.run(
        dylib=dylib,
        workers=workers,
        core_offset=1,
        system_threads=1,
        slots=1,
        max_streams=total_streams,
        exclude_streams=warmup,
        batching_size=1,
        timing=str(timing_path),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
    )

    total_s, s_per_iter, iters = _parse_synstream_timing(timing_path)
    avg_latency_us = s_per_iter * 1e6

    print(f"SynStream Wavefront | n={n} workers={workers} tile_size={tile_size}")
    print(f"  avg_latency_us = {avg_latency_us:.1f}")
    print(f"  total_s = {total_s:.4f}  iters = {iters}")

    # -------------------------------------------------------------------
    # Correctness: call the provided verifier
    # -------------------------------------------------------------------
    import subprocess
    subprocess.run(
        [sys.executable, str(_REPO_ROOT / "agent-bench" / "tools" / "verify_wavefront.py"),
         "--n", str(n)],
        check=True,
    )

    # -------------------------------------------------------------------
    # Write report
    # -------------------------------------------------------------------
    report = {
        "summary": {
            "avg_latency_us":  avg_latency_us,
            "p99_latency_us":  None,
            "min_latency_us":  None,
            "total_streams":   iters,
            "worker_busy_pct": None,
        },
        "config": {
            "n":         n,
            "workers":   workers,
            "tile_size": tile_size,
        },
        "bottleneck_hints": [],
        "critical_path":    [],
    }

    report_path = args.report or (HERE / "report.json")
    report_path.write_text(json.dumps(report, indent=2))
    print(f"  -> report written to {report_path}", flush=True)


if __name__ == "__main__":
    main()
