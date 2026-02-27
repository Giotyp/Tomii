"""SynStream STREAM benchmark — sweep of all 4 STREAM kernels across worker counts.

Builds the stream-bench plugin once, then runs each (kernel, workers) combination
and records timing CSVs for downstream analysis.

Usage:
    # From repo root (activate venv first: source .venv/bin/activate)
    python examples/stream-bench/run_bench.py

    # Custom worker list and kernel selection
    python examples/stream-bench/run_bench.py --workers 1 2 4 8 --kernels copy triad

    # Skip recompilation
    python examples/stream-bench/run_bench.py --no-clean
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent       # examples/stream-bench/
REPO_ROOT = HERE.parents[1]                  # workspace root
sys.path.insert(0, str(REPO_ROOT))

import synstream as ss                       # noqa: E402


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SynStream STREAM benchmark sweep")
    p.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8],
        metavar="N",
        help="worker counts to sweep (default: 1 2 4 8)",
    )
    p.add_argument(
        "--kernels",
        nargs="+",
        choices=["copy", "scale", "add", "triad"],
        default=["copy", "scale", "add", "triad"],
        help="kernels to benchmark (default: all 4)",
    )
    p.add_argument(
        "--array-size",
        type=int,
        default=268435456,
        help="elements per worker array (default: 256M f64 = 2 GB)",
    )
    p.add_argument(
        "--max-streams",
        type=int,
        default=20,
        help="streams per run (default: 20)",
    )
    p.add_argument(
        "--exclude-streams",
        type=int,
        default=3,
        help="warm-up streams to exclude from stats (default: 3)",
    )
    p.add_argument(
        "--results-dir",
        type=Path,
        default=HERE / "results",
        help="output directory for timing CSVs",
    )
    p.add_argument(
        "--no-clean",
        dest="clean",
        action="store_false",
        default=True,
        help="skip cargo clean before build",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph builders
# NOTE: num_workers is passed explicitly because "$workers" in predecessor
# index ranges resolves to a placeholder at graph-build time.  Using a
# concrete init var makes the range "0-num_workers" resolve correctly.
# ---------------------------------------------------------------------------

def build_copy_graph(array_size: int, workers: int) -> ss.Graph:
    app = ss.Graph()
    nw   = app.var("num_workers", ss.usize(workers))
    n    = app.var("array_size",  ss.usize(array_size))
    fill = app.var("fill_val",    ss.f64(2.0))

    gen_b   = app.node("gen_b",   func="generate_array", factor=nw,
                       args=[n, fill])
    copy_op = app.node("copy_op", func="stream_copy",    factor=nw,
                       args=[gen_b.out()])
    app.node("sink", func="sink",
             args=[copy_op.wait(0, nw)])
    return app


def build_scale_graph(array_size: int, workers: int) -> ss.Graph:
    app = ss.Graph()
    nw     = app.var("num_workers", ss.usize(workers))
    n      = app.var("array_size",  ss.usize(array_size))
    fill   = app.var("fill_val",    ss.f64(2.0))
    scalar = app.var("scalar",      ss.f64(3.0))

    gen_b    = app.node("gen_b",    func="generate_array", factor=nw,
                        args=[n, fill])
    scale_op = app.node("scale_op", func="stream_scale",   factor=nw,
                        args=[gen_b.out(), scalar])
    app.node("sink", func="sink",
             args=[scale_op.wait(0, nw)])
    return app


def build_add_graph(array_size: int, workers: int) -> ss.Graph:
    app = ss.Graph()
    nw     = app.var("num_workers", ss.usize(workers))
    n      = app.var("array_size",  ss.usize(array_size))
    fill_b = app.var("fill_b",      ss.f64(2.0))
    fill_c = app.var("fill_c",      ss.f64(1.0))

    gen_b  = app.node("gen_b",  func="generate_array", factor=nw,
                      args=[n, fill_b])
    gen_c  = app.node("gen_c",  func="generate_array", factor=nw,
                      args=[n, fill_c])
    add_op = app.node("add_op", func="stream_add",     factor=nw,
                      args=[gen_b.out(), gen_c.out()])
    app.node("sink", func="sink",
             args=[add_op.wait(0, nw)])
    return app


def build_triad_graph(array_size: int, workers: int) -> ss.Graph:
    app = ss.Graph()
    nw     = app.var("num_workers", ss.usize(workers))
    n      = app.var("array_size",  ss.usize(array_size))
    fill_b = app.var("fill_b",      ss.f64(2.0))
    fill_c = app.var("fill_c",      ss.f64(1.0))
    scalar = app.var("scalar",      ss.f64(3.0))

    gen_b    = app.node("gen_b",    func="generate_array", factor=nw,
                        args=[n, fill_b])
    gen_c    = app.node("gen_c",    func="generate_array", factor=nw,
                        args=[n, fill_c])
    triad_op = app.node("triad_op", func="stream_triad",   factor=nw,
                        args=[gen_b.out(), gen_c.out(), scalar])
    app.node("sink", func="sink",
             args=[triad_op.wait(0, nw)])
    return app


_GRAPH_BUILDERS = {
    "copy":  build_copy_graph,
    "scale": build_scale_graph,
    "add":   build_add_graph,
    "triad": build_triad_graph,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    # Build plugin + binary once
    build_app = ss.Graph()
    build_app.var("_dummy", ss.usize(0))
    result = build_app.build(
        wrap_path=str(HERE / "wrappers.rs"),
        reg_path=str(HERE / "reg.rs"),
        plugin_manifest=str(HERE / "Cargo.toml"),
        release=True,
        clean=args.clean,
    )
    dylib = result.dylib

    # Sweep (kernel, workers)
    for kernel in args.kernels:
        builder = _GRAPH_BUILDERS[kernel]
        for workers in args.workers:
            timing_file = args.results_dir / f"synstream_stream_{kernel}_w{workers}.csv"
            print(
                f"\n=== SynStream STREAM {kernel.upper()} | workers={workers} ===",
                flush=True,
            )
            graph = builder(args.array_size, workers)
            graph.run(
                dylib=dylib,
                workers=workers,
                system_threads=1,
                slots=1,
                max_streams=args.max_streams,
                exclude_streams=args.exclude_streams,
                batching_size=1,
                timing=str(timing_file),
                use_rdtsc=True,
            )
            print(f"  -> {timing_file}", flush=True)

    print(f"\nDone. Results written to {args.results_dir}")


if __name__ == "__main__":
    main()
