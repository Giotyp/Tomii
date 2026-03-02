"""SynStream PageRank (COST) benchmark.

Runs 20 iterations of PageRank on a SNAP graph file, sweeping worker counts.
Each "stream" corresponds to one PageRank iteration; the rank vector persists
across streams via the shared CmTypes::Any(Vec<f32>) ranks object.

Usage:
    # From repo root (activate venv first: source .venv/bin/activate)
    SNAP_GRAPH_FILE=/data/snap/soc-LiveJournal1.txt \\
        python examples/cost-bench/run_bench.py

    # Custom parameters
    python examples/cost-bench/run_bench.py \\
        --workers 1 2 4 8 \\
        --iterations 20 \\
        --graph-file /data/snap/twitter_rv.txt \\
        --no-clean
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Resolve paths
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent       # examples/cost-bench/
REPO_ROOT = HERE.parents[1]                  # workspace root
sys.path.insert(0, str(REPO_ROOT))

import synstream as ss                       # noqa: E402
from synstream._types import TypedValue      # noqa: E402  (private but stable)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SynStream PageRank (COST) benchmark sweep")
    p.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8],
        metavar="N",
        help="worker counts to sweep (default: 1 2 4 8)",
    )
    p.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="PageRank iterations per run (default: 20)",
    )
    p.add_argument(
        "--exclude-streams",
        type=int,
        default=0,
        help="warm-up iterations to exclude from timing stats (default: 0)",
    )
    p.add_argument(
        "--damping",
        type=float,
        default=0.85,
        help="damping factor (default: 0.85)",
    )
    p.add_argument(
        "--graph-file",
        type=str,
        default=os.environ.get("SNAP_GRAPH_FILE", ""),
        help="path to SNAP edge-list file (or set SNAP_GRAPH_FILE env var)",
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
# Graph definition
# ---------------------------------------------------------------------------

def build_pagerank_graph(damping: float, workers: int) -> ss.Graph:
    """Build the PageRank computation graph for a given worker count.

    NOTE: ``num_workers`` is embedded as a concrete init var (not ``$workers``)
    because predecessor index ranges like "0-num_workers" require a resolved
    init object value — the ``$workers`` placeholder in init_objects resolves
    to a sentinel 1, not the actual worker count.
    """
    app = ss.Graph()

    # Special $ref arguments resolved at task-execution time
    _index   = TypedValue("$ref", "$index")    # current instance index (0..workers-1)
    _workers = TypedValue("$ref", "$workers")  # total worker count

    # Initializations
    nw             = app.var("num_workers",   ss.usize(workers))
    graph_file     = app.var("graph_file",    ss.String("SNAP_GRAPH_FILE"))
    damping_var    = app.var("damping",       ss.f64(damping))
    graph          = app.var("graph",         func="load_graph",        args=[graph_file])
    ranks          = app.var("ranks",         func="create_ranks",      args=[graph])
    all_partitions = app.var("all_partitions", func="get_all_partitions", args=[graph, nw])

    # scatter[i]: compute contributions for pre-computed partition[i] using current ranks.
    # all_partitions is computed once at init; $index selects this worker's slice.
    scatter = app.node(
        "scatter",
        func="pr_scatter",
        factor=nw,
        args=[all_partitions, _index, graph, ranks],
    )

    # gather: args = [ranks, damping, scatter_0, scatter_1, ..., scatter_{w-1}]
    # Using scatter.out(0, nw) → "0-num_workers" → indices [0..workers)
    app.node(
        "gather",
        func="pr_gather",
        args=[ranks, damping_var, scatter.out(0, nw)],
    )

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    if not args.graph_file:
        print(
            "ERROR: --graph-file not specified and SNAP_GRAPH_FILE env var is not set.",
            file=sys.stderr,
        )
        sys.exit(1)

    args.results_dir.mkdir(parents=True, exist_ok=True)

    # Build plugin + binary once
    build_app = ss.Graph()
    build_app.var("_dummy", ss.usize(0))
    build_result = build_app.build(
        wrap_path=str(HERE / "wrappers.rs"),
        reg_path=str(HERE / "reg.rs"),
        plugin_manifest=str(HERE / "Cargo.toml"),
        release=True,
        clean=args.clean,
    )
    dylib = build_result.dylib

    # Sweep worker counts
    for workers in args.workers:
        timing_file = args.results_dir / f"synstream_pagerank_w{workers}.csv"
        print(f"\n=== SynStream PageRank | workers={workers} ===", flush=True)

        graph = build_pagerank_graph(args.damping, workers)

        # SNAP_GRAPH_FILE is read by load_graph_cm at initialization time
        env = {"SNAP_GRAPH_FILE": args.graph_file}

        graph.run(
            dylib=dylib,
            env=env,
            workers=workers,
            system_threads=1,
            slots=1,
            max_streams=args.iterations,
            exclude_streams=args.exclude_streams,
            batching_size=1,
            timing=str(timing_file),
            use_rdtsc=True,
        )
        print(f"  -> {timing_file}", flush=True)

    print(f"\nDone. Results written to {args.results_dir}")


if __name__ == "__main__":
    main()
