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
import csv
import os
import re
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
# Timing helpers
# ---------------------------------------------------------------------------

def _parse_synstream_timing(timing_file: Path):
    """Return (total_s, s_per_iter, iterations) from a SynStream timing CSV."""
    text = timing_file.read_text()
    total_m = re.search(r"Total Runtime:\s+([\d.]+)s", text)
    avg_m   = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|s)", text)
    iters_m = re.search(r"Total Streams Processed:\s+(\d+)", text)
    total_s    = float(total_m.group(1)) if total_m else 0.0
    if avg_m:
        val  = float(avg_m.group(1))
        s_per_iter = val / 1000.0 if avg_m.group(2) == "ms" else val
    else:
        s_per_iter = 0.0
    iterations = int(iters_m.group(1)) if iters_m else 0
    return total_s, s_per_iter, iterations


def _write_standard_csv(out_path: Path, dataset: str, workers: int,
                        total_s: float, s_per_iter: float, iterations: int) -> None:
    """Write a standard-format CSV compatible with compare_results.py."""
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "dataset", "workers", "iterations", "total_s", "s_per_iter"])
        w.writerow(["synstream", dataset, workers, iterations,
                    f"{total_s:.6f}", f"{s_per_iter:.6f}"])


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

    # partial_gather[i]: sum all N scatter contributions for node range [i*chunk, (i+1)*chunk).
    # factor=nw → N parallel instances; $index selects each instance's range.
    partial_gather = app.node(
        "partial_gather",
        func="pr_partial_gather",
        factor=nw,
        args=[nw, _index, scatter.out(0, nw)],
    )

    # reduce: apply damping formula — factor=nw so each instance writes its own
    # node-range chunk [idx*chunk, (idx+1)*chunk) in parallel (no serialization).
    app.node(
        "reduce",
        func="pr_reduce_partial",
        factor=nw,
        args=[nw, _index, ranks, damping_var, partial_gather.out(0, nw)],
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

    # Infer dataset name from graph file stem (e.g. "livejournal", "twitter")
    dataset = Path(args.graph_file).stem

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

        # Also write a standard-format CSV for compare_results.py
        total_s, s_per_iter, iters = _parse_synstream_timing(timing_file)
        std_csv = args.results_dir / f"synstream_pagerank_{dataset}_w{workers}.csv"
        _write_standard_csv(std_csv, dataset, workers, total_s, s_per_iter, iters)
        print(f"  -> {std_csv}", flush=True)

    print(f"\nDone. Results written to {args.results_dir}")


if __name__ == "__main__":
    main()
