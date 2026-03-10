"""SynStream Wavefront benchmark.

Runs 20 iterations of an N×N anti-diagonal wavefront sweep, sweeping worker
counts and system-thread counts.  Each "stream" is one complete wavefront pass
over the grid.

The graph has 2N-1 nodes (one per anti-diagonal), each with factor equal to
the anti-diagonal width.  A $barrier dependency between consecutive diagonals
enforces the wavefront synchronisation: all instances of diagonal d must
complete before any instance of diagonal d+1 starts.

Usage:
    # From repo root (activate venv first: source .venv/bin/activate)
    python examples/wavefront-bench/run_bench.py

    # Custom parameters
    python examples/wavefront-bench/run_bench.py \\
        --workers 1 2 4 8 \\
        --system-threads 1 2 4 \\
        --n 64 128 256 512 \\
        --iterations 20 \\
        --no-clean
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent       # examples/wavefront-bench/
REPO_ROOT = HERE.parents[1]                  # workspace root
sys.path.insert(0, str(REPO_ROOT))

import synstream as ss                       # noqa: E402
from synstream._types import TypedValue      # noqa: E402


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def _parse_synstream_timing(timing_file: Path):
    """Return (total_s, s_per_iter, iterations) from a SynStream timing CSV."""
    text = timing_file.read_text()
    total_m = re.search(r"Total Runtime:\s+([\d.]+)(ms|µs|us|s)", text)
    avg_m   = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|µs|us|s)", text)
    iters_m = re.search(r"Total Streams Processed:\s+(\d+)", text)
    def to_seconds(val: float, unit: str) -> float:
        if unit in ("ms",):      return val / 1e3
        if unit in ("µs", "us"): return val / 1e6
        return val  # "s"
    total_s    = to_seconds(float(total_m.group(1)), total_m.group(2)) if total_m else 0.0
    s_per_iter = to_seconds(float(avg_m.group(1)),   avg_m.group(2))   if avg_m   else 0.0
    iterations = int(iters_m.group(1)) if iters_m else 0
    return total_s, s_per_iter, iterations


def _write_wavefront_csv(out_path: Path, system: str, n: int, workers: int,
                         total_s: float, s_per_iter: float, iterations: int) -> None:
    """Write a standard-format wavefront CSV compatible with compare_results.py."""
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["system", "n", "workers", "iterations", "total_s", "s_per_iter"])
        w.writerow([system, n, workers, iterations,
                    f"{total_s:.6f}", f"{s_per_iter:.6f}"])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SynStream Wavefront benchmark sweep")
    p.add_argument(
        "--workers",
        type=int,
        nargs="+",
        default=[1, 2, 4, 8, 16, 32],
        metavar="N",
        help="worker counts to sweep (default: 1 2 4 8 16 32)",
    )
    p.add_argument(
        "--n",
        type=int,
        nargs="+",
        default=[64, 128, 256, 512],
        metavar="N",
        help="grid sizes to sweep (default: 64 128 256 512)",
    )
    p.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="timed wavefront sweeps per configuration (default: 20)",
    )
    p.add_argument(
        "--warmup",
        type=int,
        default=3,
        help="untimed warm-up sweeps (default: 3)",
    )
    p.add_argument(
        "--results-dir",
        type=Path,
        default=HERE / "results",
        help="output directory for timing CSVs",
    )
    p.add_argument(
        "--system-threads",
        type=int,
        nargs="+",
        default=[1],
        metavar="N",
        help="system thread counts to sweep (default: 1)",
    )
    p.add_argument(
        "--tile-sizes",
        type=int,
        nargs="+",
        default=[1, 8, 32],
        metavar="T",
        help="tile sizes to sweep (default: 1 8 32); 1=one task per cell",
    )
    p.add_argument(
        "--no-clean",
        dest="clean",
        action="store_false",
        default=True,
        help="skip cargo clean before build",
    )
    p.add_argument(
        "--block-dag",
        action="store_true",
        default=False,
        help="also run the 2D block DAG variant (tile_size=32 explicit B×B node graph)",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph definition
# ---------------------------------------------------------------------------

def build_wavefront_block_dag(n: int, tile_size: int = 32) -> ss.Graph:
    """Build the 2D block DAG wavefront for an N×N grid with T×T blocks.

    Creates B×B nodes (B = ceil(N/T)), each with factor=1, computing one T×T
    block.  Each interior block receives a ``$res`` from its top neighbour
    (block_row-1, block_col) and its left neighbour (block_row, block_col-1),
    so the scheduler fires it as soon as both predecessors complete — no global
    anti-diagonal barrier required.

    This matches Taskflow's optimal wavefront implementation (per-block DAG with
    left+top dependencies) while remaining fully expressible in SynStream's
    ``$res`` dependency model.
    """
    B = math.ceil(n / tile_size)

    app = ss.Graph()
    n_var = app.var("n", ss.usize(n))
    grid  = app.var("grid", func="init_grid", args=[n_var])

    blocks: dict = {}
    for i in range(B):
        for j in range(B):
            args = [grid, n_var, ss.usize(i), ss.usize(j), ss.usize(tile_size)]
            if i > 0:
                args.append(blocks[(i - 1, j)].out(0))   # $res from top neighbour
            if j > 0:
                args.append(blocks[(i, j - 1)].out(0))   # $res from left neighbour
            blocks[(i, j)] = app.node(
                f"block_{i}_{j}", func="wf_block", factor=1, args=args
            )

    return app


def build_wavefront_graph(n: int, tile_size: int = 1) -> ss.Graph:
    """Build the anti-diagonal wavefront graph for an N×N grid.

    When tile_size == 1 (default), one task per cell (original behaviour):
      - diag_d has factor = width = min(d+1, N, 2N-1-d)
      - each instance computes one cell via wf_cell

    When tile_size > 1, tile-coarsened graph:
      - diag_d has factor = ceil(width / tile_size) tiles
      - each instance computes tile_size consecutive cells via wf_tile
      - total tasks reduced by ~tile_size, eliminating scheduler overhead

    One SynStream stream = one full N×N wavefront sweep.
    """
    app = ss.Graph()

    n_var = app.var("n", ss.usize(n))
    grid  = app.var("grid", func="init_grid", args=[n_var])

    # $index is resolved at runtime to the instance index within the diagonal
    _index = TypedValue("$ref", "$index")

    prev         = None
    prev_factor  = 0  # number of tasks (tiles or cells) in the previous diagonal

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
            # $barrier: wait for ALL prev_factor tasks of the previous diagonal
            args.append(prev.wait(0, prev_factor))

        cur         = app.node(f"diag_{d}", func=func, factor=factor, args=args)
        prev        = cur
        prev_factor = factor

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    # Build the plugin once (all N values use the same library)
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

    total_streams = args.warmup + args.iterations

    for n in args.n:
        for workers in args.workers:
            for st in args.system_threads:
                for tile_size in args.tile_sizes:
                    tile_tag    = f"t{tile_size}"
                    system_label = f"synstream_st{st}_{tile_tag}"
                    timing_file  = args.results_dir / f"synstream_wavefront_n{n}_w{workers}_st{st}_{tile_tag}.csv"
                    print(
                        f"\n=== SynStream Wavefront | n={n} workers={workers} "
                        f"system_threads={st} tile_size={tile_size} ===",
                        flush=True,
                    )

                    graph = build_wavefront_graph(n, tile_size=tile_size)

                    graph.run(
                        dylib=dylib,
                        workers=workers,
                        core_offset=1,
                        system_threads=st,
                        slots=1,
                        max_streams=total_streams,
                        exclude_streams=args.warmup,
                        batching_size=1,
                        timing=str(timing_file),
                        use_rdtsc=True,
                        custom=True,
                        coalesce_barriers=True,
                        inline_continuation=True,
                    )
                    print(f"  -> {timing_file}", flush=True)

                    total_s, s_per_iter, iters = _parse_synstream_timing(timing_file)
                    std_csv = args.results_dir / f"synstream_wavefront_n{n}_w{workers}_st{st}_{tile_tag}_result.csv"
                    _write_wavefront_csv(std_csv, system_label, n, workers, total_s, s_per_iter, iters)
                    print(f"  -> {std_csv}", flush=True)

    if args.block_dag:
        tile_size = 32  # fixed: block DAG always uses T=32 blocks
        for n in args.n:
            B = math.ceil(n / tile_size)
            for workers in args.workers:
                system_label = f"synstream_block_t{tile_size}"
                timing_file  = args.results_dir / f"synstream_wavefront_block_n{n}_w{workers}_t{tile_size}.csv"
                print(
                    f"\n=== SynStream Block DAG | n={n} B={B}×{B} workers={workers} "
                    f"tile_size={tile_size} ===",
                    flush=True,
                )

                graph = build_wavefront_block_dag(n, tile_size=tile_size)

                graph.run(
                    dylib=dylib,
                    workers=workers,
                    core_offset=1,
                    system_threads=1,
                    slots=1,
                    max_streams=total_streams,
                    exclude_streams=args.warmup,
                    batching_size=1,
                    timing=str(timing_file),
                    use_rdtsc=True,
                    custom=True,
                    inline_continuation=True,
                )
                print(f"  -> {timing_file}", flush=True)

                total_s, s_per_iter, iters = _parse_synstream_timing(timing_file)
                std_csv = args.results_dir / f"synstream_wavefront_block_n{n}_w{workers}_t{tile_size}_result.csv"
                _write_wavefront_csv(std_csv, system_label, n, workers, total_s, s_per_iter, iters)
                print(f"  -> {std_csv}", flush=True)

    print(f"\nDone. Results written to {args.results_dir}")


if __name__ == "__main__":
    main()
