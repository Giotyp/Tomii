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
        default=[1, 2, 4],
        metavar="N",
        help="system thread counts to sweep (default: 1 2 4)",
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

def build_wavefront_graph(n: int) -> ss.Graph:
    """Build the anti-diagonal wavefront graph for an N×N grid.

    Graph structure:
      - 1 initialisation: grid (N×N f64 with boundary values)
      - 2N-1 compute nodes: diag_0 .. diag_{2N-2}
        - diag_d has factor = anti-diagonal width = min(d+1, N, 2N-1-d)
        - diag_d has $barrier on diag_{d-1} (all instances) for d > 0
        - Each instance computes one cell: grid[i][j] = 0.5*(grid[i-1][j]+grid[i][j-1])

    One SynStream stream = one full N×N wavefront sweep.
    """
    app = ss.Graph()

    n_var  = app.var("n", ss.usize(n))
    grid   = app.var("grid", func="init_grid", args=[n_var])

    # $index is resolved at runtime to the instance index within the diagonal
    _index = TypedValue("$ref", "$index")

    prev       = None
    prev_width = 0

    for d in range(2 * n - 1):
        width = min(d + 1, n, 2 * n - 1 - d)

        args = [grid, n_var, ss.usize(d), _index]
        if prev is not None:
            # $barrier: wait for ALL prev_width instances of the previous diagonal
            args.append(prev.wait(0, prev_width))

        cur = app.node(f"diag_{d}", func="wf_cell", factor=width, args=args)
        prev       = cur
        prev_width = width

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
                system_label = f"synstream_st{st}"
                timing_file = args.results_dir / f"synstream_wavefront_n{n}_w{workers}_st{st}.csv"
                print(f"\n=== SynStream Wavefront | n={n} workers={workers} system_threads={st} ===", flush=True)

                graph = build_wavefront_graph(n)

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
                )
                print(f"  -> {timing_file}", flush=True)

                total_s, s_per_iter, iters = _parse_synstream_timing(timing_file)
                std_csv = args.results_dir / f"synstream_wavefront_n{n}_w{workers}_st{st}_result.csv"
                _write_wavefront_csv(std_csv, system_label, n, workers, total_s, s_per_iter, iters)
                print(f"  -> {std_csv}", flush=True)

    print(f"\nDone. Results written to {args.results_dir}")


if __name__ == "__main__":
    main()
