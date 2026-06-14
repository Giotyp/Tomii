"""Tomii anti-diagonal wavefront benchmark.

Sweeps worker counts for an N×N wavefront and writes per-stream timing.
Matches the Taskflow benchmark (N=512, 100 iterations, 10 warmup) so the
two CSV outputs can be compared directly.

Usage (from repo root):
    cd .worktrees/bench
    python anti-diag-bench/tomii/run_bench.py                   # per-cell only
    python anti-diag-bench/tomii/run_bench.py --func wf_cell_bulk
    python anti-diag-bench/tomii/run_bench.py --func all        # both + comparison table
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent  # anti-diag-bench/tomii/
BENCH_ROOT = HERE.parents[2]  # workspace root
DEVELOP_ROOT = BENCH_ROOT  # same as workspace root on develop
# Prefer develop workspace so we pick up the installed editable tomii package
sys.path.insert(0, str(DEVELOP_ROOT))

import tomii as tm
from tomii._types import TypedValue

ALL_FUNCS = ["wf_cell", "wf_cell_bulk"]


def _parse_avg_ms(timing_file: Path) -> float:
    text = timing_file.read_text()
    m = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|µs|us|s)", text)
    if not m:
        return float("nan")
    val, unit = float(m.group(1)), m.group(2)
    if unit in ("µs", "us"):
        return val / 1e3
    if unit == "s":
        return val * 1e3
    return val  # ms


def build_wavefront(n: int, func: str = "wf_cell") -> tm.Graph:
    app = tm.Graph()
    n_var = app.var("n", tm.usize(n))
    grid = app.var("grid", func="init_grid", args=[n_var])
    _index = TypedValue("$ref", "$index")

    prev = None
    prev_factor = 0

    for d in range(2 * n - 1):
        width = min(d + 1, n, 2 * n - 1 - d)
        args = [grid, n_var, tm.usize(d), _index]
        if prev is not None:
            args.append(prev.wait(0, prev_factor))
        cur = app.node(f"diag_{d}", func=func, factor=width, args=args)
        prev = cur
        prev_factor = width

    return app


def run_sweep(
    func_tag: str,
    workers: list[int],
    n: int,
    warmup: int,
    iterations: int,
    results_dir: Path,
    dylib: str,
) -> dict[int, float]:
    """Run a single variant sweep; return {workers: ms} mapping."""
    csv_path = results_dir / f"tomii_wavefront_n{n}_{func_tag}_sweep.csv"
    with open(csv_path, "w") as f:
        f.write("system,n,workers,iterations,ms_per_iter\n")

    results: dict[int, float] = {}
    total_streams = warmup + iterations

    for w in workers:
        timing_file = results_dir / f"tomii_wavefront_n{n}_{func_tag}_w{w}.txt"
        print(f"\n=== Tomii | n={n} workers={w} func={func_tag} ===", flush=True)

        graph = build_wavefront(n, func=func_tag)
        graph.run(
            dylib=dylib,
            workers=w,
            core_offset=1,
            system_threads=1,
            slots=1,
            max_streams=total_streams,
            exclude_streams=warmup,
            batching_size=1,
            timing=str(timing_file),
            use_rdtsc=True,
            custom=True,
            coalesce_barriers=True,
            inline_continuation=True,
        )

        ms = _parse_avg_ms(timing_file)
        results[w] = ms
        print(f"  avg: {ms:.3f} ms/iter", flush=True)

        with open(csv_path, "a") as f:
            f.write(f"tomii_{func_tag},{n},{w},{iterations},{ms:.6f}\n")

    print(f"\nResults: {csv_path}", flush=True)
    return results


def print_comparison(
    worker_list: list[int],
    all_results: dict[str, dict[int, float]],
) -> None:
    funcs = list(all_results.keys())
    col_w = 14

    header = f"{'Workers':>8}" + "".join(f"{f:>{col_w}}" for f in funcs)
    if len(funcs) == 2:
        header += f"{'speedup':>{col_w}}"
    print("\n" + "=" * len(header))
    print(header)
    print("-" * len(header))

    for w in worker_list:
        row = f"{w:>8}"
        vals = [all_results[f].get(w, float("nan")) for f in funcs]
        for v in vals:
            row += f"{v:>{col_w - 3}.3f} ms"
        if len(funcs) == 2 and vals[1] > 0:
            speedup = vals[0] / vals[1]
            row += f"{speedup:>{col_w - 1}.2f}×"
        print(row)

    print("=" * len(header))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--n", type=int, default=512)
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8, 16, 32])
    p.add_argument("--iterations", type=int, default=100)
    p.add_argument("--warmup", type=int, default=10)
    p.add_argument("--results-dir", type=Path, default=HERE / "results")
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument(
        "--func",
        default="wf_cell",
        choices=ALL_FUNCS + ["all"],
        help="kernel to use: wf_cell (per-cell), wf_cell_bulk (Tier 4 bulk), or all (run both + compare)",
    )
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)

    funcs_to_run = ALL_FUNCS if args.func == "all" else [args.func]

    print("Building plugin...", flush=True)
    build_graph = tm.Graph()
    build_graph.var("_dummy", tm.usize(0))
    if args.clean:
        subprocess.run(
            ["cargo", "clean", "--manifest-path", str(HERE / "Cargo.toml")],
            check=True,
        )
    subprocess.run(
        ["cargo", "build", "--manifest-path", str(HERE / "Cargo.toml"), "--release"],
        check=True,
        env={**__import__("os").environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")},
    )
    dylib = str(HERE / "target" / "release" / "libwf_bench.so")

    build_graph.build(
        func_path=str(HERE / "src" / "lib.rs"),
        release=True,
        clean=False,
    )
    print(f"  dylib: {dylib}", flush=True)

    all_results: dict[str, dict[int, float]] = {}
    for func_tag in funcs_to_run:
        all_results[func_tag] = run_sweep(
            func_tag=func_tag,
            workers=args.workers,
            n=args.n,
            warmup=args.warmup,
            iterations=args.iterations,
            results_dir=args.results_dir,
            dylib=dylib,
        )

    if len(funcs_to_run) > 1:
        print_comparison(args.workers, all_results)


if __name__ == "__main__":
    main()
