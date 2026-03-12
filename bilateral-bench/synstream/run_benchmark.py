"""SynStream bilateral image denoising benchmark.

Defines a 2D wavefront DAG where each node T(i,j) applies a bilateral filter
to one image tile.  Dependencies enforce the correct halo pixel ordering:
  T(i,j) depends on T(i-1,j) and T(i,j-1).

The *timed* graph contains exactly N² bilateral_filter_tile nodes — the same
task count as the Taskflow reference implementation.  A separate validation
graph (run once per image size before the sweep) includes decompose_tiles +
tiles + reassemble_tiles + compute_psnr to verify correctness.

Usage (from bilateral-bench/synstream/):
    python run_benchmark.py

    python run_benchmark.py \\
        --image-sizes 1024 4096 \\
        --tile-size 256 \\
        --kernel-radius 4 \\
        --sigma-s 3.0 \\
        --sigma-r 0.1 \\
        --workers 1 2 4 8 \\
        --system-threads 1 2 \\
        --iterations 10 \\
        --no-clean

The script writes one CSV per (image_size, workers, system_threads) combination
to results/ and a merged results/ss_bilateral_all.csv at the end.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Optional

HERE      = Path(__file__).resolve().parent          # bilateral-bench/synstream/
REPO_ROOT = HERE.parents[2]                          # workspace root
DATA_DIR  = HERE.parent / "data"
RESULTS   = HERE / "results"

sys.path.insert(0, str(REPO_ROOT))
import synstream as ss                               # noqa: E402
from synstream import NodeDep                        # noqa: E402


# ---------------------------------------------------------------------------
# Timing parsing
# ---------------------------------------------------------------------------

def _parse_synstream_timing(timing_file: Path):
    """Return (mean_ms, iterations) from a SynStream timing CSV."""
    text = timing_file.read_text()
    avg_m   = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|s)", text)
    iters_m = re.search(r"Total Streams Processed:\s+(\d+)", text)

    if avg_m:
        val      = float(avg_m.group(1))
        mean_ms  = val if avg_m.group(2) == "ms" else val * 1000.0
    else:
        mean_ms = 0.0
    iterations = int(iters_m.group(1)) if iters_m else 0
    return mean_ms, iterations


# ---------------------------------------------------------------------------
# Lean benchmark graph — exactly N² tile nodes (matches Taskflow task count)
# ---------------------------------------------------------------------------

def build_benchmark_graph(
    image_size:    int,
    tile_size:     int,
    sigma_s:       float,
    sigma_r:       float,
    kernel_radius: int,
    use_dep:       bool = False,
) -> ss.Graph:
    """Build the lean bilateral wavefront graph for timing.

    Contains exactly N² bilateral_filter_tile nodes — no decompose, reassemble,
    or compute_psnr — matching the Taskflow task count exactly.

    Tile dependencies (wavefront):
      tile(0,0):   no predecessor tiles  (starts as initial node)
      tile(0,j):   depends on tile(0,j-1)
      tile(i,0):   depends on tile(i-1,0)
      tile(i,j):   depends on tile(i-1,j) and tile(i,j-1)
    """
    S = image_size
    T = tile_size
    assert S % T == 0, f"image_size {S} must be divisible by tile_size {T}"
    N = S // T

    noisy_path = str(DATA_DIR / f"noisy_{S}x{S}.npy")
    clean_path = str(DATA_DIR / f"clean_{S}x{S}.npy")

    app = ss.Graph()

    # Var: load images and allocate output buffer (runs once before streaming)
    state = app.var(
        "state",
        func="init_bench_state",
        args=[
            ss.String(noisy_path),
            ss.String(clean_path),
            ss.usize(T),
            ss.f32(sigma_s),
            ss.f32(sigma_r),
            ss.usize(kernel_radius),
        ],
    )

    # N×N wavefront tile grid — identical task count to Taskflow.
    # With use_dep=True, ordering edges use $dep instead of $res: the runtime
    # skips result storage for every tile (needs_result_store=False), eliminating
    # one Box::new(CmTypes::None) allocation + AtomicPtr::swap per tile task.
    tiles: list[list] = [[None] * N for _ in range(N)]

    for i in range(N):
        for j in range(N):
            args = [state, ss.usize(i), ss.usize(j)]

            if use_dep:
                if i > 0:
                    args.append(tiles[i - 1][j].dep(0))
                if j > 0:
                    args.append(tiles[i][j - 1].dep(0))
            else:
                if i > 0:
                    args.append(tiles[i - 1][j].out(0))
                if j > 0:
                    args.append(tiles[i][j - 1].out(0))

            tiles[i][j] = app.node(
                f"tile_{i}_{j}",
                func="bilateral_filter_tile",
                args=args,
            )

    return app


# ---------------------------------------------------------------------------
# Validation graph — full pipeline, run once for correctness check
# ---------------------------------------------------------------------------

def build_validation_graph(
    image_size:    int,
    tile_size:     int,
    sigma_s:       float,
    sigma_r:       float,
    kernel_radius: int,
) -> ss.Graph:
    """Full graph: decompose + N² tiles + reassemble + compute_psnr.

    Run once per image size to verify correctness (PSNR).  Not used for timing.
    """
    S = image_size
    T = tile_size
    N = S // T

    noisy_path = str(DATA_DIR / f"noisy_{S}x{S}.npy")
    clean_path = str(DATA_DIR / f"clean_{S}x{S}.npy")

    app = ss.Graph()

    state = app.var(
        "state",
        func="init_bench_state",
        args=[
            ss.String(noisy_path),
            ss.String(clean_path),
            ss.usize(T),
            ss.f32(sigma_s),
            ss.f32(sigma_r),
            ss.usize(kernel_radius),
        ],
    )

    decompose = app.node("decompose_tiles", func="decompose_tiles", args=[state])

    tiles: list[list] = [[None] * N for _ in range(N)]
    for i in range(N):
        for j in range(N):
            args = [state, ss.usize(i), ss.usize(j)]
            if i == 0 and j == 0:
                args.append(decompose.out(0))
            elif i == 0:
                args.append(tiles[i][j - 1].out(0))
            elif j == 0:
                args.append(tiles[i - 1][j].out(0))
            else:
                args.append(tiles[i - 1][j].out(0))
                args.append(tiles[i][j - 1].out(0))
            tiles[i][j] = app.node(f"tile_{i}_{j}", func="bilateral_filter_tile", args=args)

    border_deps = []
    for i in range(N):
        border_deps.append(tiles[i][N - 1].out(0))
    for j in range(N - 1):
        border_deps.append(tiles[N - 1][j].out(0))

    reassemble = app.node(
        "reassemble_tiles", func="reassemble_tiles", args=[state] + border_deps
    )
    app.node("compute_psnr", func="compute_psnr", args=[state, reassemble.out(0)])

    return app


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SynStream bilateral benchmark sweep")
    p.add_argument("--image-sizes",    type=int,   nargs="+", default=[4096],
                   metavar="N", help="image sizes to sweep")
    p.add_argument("--tile-size",      type=int,   default=256)
    p.add_argument("--kernel-radius",  type=int,   default=4)
    p.add_argument("--sigma-s",        type=float, default=3.0)
    p.add_argument("--sigma-r",        type=float, default=0.1)
    p.add_argument("--workers",        type=int,   nargs="+", default=[1, 2, 4, 8],
                   metavar="W")
    p.add_argument("--system-threads", type=int,   nargs="+", default=[1, 2],
                   metavar="ST",
                   help="system (resolution) thread counts to sweep; reported separately")
    p.add_argument("--iterations",     type=int,   default=10,
                   help="timed runs per configuration")
    p.add_argument("--warmup",         type=int,   default=2,
                   help="untimed warm-up runs")
    p.add_argument("--results-dir",    type=Path,  default=RESULTS)
    p.add_argument("--no-clean",       dest="clean", action="store_false", default=True)
    p.add_argument("--skip-validation", action="store_true",
                   help="skip the one-time PSNR validation run")
    p.add_argument("--inline-continuation", action="store_true",
                   help="enable SynStream inline continuation optimisation")
    p.add_argument("--use-dep", action="store_true",
                   help="use $dep (ordering-only) edges instead of $res, skipping result storage")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()
    args.results_dir.mkdir(parents=True, exist_ok=True)

    # Build plugin once (same Rust code for all configs)
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

    all_rows: list[dict] = []
    total_streams = args.warmup + args.iterations

    for image_size in args.image_sizes:
        S = image_size
        T = args.tile_size
        N = S // T

        # Check input data exists
        noisy_path = DATA_DIR / f"noisy_{S}x{S}.npy"
        if not noisy_path.exists():
            print(f"[SKIP] {noisy_path} not found — run data/generate_input.py first")
            continue

        # ------------------------------------------------------------------
        # Correctness validation (once per image size, not timed)
        # ------------------------------------------------------------------
        if not args.skip_validation:
            print(f"\n=== Validation run | img={S} tile={T} kr={args.kernel_radius} ===",
                  flush=True)
            val_graph = build_validation_graph(
                image_size    = S,
                tile_size     = T,
                sigma_s       = args.sigma_s,
                sigma_r       = args.sigma_r,
                kernel_radius = args.kernel_radius,
            )
            val_timing = args.results_dir / f"val_img{S}_tile{T}_kr{args.kernel_radius}_timing.csv"
            val_graph.run(
                dylib          = dylib,
                workers        = 4,
                core_offset    = 1,
                system_threads = 2,
                slots          = 1,
                max_streams    = 1,
                exclude_streams= 0,
                batching_size  = 1,
                timing         = str(val_timing),
                use_rdtsc      = True,
                custom         = True,
            )
            print(f"  [validation complete — see PSNR in output above]")

        # ------------------------------------------------------------------
        # Timed benchmark sweep — lean graph (N² tasks)
        # ------------------------------------------------------------------
        print(f"\n=== Timed benchmark | img={S} tile={T} kr={args.kernel_radius} "
              f"| grid={N}x{N} ({N*N} tasks) ===", flush=True)

        bench_graph = build_benchmark_graph(
            image_size    = S,
            tile_size     = T,
            sigma_s       = args.sigma_s,
            sigma_r       = args.sigma_r,
            kernel_radius = args.kernel_radius,
            use_dep       = args.use_dep,
        )

        for workers in args.workers:
            for st in args.system_threads:
                ic_suffix  = "_ic"  if args.inline_continuation else ""
                dep_suffix = "_dep" if args.use_dep            else ""
                label = f"synstream_w{workers}_st{st}{ic_suffix}{dep_suffix}"
                tag   = f"ss_bilateral_img{S}_tile{T}_kr{args.kernel_radius}_w{workers}_st{st}{ic_suffix}{dep_suffix}"
                timing_file = args.results_dir / f"{tag}_timing.csv"

                print(
                    f"  workers={workers} system_threads={st} ...",
                    end=" ", flush=True,
                )

                bench_graph.run(
                    dylib                = dylib,
                    workers              = workers,
                    core_offset          = 1,
                    system_threads       = st,
                    slots                = 1,
                    max_streams          = total_streams,
                    exclude_streams      = args.warmup,
                    batching_size        = 1,
                    timing               = str(timing_file),
                    use_rdtsc            = True,
                    custom               = True,
                    inline_continuation  = args.inline_continuation,
                )

                mean_ms, iters = _parse_synstream_timing(timing_file)
                row = {
                    "system":        label,
                    "image_size":    S,
                    "tile_size":     T,
                    "kernel_radius": args.kernel_radius,
                    "workers":       workers,
                    "system_threads":st,
                    "time_ms":       f"{mean_ms:.3f}",
                    "grid_n":        N,
                    "total_tasks":   N * N,
                }
                all_rows.append(row)

                result_csv = args.results_dir / f"{tag}_result.csv"
                with open(result_csv, "w", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=list(row.keys()))
                    w.writeheader()
                    w.writerow(row)
                print(f"{mean_ms:.1f} ms/iter ({iters} iters)")

    # Write merged CSV
    if all_rows:
        merged = args.results_dir / "ss_bilateral_all.csv"
        with open(merged, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_rows[0].keys()))
            w.writeheader()
            w.writerows(all_rows)
        print(f"\nMerged results: {merged}")


if __name__ == "__main__":
    main()
