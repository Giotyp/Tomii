"""Taskflow 4-stage linear pipeline benchmark driver.

Builds the CMake project, then sweeps S (slots) and W (workers), writing
results to build/tf_pipeline_sweep.csv in the same format as the Tomii
pipeline_sweep.csv so the two can be compared directly.

Usage (from bench worktree root):
    python pipeline-bench/taskflow/run_bench.py
    python pipeline-bench/taskflow/run_bench.py --slots 1 4 16 --workers 4 8
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent   # pipeline-bench/taskflow/
BUILD_DIR = HERE / "build"


# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

def build_cmake(clean: bool) -> None:
    """Configure and build the CMake project."""
    BUILD_DIR.mkdir(parents=True, exist_ok=True)

    if clean and (BUILD_DIR / "CMakeCache.txt").exists():
        print("Cleaning build directory...", flush=True)
        subprocess.run(["cmake", "--build", str(BUILD_DIR), "--target", "clean"],
                       check=False)

    print("Configuring CMake...", flush=True)
    subprocess.run(
        ["cmake", "-S", str(HERE), "-B", str(BUILD_DIR),
         "-DCMAKE_BUILD_TYPE=Release"],
        check=True,
    )

    print("Building tf_pipeline...", flush=True)
    subprocess.run(
        ["cmake", "--build", str(BUILD_DIR), "--", "-j4"],
        check=True,
    )

    binary = BUILD_DIR / "tf_pipeline"
    if not binary.exists():
        print(f"ERROR: binary not found at {binary}", file=sys.stderr)
        sys.exit(1)
    print(f"  binary: {binary}", flush=True)


# ---------------------------------------------------------------------------
# Single (S, W) run
# ---------------------------------------------------------------------------

def run_one(
    *,
    n: int,
    slots: int,
    workers: int,
    streams: int,
    warmup: int,
    mode: str,
    output_csv: Path,
) -> None:
    """Invoke tf_pipeline for one (slots, workers) configuration."""
    binary = BUILD_DIR / "tf_pipeline"
    cmd = [
        str(binary),
        "--n",       str(n),
        "--slots",   str(slots),
        "--workers", str(workers),
        "--streams", str(streams),
        "--warmup",  str(warmup),
        "--mode",    mode,
        "--output",  str(output_csv),
    ]
    print(
        f"\n=== Taskflow | n={n}  slots={slots}  workers={workers}"
        f"  mode={mode} ===",
        flush=True,
    )
    subprocess.run(cmd, check=True)


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Taskflow pipeline benchmark sweep over slots and workers."
    )
    p.add_argument("--n", type=int, default=256,
                   help="items per stream (pipeline width)")
    p.add_argument("--slots", type=int, nargs="+", default=[1, 4, 16, 64],
                   help="concurrent slot counts to sweep")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8],
                   help="worker thread counts to sweep")
    p.add_argument("--streams", type=int, default=2000,
                   help="total streams to process (excluding warmup)")
    p.add_argument("--warmup", type=int, default=200,
                   help="warmup streams excluded from timing")
    p.add_argument("--mode", default="clone",
                   choices=["clone", "sequential"],
                   help="Taskflow execution mode")
    p.add_argument("--csv-out", type=Path, default=None,
                   help="output CSV path (default: build/tf_pipeline_sweep_heavy.csv)")
    p.add_argument("--no-clean", dest="clean", action="store_false",
                   default=True,
                   help="skip cmake clean before building")
    args = p.parse_args()

    build_cmake(args.clean)

    output_csv = args.csv_out or (BUILD_DIR / "tf_pipeline_sweep_heavy.csv")

    # Remove stale CSV so headers are written fresh by the binary.
    if output_csv.exists():
        output_csv.unlink()

    for w in args.workers:
        for s in args.slots:
            run_one(
                n=args.n,
                slots=s,
                workers=w,
                streams=args.streams,
                warmup=args.warmup,
                mode=args.mode,
                output_csv=output_csv,
            )

    print(f"\nResults written to: {output_csv}", flush=True)


if __name__ == "__main__":
    main()
