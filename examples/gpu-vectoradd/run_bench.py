"""gpu-vectoradd benchmark — Python runner using the Τομί API.

Implements a 6-node GPU vector-add DAG using CUDA C++ plugin functions
compiled from src/gpu_vadd.cu via nvcc.

Pipeline per stream:
  gen_vec_a ─► copy_h2d_a ─►─┐
                              ├─► vadd_gpu ─► copy_d2h ─► validate
  gen_vec_b ─► copy_h2d_b ─►─┘

GPU nodes (copy_h2d_a, copy_h2d_b, vadd_gpu, copy_d2h) are pinned to workers
0-1 via use_workers="0-1", giving two dedicated GPU proxy threads each with
their own per-thread CUDA stream (--default-stream per-thread in Makefile).

Usage (from repo root, with venv active):
    python examples/gpu-vectoradd/run_bench.py
    python examples/gpu-vectoradd/run_bench.py --workers 4 --max-streams 32
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

HERE      = Path(__file__).resolve().parent   # examples/gpu-vectoradd/
REPO_ROOT = HERE.parents[1]                   # workspace root
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="gpu-vectoradd Τομί benchmark")
    p.add_argument("--workers",        type=int, default=4)
    p.add_argument("--system-threads", type=int, default=3)
    p.add_argument("--slots",          type=int, default=2)
    p.add_argument("--max-streams",    type=int, default=4)
    p.add_argument("--max-runtime",    type=int, default=60)
    p.add_argument("--vec-size",       type=int, default=1024 * 1024,
                   help="Number of float elements per vector (default 1M)")
    p.add_argument("--no-clean",  dest="clean",  action="store_false", default=True)
    p.add_argument("--no-record", dest="record", action="store_false", default=True)
    p.add_argument("--debug",     action="store_true", default=False)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------


def build_graph(vec_size: int) -> tm.Graph:
    app = tm.Graph()

    # Initializations
    buf_size = app.var("buf_size", vec_size)
    seed_a   = app.var("seed_a",   tm.u64(1))
    seed_b   = app.var("seed_b",   tm.u64(2))

    # CPU: generate host vectors
    gen_vec_a = app.node("gen_vec_a", func="generate_host_vec", args=[buf_size, seed_a])
    gen_vec_b = app.node("gen_vec_b", func="generate_host_vec", args=[buf_size, seed_b])

    # GPU: copy to device (pinned to workers 0-1)
    cp_h2d_a = app.node("cp_h2d_a", func="copy_h2d",
                         args=[gen_vec_a.out()], use_workers="0-1")
    cp_h2d_b = app.node("cp_h2d_b", func="copy_h2d",
                         args=[gen_vec_b.out()], use_workers="0-1")

    # GPU: vector add (consumes and frees both device inputs)
    vadd = app.node("vadd", func="vadd_gpu",
                    args=[cp_h2d_a.out(), cp_h2d_b.out()], use_workers="0-1")

    # GPU: copy result back to host (consumes and frees device output)
    cp_d2h = app.node("cp_d2h", func="copy_d2h",
                       args=[vadd.out(), buf_size], use_workers="0-1")

    # CPU: validate gpu_result == host_a + host_b element-wise
    app.node("validate", func="validate",
             args=[cp_d2h.out(), gen_vec_a.out(), gen_vec_b.out()])

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    c_lib  = HERE / "libgpu_vadd.so"
    header = HERE / "include" / "gpu_vadd.h"

    # ── Step 1: build the CUDA shared library ─────────────────────────────
    print("==> Building CUDA library (make)...")
    result = subprocess.run(["make", "-C", str(HERE)], check=False)
    if result.returncode != 0:
        sys.exit(f"make failed (exit {result.returncode})")

    # ── Step 2: build tomii-core with C-header wrappers ──────────────────
    # FUNC_PATH → gpu_vadd.h → converter generates C-dispatch wrappers
    app = build_graph(args.vec_size)
    app.build(
        func_path=str(header),
        release=True,
        clean=args.clean,
    )

    # ── Step 3: run ──────────────────────────────────────────────────────
    timing_file = HERE / "timing.txt"
    report_file = HERE / "report.txt"
    timing_file.unlink(missing_ok=True)
    report_file.unlink(missing_ok=True)

    # Target GPU 5 by default; override with CUDA_VISIBLE_DEVICES env var.
    import os
    run_env = {"CUDA_VISIBLE_DEVICES": os.environ.get("CUDA_VISIBLE_DEVICES", "5")}

    app.run(
        dylib=str(c_lib),
        env=run_env,
        workers=args.workers,
        system_threads=args.system_threads,
        slots=args.slots,
        max_streams=args.max_streams,
        max_runtime=args.max_runtime,
        timing=str(timing_file),
        record=args.record,
        debug=args.debug,
        report=str(report_file),
    )

    print(f"==> Done. Timing written to {timing_file}")


if __name__ == "__main__":
    main()
