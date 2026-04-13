"""matrix-compute-C benchmark — Python runner using the SynStream API.

Implements the same FFT + matrix-multiply DAG as examples/matrix-compute/
but calls C functions (FFTW + OpenBLAS) via a compiled libmatcomp_c.so.

The C library is compiled with Make, then synstream-core is rebuilt with
FUNC_PATH pointing at the C header so the converter generates the C wrappers.

Usage (from repo root, with venv active):
    python examples/matrix-compute-C/run_bench.py
    python examples/matrix-compute-C/run_bench.py --workers 4 --no-clean
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent        # examples/matrix-compute-C/
REPO_ROOT = HERE.parents[1]                   # workspace root
sys.path.insert(0, str(REPO_ROOT))

import synstream as ss                        # noqa: E402

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="matrix-compute-C SynStream benchmark")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--system-threads", type=int, default=3)
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--max-streams", type=int, default=1)
    p.add_argument("--max-runtime", type=int, default=60)
    p.add_argument("--batching-size", type=int, default=1)
    p.add_argument("--batching-limit", type=int, default=10)
    p.add_argument("--exclude-streams", type=int, default=0)
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument("--no-record", dest="record", action="store_false", default=True)
    p.add_argument("--no-inits", dest="inits", action="store_false", default=True)
    p.add_argument("--slot-priority", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph (same DAG as examples/matrix-compute/run_bench.py)
# ---------------------------------------------------------------------------


def build_graph() -> ss.Graph:
    app = ss.Graph()

    # Initializations
    buf_size   = app.var("buf_size",   100)
    num_nodes  = app.var("num_nodes",  200)
    fft_planner = app.var(
        "fft_planner",
        func="fft_planner",
        args=[buf_size],
    )
    result_file = app.var(
        "result_file",
        func="get_out_file",
        args=[ss.String("SCRIPT_DIR"), ss.String("result.txt")],
    )

    # Pipeline nodes
    gen_vec = app.node(
        "gen_vec",
        func="generate_vector",
        factor=num_nodes,
        args=[buf_size],
    )
    compute_fft = app.node(
        "compute_fft",
        func="compute_fft",
        factor=num_nodes,
        args=[fft_planner, gen_vec.out()],
    )
    vec_mat = app.node(
        "vec_mat",
        func="vec_to_mat",
        factor=num_nodes,
        args=[gen_vec.out(), compute_fft.wait()],
    )
    mat_mul = app.node(
        "mat_mul",
        func="mat_mul",
        factor=num_nodes,
        args=[vec_mat.out(), vec_mat.out()],
    )
    app.node(
        "write_res",
        func="write_to_file",
        args=[result_file, mat_mul.out(end=num_nodes)],
    )

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = _parse_args()

    c_lib  = HERE / "libmatcomp_c.so"
    header = HERE / "include" / "matcomp.h"
    env    = {"SCRIPT_DIR": str(HERE)}

    # ── Step 1: build the C shared library ─────────────────────────────────
    print("==> Building C library (make)...")
    result = subprocess.run(["make", "-C", str(HERE)], check=False)
    if result.returncode != 0:
        sys.exit(f"make failed (exit {result.returncode})")

    # ── Step 2: build synstream-core with C header wrappers ─────────────────
    # FUNC_PATH → matcomp.h  →  converter generates wrappers.rs for C
    app = build_graph()
    app.build(
        func_path=str(header),
        # No plugin_manifest — the C library is built by Make above.
        release=True,
        clean=args.clean,
        env=env,
    )

    # ── Step 3: run ─────────────────────────────────────────────────────────
    # Pass the C dylib explicitly; main.rs sets PLUGIN_LIB from --dylib.
    out_file    = HERE / "out.txt"
    timing_file = HERE / "timing.txt"
    report_file = HERE / "report.txt"
    out_file.unlink(missing_ok=True)
    timing_file.unlink(missing_ok=True)
    report_file.unlink(missing_ok=True)

    app.run(
        dylib=str(c_lib),
        env=env,
        workers=args.workers,
        system_threads=args.system_threads,
        slots=args.slots,
        max_streams=args.max_streams,
        max_runtime=args.max_runtime,
        batching_size=args.batching_size,
        batching_limit=args.batching_limit,
        exclude_streams=args.exclude_streams,
        output=str(out_file),
        timing=str(timing_file),
        record=args.record,
        inits=args.inits,
        slot_priority=args.slot_priority,
        debug=args.debug,
        report=str(report_file),
    )

    print(f"==> Done. Results written to {HERE / 'result.txt'}")


if __name__ == "__main__":
    main()
