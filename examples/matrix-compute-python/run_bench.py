"""Matrix-compute-python benchmark.

Python equivalent of examples/matrix-compute/, with compute functions
written in pure NumPy (matcomp.py) decorated with @tomii.export.
The bridge plugin (tomii/_python_bridge/) is compiled automatically on
the first build.

Graph topology (identical to the Rust version):
    gen_vec(buf_size) ──────────────────────────────────────► vec_mat
                      └─► compute_fft ──(barrier)──────────► vec_mat
                                                              └─► mat_mul ──► write_res

Usage
-----
    # Canonical entry point — creates an isolated venv and runs the bench:
    bash examples/matrix-compute-python/run_bench.sh

    # Outside the repo: install tomii and numpy into your own venv first,
    # then run the script directly:
    #   pip install tomii numpy
    #   python run_bench.py

    # Free-threaded Python (no GIL, full multi-core parallelism):
    bash examples/matrix-compute-python/run_bench.sh \\
        --python-interpreter python3.13t

    # Quick iteration (skip clean, fewer streams):
    bash examples/matrix-compute-python/run_bench.sh --no-clean

GIL notes
---------
- Stock Python 3.12:  NumPy matmul/FFT release the GIL internally.
  mat_mul and compute_fft nodes run in parallel across worker threads.
  vec_to_mat (reshape) is fast pure-Python and serialises briefly.
- python3.13t (PEP 703 free-threaded):  zero GIL; all Python code
  runs in parallel. Pass --python-interpreter python3.13t to enable.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent          # examples/matrix-compute-python/
# Ensure the co-located matcomp.py is importable (mirrors a user placing a
# compute module next to their run script before packaging it as a proper lib).
sys.path.insert(0, str(HERE))

import tomii as tm
import matcomp  # noqa: E402  — triggers @tomii.export registration


# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="matrix-compute-python Τομί benchmark")
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--system-threads", type=int, default=3)
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--max-streams", type=int, default=1)
    p.add_argument("--max-runtime", type=int, default=60)
    p.add_argument("--batching-size", type=int, default=1)
    p.add_argument("--batching-limit", type=int, default=10)
    p.add_argument("--exclude-streams", type=int, default=0)
    p.add_argument(
        "--python-interpreter",
        default=None,
        metavar="PATH",
        help="Python interpreter to link the bridge against (default: current interpreter). "
             "Use 'python3.13t' for the free-threaded build.",
    )
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument("--no-record", dest="record", action="store_false", default=True)
    p.add_argument("--no-inits", dest="inits", action="store_false", default=True)
    p.add_argument("--slot-priority", action="store_true", default=False)
    p.add_argument("--debug", action="store_true", default=False)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Graph definition
# ---------------------------------------------------------------------------

def build_graph() -> tm.Graph:
    app = tm.Graph()

    buf_size  = app.var("buf_size",  100)
    num_nodes = app.var("num_nodes", 200)
    result_file = app.var("result_file", tm.String(str(HERE / "result.npz")))

    # Each @tomii.export function is registered once as a py_load_callable init,
    # then referenced via py_call_any / py_call_void at each node invocation.

    gen_vec = app.py_node(
        "gen_vec",
        fn=matcomp.generate_vector,
        factor=num_nodes,
        args=[buf_size],
    )
    compute_fft = app.py_node(
        "compute_fft",
        fn=matcomp.compute_fft,
        factor=num_nodes,
        args=[gen_vec.out()],
    )
    vec_mat = app.py_node(
        "vec_mat",
        fn=matcomp.vec_to_mat,
        factor=num_nodes,
        # gen_vec result + barrier on compute_fft (same pattern as Rust example)
        args=[gen_vec.out(), compute_fft.wait()],
    )
    mat_mul = app.py_node(
        "mat_mul",
        fn=matcomp.mat_mul,
        factor=num_nodes,
        args=[vec_mat.out(), vec_mat.out()],
    )
    app.py_node(
        "write_res",
        fn=matcomp.write_to_file,
        args=[result_file, mat_mul.out(end=num_nodes)],
    )

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    args = _parse_args()

    out_file     = HERE / "result.npz"
    timing_file  = HERE / "timing.txt"
    report_file  = HERE / "report.txt"
    for f in [out_file, timing_file, report_file]:
        f.unlink(missing_ok=True)

    app = build_graph()

    app.build(
        python_plugin=True,
        python_interpreter=args.python_interpreter,
        release=True,
        clean=args.clean,
    )

    app.run(
        workers=args.workers,
        system_threads=args.system_threads,
        slots=args.slots,
        max_streams=args.max_streams,
        max_runtime=args.max_runtime,
        batching_size=args.batching_size,
        batching_limit=args.batching_limit,
        exclude_streams=args.exclude_streams,
        timing=str(timing_file),
        record=args.record,
        inits=args.inits,
        slot_priority=args.slot_priority,
        debug=args.debug,
        report=str(report_file),
    )


if __name__ == "__main__":
    main()
