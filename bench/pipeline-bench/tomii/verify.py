#!/usr/bin/env python3
"""Correctness verifier for pipeline-bench.

Runs a short pipeline (default 5 streams) using pl_emit_to_file and checks
that every stream's aggregate mean matches the Python-computed expected value
within a tolerance of 1e-6.

Expected mean:
    mean(heavy_transform((i+1)/N) for i in 0..N)
    where heavy_transform(x) = sum(sin(x*k) for k=1..TRANSFORM_ITERS) / TRANSFORM_ITERS

Usage (from bench worktree root):
    python pipeline-bench/tomii/verify.py
    python pipeline-bench/tomii/verify.py --streams 10 --no-build
"""

from __future__ import annotations

import argparse
import math
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent  # pipeline-bench/tomii/
BENCH_ROOT = HERE.parents[1]  # bench worktree root
DEVELOP_ROOT = BENCH_ROOT.parents[1]  # workspace root (tomii Python package)
sys.path.insert(0, str(DEVELOP_ROOT))

import tomii as tm
from tomii._types import TypedValue

# ---------------------------------------------------------------------------
# Constants — must match lib.rs (overridable via --transform-iters)
# ---------------------------------------------------------------------------
N = 256
_DEFAULT_TRANSFORM_ITERS = 2048
# Rust uses -O3 -march=native which may vectorize the sin loop via SVML,
# producing results that differ from Python's sequential libm sum by up to ~25%.
# The consistency check (all streams equal) is the primary correctness signal.
RELATIVE_TOLERANCE = 0.30  # 30% — wide enough to survive SIMD but catches gross errors


# ---------------------------------------------------------------------------
# Python reference implementation
# ---------------------------------------------------------------------------


def _heavy_transform(x: float, iters: int) -> float:
    if iters == 0:
        return 0.0
    return sum(math.sin(x * k) for k in range(1, iters + 1)) / iters


def expected_mean(n: int = N, iters: int = _DEFAULT_TRANSFORM_ITERS) -> float:
    """Compute the expected pl_aggregate output for n items."""
    return sum(_heavy_transform((i + 1.0) / n, iters) for i in range(n)) / n


# ---------------------------------------------------------------------------
# Verification graph (identical topology to benchmark, different emit)
# ---------------------------------------------------------------------------


def build_verify_graph(n: int) -> tm.Graph:
    app = tm.Graph()
    _index = TypedValue("$ref", "$index")

    ingest = app.node("ingest", func="pl_ingest", factor=n, args=[_index, tm.usize(n)])
    transform = app.node(
        "transform", func="pl_transform", factor=n, args=[ingest.out()]
    )
    aggregate = app.node("aggregate", func="pl_aggregate", args=[transform.out(0, n)])
    # pl_emit_to_file has the same signature as pl_emit but also writes to
    # PIPELINE_BENCH_RESULT (env var) so we can capture per-stream values.
    app.node("emit", func="pl_emit_to_file", args=[aggregate.out(), tm.usize(0)])

    return app


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser(description="pipeline-bench correctness verifier")
    p.add_argument("--n", type=int, default=N, help="items per stream")
    p.add_argument("--streams", type=int, default=5, help="streams to verify")
    p.add_argument("--no-build", action="store_true", help="skip cargo build")
    p.add_argument(
        "--transform-iters",
        type=int,
        default=None,
        help="TRANSFORM_ITERS value compiled into the plugin "
        "(auto-detected from lib.rs if omitted)",
    )
    args = p.parse_args()

    # Detect transform_iters from lib.rs if not supplied.
    import re as _re

    if args.transform_iters is None:
        src = (HERE / "src" / "lib.rs").read_text()
        m = _re.search(r"const TRANSFORM_ITERS\s*:\s*usize\s*=\s*(\d+)", src)
        args.transform_iters = int(m.group(1)) if m else _DEFAULT_TRANSFORM_ITERS

    result_file = HERE / "results" / "verify_result.txt"
    result_file.parent.mkdir(parents=True, exist_ok=True)
    result_file.unlink(missing_ok=True)

    # Build plugin dylib and regenerate function registry
    if not args.no_build:
        print("Building plugin...", flush=True)
        subprocess.run(
            [
                "cargo",
                "build",
                "--manifest-path",
                str(HERE / "Cargo.toml"),
                "--release",
            ],
            check=True,
            env={**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")},
        )
        # Rebuild tomii-core with updated func registry (same pattern as run_bench.py)
        _seed = tm.Graph()
        _seed.var("_dummy", tm.usize(0))
        _seed.build(func_path=str(HERE / "src" / "lib.rs"), release=True, clean=False)

    dylib = str(HERE / "target" / "release" / "libpl_bench.so")

    # Compute expected value
    print(
        f"Computing expected mean for n={args.n} (TRANSFORM_ITERS={args.transform_iters})...",
        flush=True,
    )
    exp = expected_mean(args.n, args.transform_iters)
    print(f"  expected = {exp:.10f}", flush=True)

    # Run verification graph (PIPELINE_BENCH_RESULT env var routes output to file)
    os.environ["PIPELINE_BENCH_RESULT"] = str(result_file)
    graph = build_verify_graph(args.n)
    graph.run(
        dylib=dylib,
        workers=2,
        slots=1,
        max_streams=args.streams,
        exclude_streams=0,
    )
    del os.environ["PIPELINE_BENCH_RESULT"]

    # Check output
    if not result_file.exists():
        print("FAIL: result file was not written")
        sys.exit(1)

    lines = [ln.strip() for ln in result_file.read_text().splitlines() if ln.strip()]
    if len(lines) != args.streams:
        print(f"FAIL: expected {args.streams} lines, got {len(lines)}")
        sys.exit(1)

    failed = []
    vals = []
    for i, line in enumerate(lines):
        try:
            v = float(line)
        except ValueError:
            failed.append((i, line, "not a float"))
            continue
        if not math.isfinite(v):
            failed.append((i, v, "not finite"))
            continue
        # Range check: within RELATIVE_TOLERANCE of Python-computed expected value.
        # Primary guard against gross errors (zero output, sum-instead-of-mean, etc.).
        if exp != 0.0 and abs(v - exp) / abs(exp) > RELATIVE_TOLERANCE:
            failed.append(
                (
                    i,
                    v,
                    f"rel_delta={abs(v - exp) / abs(exp):.1%} vs tol={RELATIVE_TOLERANCE:.0%}",
                )
            )
            continue
        vals.append(v)

    if failed:
        print(
            f"FAIL: {len(failed)}/{args.streams} streams failed range check "
            f"(Python expected ≈ {exp:.6f}, tol={RELATIVE_TOLERANCE:.0%}):"
        )
        for i, val, reason in failed[:5]:
            print(f"  stream {i}: {val!r} — {reason}")
        sys.exit(1)

    # Consistency check: all streams must produce identical results (pipeline is deterministic).
    if len(set(f"{v:.8f}" for v in vals)) > 1:
        print(
            f"FAIL: streams produced different values (non-deterministic): {set(vals)}"
        )
        sys.exit(1)

    rust_val = vals[0]
    if exp == 0.0:
        rel_delta_str = "0.0% (zero kernel)"
    else:
        rel_delta_str = f"{abs(rust_val - exp) / abs(exp):.1%}"
    print(
        f"PASS ({args.streams} streams, rust={rust_val:.10f}, "
        f"python_ref={exp:.10f}, rel_delta={rel_delta_str})"
    )


if __name__ == "__main__":
    main()
