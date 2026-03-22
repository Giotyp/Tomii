#!/usr/bin/env python3
"""Python wrapper for the Taskflow wavefront benchmark (harness interface).

Builds the C++ binary if needed, runs it, calls the verifier, and writes
a report.json — matching the interface expected by the agent-bench harness.

Usage (as invoked by the harness):
    python run_wavefront.py \\
        --n 64 --workers 4 --iterations 10 \\
        --report report.json
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _find_verifier() -> Path:
    """Walk up from HERE to find agent-bench/tools/verify_wavefront.py."""
    p = HERE
    for _ in range(10):
        v = p / "agent-bench" / "tools" / "verify_wavefront.py"
        if v.exists():
            return v
        p = p.parent
    raise FileNotFoundError("verify_wavefront.py not found in any parent directory")


def main() -> None:
    p = argparse.ArgumentParser(description="Taskflow Wavefront benchmark")
    p.add_argument("--n",          type=int, default=64)
    p.add_argument("--workers",    type=int, default=4)
    p.add_argument("--iterations", type=int, default=10)
    p.add_argument("--warmup",     type=int, default=2)
    p.add_argument("--report",     default=None)
    args = p.parse_args()

    # Build binary if needed
    binary = HERE / "wavefront"
    if not binary.exists():
        r = subprocess.run(["make"], cwd=HERE, text=True, capture_output=True)
        print(r.stdout, end="")
        if r.stderr:
            print(r.stderr, end="", file=sys.stderr)
        if r.returncode != 0:
            sys.exit(r.returncode)

    # Run binary
    cmd = [
        str(binary),
        "--n",          str(args.n),
        "--workers",    str(args.workers),
        "--iterations", str(args.iterations),
        "--warmup",     str(args.warmup),
    ]
    r = subprocess.run(cmd, text=True, capture_output=True)
    print(r.stdout, end="")
    if r.stderr:
        print(r.stderr, end="", file=sys.stderr)
    if r.returncode != 0:
        sys.exit(r.returncode)

    # Parse avg latency from summary line: "... | 0.0001s/iter"
    m = re.search(r'\|\s*([\d.]+)s/iter', r.stdout)
    avg_latency_us = float(m.group(1)) * 1e6 if m else None

    # Parse corner value for verifier: "CORNER: 1.234567890123456"
    m_corner = re.search(r'CORNER:\s*([\d.e+\-]+)', r.stdout)
    if not m_corner:
        print("ERROR: CORNER not found in binary stdout", file=sys.stderr)
        sys.exit(1)
    corner_val = float(m_corner.group(1))

    # Verify correctness (prints PASS to stdout → captured in run.log)
    verifier = _find_verifier()
    subprocess.run(
        [sys.executable, str(verifier), "--n", str(args.n), "--corner", str(corner_val)],
        check=True,
    )

    # Write report.json
    report = {"summary": {"avg_latency_us": avg_latency_us}}
    report_path = Path(args.report) if args.report else HERE / "report.json"
    report_path.write_text(json.dumps(report, indent=2))
    print(f"  -> report written to {report_path}", flush=True)


if __name__ == "__main__":
    main()
