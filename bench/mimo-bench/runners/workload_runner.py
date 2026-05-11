#!/usr/bin/env python3
"""
Orchestrate a full Phase A → Phase B → Verify round for one or more MIMO workloads.

Usage:
    python runners/workload_runner.py --workloads 8x8 16x16 64x8 64x16
    python runners/workload_runner.py --workloads 16x16 --skip-phase-a
    python runners/workload_runner.py --workloads 8x8 --phase-a-only

The runner invokes harness.py per workload with the appropriate --reference-report
pointing at the matching report.json from examples/mimolib (synstream-sosp prior results
are used as the baseline reference if available; otherwise the current mimolib report.json
is used after a fresh baseline run).

Prior-round results are read from:
  synstream-sosp worktree: ../synstream-sosp/mimo-bench/results/run_<workload>/
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
HARNESS = SCRIPT_DIR.parent / "harness.py"
MIMOLIB_DIR = SCRIPT_DIR.parent.parent / "examples" / "mimolib"
BENCH_RESULTS = SCRIPT_DIR.parent / "results"

PRIOR_RESULTS_ROOT = Path(__file__).parents[4] / "synstream-sosp" / "mimo-bench" / "results"

WORKLOAD_NAMES = ["8x8", "16x16", "64x8", "64x16"]


def find_reference_report(workload: str) -> Path | None:
    """Find the most-recent prior-round summary for this workload and extract its reference report."""
    workload_dir = PRIOR_RESULTS_ROOT / f"run_{workload}"
    if not workload_dir.exists():
        return None
    run_dirs = sorted(workload_dir.iterdir(), key=lambda p: p.name)
    for run_dir in reversed(run_dirs):
        ref = run_dir / "reference_report.json"
        if ref.exists():
            return ref
    return None


def run_workload(workload: str, args: argparse.Namespace) -> dict:
    print(f"\n{'#'*70}")
    print(f"# Workload: {workload}")
    print(f"{'#'*70}")

    ref_report = find_reference_report(workload)
    if ref_report is None:
        # Fall back to current mimolib report.json (requires a fresh baseline run first)
        ref_report = MIMOLIB_DIR / "report.json"
        if not ref_report.exists():
            print(f"[ERROR] No reference report for workload {workload}. Run examples/mimolib/scripts/run_mimo.sh first.")
            return {"workload": workload, "error": "no_reference_report"}
    print(f"[INFO] Reference report: {ref_report}")

    cmd = [
        sys.executable, str(HARNESS),
        "--reference-report", str(ref_report),
        "--workload", workload,
        "--output-dir", str(BENCH_RESULTS),
        "--llm-iters", str(args.llm_iters),
        "--verify-trials", str(args.verify_trials),
    ]
    if args.model:
        cmd += ["--model", args.model]
    if args.budget:
        cmd += ["--budget", str(args.budget)]
    if args.skip_phase_a:
        cmd.append("--skip-phase-a")
    if args.phase_a_only:
        cmd.append("--phase-a-only")

    print(f"[INFO] Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return {"workload": workload, "returncode": result.returncode}


def main():
    parser = argparse.ArgumentParser(description="Run MIMO optimization rounds across workloads")
    parser.add_argument("--workloads", nargs="+", choices=WORKLOAD_NAMES + ["all"],
                        default=["all"], metavar="WL",
                        help="Workloads to run (8x8 16x16 64x8 64x16 or 'all')")
    parser.add_argument("--llm-iters", type=int, default=15)
    parser.add_argument("--verify-trials", type=int, default=3)
    parser.add_argument("--model", default="")
    parser.add_argument("--budget", type=float, default=0.0)
    parser.add_argument("--skip-phase-a", action="store_true")
    parser.add_argument("--phase-a-only", action="store_true")
    args = parser.parse_args()

    workloads = WORKLOAD_NAMES if "all" in args.workloads else args.workloads
    print(f"[INFO] Running workloads: {workloads}")

    outcomes = []
    for wl in workloads:
        outcome = run_workload(wl, args)
        outcomes.append(outcome)

    print(f"\n{'='*60}")
    print("[DONE] All workloads complete")
    for o in outcomes:
        status = "OK" if o.get("returncode") == 0 else f"FAIL({o.get('returncode') or o.get('error')})"
        print(f"  {o['workload']:8s} {status}")


if __name__ == "__main__":
    main()
