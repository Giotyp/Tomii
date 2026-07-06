"""Agent-tuning harness — shared infrastructure for all four search arms.

Workload-agnostic since the M3 expansion: benchmark specifics (builds,
evaluation, verifier, baseline) live in `workloads.py`; this module keeps
trial logging, baseline establishment, and the CLI plumbing arms share.

Usage (standalone — establish a baseline):
    python harness.py --workload stream-analytics --results-dir results/baseline_run
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from workloads import (  # noqa: F401  (EvalResult re-exported for the arms)
    EvalResult,
    Workload,
    get_workload,
    workload_names,
)

_HERE = Path(__file__).resolve().parent


@dataclass
class TrialRecord:
    iteration: int
    knobs: dict[str, Any]
    result: EvalResult
    arm: str
    notes: str = ""


# ---------------------------------------------------------------------------
# Arm CLI plumbing
# ---------------------------------------------------------------------------


def add_common_args(p: argparse.ArgumentParser) -> None:
    """Arguments shared by every arm script."""
    p.add_argument(
        "--workload",
        default="stream-analytics",
        choices=workload_names(),
        help="benchmark to tune (see workloads.py)",
    )
    p.add_argument("--iterations", type=int, default=50)
    p.add_argument("--streams", type=int, default=500)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument(
        "--results-dir",
        type=Path,
        default=None,
        help="defaults to results/<workload>/",
    )
    p.add_argument(
        "--no-graph-knobs",
        action="store_true",
        help="restrict the search to runtime (CLI) knobs",
    )


def setup_arm(args: argparse.Namespace) -> tuple[Workload, dict[str, Any], Path]:
    """Resolve workload, knob space (honouring --no-graph-knobs), results dir."""
    workload = get_workload(args.workload)
    space = workload.knob_space()
    if args.no_graph_knobs:
        space = {**space, "knobs": [k for k in space["knobs"] if k["kind"] == "cli"]}
    results_dir = args.results_dir or Path("results") / args.workload
    results_dir.mkdir(parents=True, exist_ok=True)
    return workload, space, results_dir


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------


def establish_baseline(
    streams: int = 500,
    warmup: int = 50,
    results_dir: Path | None = None,
    workload: Workload | None = None,
) -> float:
    """Run the workload's baseline knobs and return ms_per_stream.

    Writes results_dir/baseline.json. Falls back to 0.0 on failure.
    """
    if workload is None:
        workload = get_workload("stream-analytics")
    knobs = dict(workload.baseline_knobs)
    print(
        f"[harness] establishing {workload.name} baseline with default knobs ...",
        flush=True,
    )
    result = workload.evaluate(knobs, streams=streams, warmup=warmup)

    if not result.verifier_ok or result.ms_per_stream is None:
        reason = result.rejection_reason or "unknown"
        print(f"[harness] WARNING: baseline run failed: {reason}", flush=True)
        baseline_ms = 0.0
    else:
        baseline_ms = result.ms_per_stream
        print(
            f"[harness] baseline = {baseline_ms:.4f} ms/stream  "
            f"(wall {result.wall_seconds:.1f}s)",
            flush=True,
        )

    if results_dir is not None:
        results_dir.mkdir(parents=True, exist_ok=True)
        data: dict[str, Any] = {
            "workload": workload.name,
            "baseline_ms_per_stream": baseline_ms,
            "verifier_ok": result.verifier_ok,
            "rejection_reason": result.rejection_reason,
            "wall_seconds": result.wall_seconds,
            "knobs": knobs,
        }
        (results_dir / "baseline.json").write_text(json.dumps(data, indent=2))

    return baseline_ms


# ---------------------------------------------------------------------------
# Back-compat wrappers (pre-expansion API, pinned to stream-analytics)
# ---------------------------------------------------------------------------


def evaluate(
    knobs: dict[str, Any],
    streams: int = 500,
    warmup: int = 50,
    space: dict[str, Any] | None = None,
) -> EvalResult:
    """Evaluate on stream-analytics (legacy single-workload entry point)."""
    return get_workload("stream-analytics").evaluate(
        knobs, streams=streams, warmup=warmup, space=space
    )


def load_knob_space() -> dict[str, Any]:
    """Knob space for stream-analytics (legacy single-workload entry point)."""
    return get_workload("stream-analytics").knob_space()


# ---------------------------------------------------------------------------
# Trial logging
# ---------------------------------------------------------------------------


def log_trial(record: TrialRecord, log_file: Path) -> None:
    """Append a JSON line to log_file with all trial fields."""
    entry: dict[str, Any] = {
        "iteration": record.iteration,
        "arm": record.arm,
        "notes": record.notes,
        "knobs": record.knobs,
        "verifier_ok": record.result.verifier_ok,
        "ms_per_stream": record.result.ms_per_stream,
        "rejection_reason": record.result.rejection_reason,
        "wall_seconds": record.result.wall_seconds,
    }
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# CLI (standalone use)
# ---------------------------------------------------------------------------


def _main() -> None:
    p = argparse.ArgumentParser(description="agent-tuning harness — establish baseline")
    add_common_args(p)
    args = p.parse_args()

    workload, _space, results_dir = setup_arm(args)
    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    print(f"baseline ms/stream: {baseline:.4f}")


if __name__ == "__main__":
    _main()
