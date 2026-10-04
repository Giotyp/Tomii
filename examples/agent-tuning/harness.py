"""Agent-tuning harness — shared infrastructure for all four search arms.

Workload-agnostic since the M3 expansion: benchmark specifics (builds,
evaluation, verifier, baseline) live in `workloads.py`; this module keeps
trial logging, baseline establishment, and the CLI plumbing arms share.

Usage (standalone — establish a baseline):
    python harness.py --workload stream-analytics --results-dir results/baseline_run
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from workloads import (  # noqa: F401  (EvalResult re-exported for the arms)
    EvalResult,
    Workload,
    get_workload,
    workload_names,
)

_HERE = Path(__file__).resolve().parent

#: Exclusive measurement lock shared by every eval-campaign experiment
#: (EVAL_PROTOCOL.md).  Held per trial only — never across LLM calls.
LOCK_PATH = Path(
    os.environ.get("E8_LOCK_PATH", "/home/george/Tomii/.eval-server.lock")
)


@dataclass
class TrialRecord:
    iteration: int
    knobs: dict[str, Any]
    result: EvalResult
    arm: str
    notes: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Measurement lock + per-trial evaluation
# ---------------------------------------------------------------------------

#: Accumulated lock-wait seconds for the current process (excluded from the
#: arm's reported wall time: waiting on other experiments is not search cost).
LOCK_WAIT_S = [0.0]


@contextlib.contextmanager
def server_lock():
    """flock(2) on the campaign lock file — same lock as `flock(1)`."""
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fh = LOCK_PATH.open("a")
    t0 = time.monotonic()
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
    LOCK_WAIT_S[0] += time.monotonic() - t0
    try:
        yield
    finally:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


def run_trial(
    workload: Workload,
    knobs: dict[str, Any],
    args: argparse.Namespace,
    space: dict[str, Any],
) -> EvalResult:
    """Evaluate one configuration while holding the measurement lock."""
    with server_lock():
        return workload.evaluate(
            knobs, frames=args.frames, warmup=args.warmup, space=space
        )


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
    p.add_argument("--frames", type=int, default=500)
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
    p.add_argument("--seed", type=int, default=0, help="arm RNG seed / run index")


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
    frames: int = 500,
    warmup: int = 50,
    results_dir: Path | None = None,
    workload: Workload | None = None,
) -> float:
    """Run the workload's baseline knobs and return ms_per_frame.

    Writes results_dir/baseline.json. Falls back to 0.0 on failure.
    """
    if workload is None:
        workload = get_workload("stream-analytics")
    knobs = dict(workload.baseline_knobs)
    print(
        f"[harness] establishing {workload.name} baseline with default knobs ...",
        flush=True,
    )
    with server_lock():
        result = workload.evaluate(knobs, frames=frames, warmup=warmup)

    if not result.verifier_ok or result.ms_per_frame is None:
        reason = result.rejection_reason or "unknown"
        print(f"[harness] WARNING: baseline run failed: {reason}", flush=True)
        baseline_ms = 0.0
    else:
        baseline_ms = result.ms_per_frame
        print(
            f"[harness] baseline = {baseline_ms:.4f} ms/frame  "
            f"(wall {result.wall_seconds:.1f}s)",
            flush=True,
        )

    if results_dir is not None:
        results_dir.mkdir(parents=True, exist_ok=True)
        data: dict[str, Any] = {
            "workload": workload.name,
            "baseline_ms_per_frame": baseline_ms,
            "verifier_ok": result.verifier_ok,
            "rejection_reason": result.rejection_reason,
            "wall_seconds": result.wall_seconds,
            "knobs": knobs,
            "report": result.report,
        }
        (results_dir / "baseline.json").write_text(json.dumps(data, indent=2))

    return baseline_ms


# ---------------------------------------------------------------------------
# Back-compat wrappers (pre-expansion API, pinned to stream-analytics)
# ---------------------------------------------------------------------------


def evaluate(
    knobs: dict[str, Any],
    frames: int = 500,
    warmup: int = 50,
    space: dict[str, Any] | None = None,
) -> EvalResult:
    """Evaluate on stream-analytics (legacy single-workload entry point)."""
    return get_workload("stream-analytics").evaluate(
        knobs, frames=frames, warmup=warmup, space=space
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
        "ms_per_frame": record.result.ms_per_frame,
        "rejection_reason": record.result.rejection_reason,
        "wall_seconds": record.result.wall_seconds,
        "report": record.result.report,
        **record.extra,
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
        frames=args.frames,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    print(f"baseline ms/frame: {baseline:.4f}")


if __name__ == "__main__":
    _main()


def write_run_meta(
    results_dir: Path,
    arm: str,
    args: argparse.Namespace,
    space: dict[str, Any],
    t_start: float,
    **extra: Any,
) -> None:
    """Record per-run provenance and cost (wall time excludes lock waits)."""
    meta = {
        "arm": arm,
        "workload": args.workload,
        "seed": args.seed,
        "iterations": args.iterations,
        "frames": args.frames,
        "warmup": args.warmup,
        "knob_space_version": space.get("version"),
        "knob_names": [k["name"] for k in space["knobs"]],
        "wall_seconds_total": time.monotonic() - t_start,
        "lock_wait_seconds": LOCK_WAIT_S[0],
        "wall_seconds_excl_lock": time.monotonic() - t_start - LOCK_WAIT_S[0],
        **extra,
    }
    (results_dir / f"{arm}_meta.json").write_text(json.dumps(meta, indent=2))
