"""Aggregate per-arm trial logs into summary.csv.

Usage:
    python aggregate.py --results-dir results/run_20260513_120000
    python aggregate.py --results-dir results/run_20260513_120000 --baseline-file baseline.json
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

ARMS = ["random", "bayesian", "grid", "agent"]
JSONL_NAMES = {
    "random": "random_trials.jsonl",
    "bayesian": "bayesian_trials.jsonl",
    "grid": "grid_trials.jsonl",
    "agent": "agent_trials.jsonl",
}


def _load_trials(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    trials = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                trials.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return trials


def aggregate(results_dir: Path) -> list[dict[str, Any]]:
    baseline_file = results_dir / "baseline.json"
    baseline_ms: float | None = None
    if baseline_file.exists():
        try:
            data = json.loads(baseline_file.read_text())
            baseline_ms = data.get("baseline_ms_per_stream")
        except Exception:
            pass

    rows = []
    for arm in ARMS:
        trials = _load_trials(results_dir / JSONL_NAMES[arm])
        passing = [t for t in trials if t.get("verifier_ok") and t.get("ms_per_stream") is not None]
        n_total = len(trials)
        n_passing = len(passing)
        n_rejected = n_total - n_passing

        best_ms: float | None = min((t["ms_per_stream"] for t in passing), default=None)
        mean_ms: float | None = (
            sum(t["ms_per_stream"] for t in passing) / n_passing if n_passing > 0 else None
        )
        wall_total = sum(t.get("wall_seconds", 0.0) for t in trials)

        improvement_pct: float | None = None
        if best_ms is not None and baseline_ms is not None and baseline_ms > 0.0:
            improvement_pct = (baseline_ms - best_ms) / baseline_ms * 100.0

        rows.append(
            {
                "arm": arm,
                "n_total": n_total,
                "n_passing": n_passing,
                "n_rejected": n_rejected,
                "best_ms": f"{best_ms:.4f}" if best_ms is not None else "",
                "mean_ms_passing": f"{mean_ms:.4f}" if mean_ms is not None else "",
                "improvement_pct": f"{improvement_pct:.1f}" if improvement_pct is not None else "",
                "wall_seconds_total": f"{wall_total:.1f}",
                "baseline_ms": f"{baseline_ms:.4f}" if baseline_ms is not None else "",
            }
        )
    return rows


def write_csv(rows: list[dict[str, Any]], out_path: Path) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="Aggregate agent-tuning trial logs to summary.csv")
    p.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="directory containing *_trials.jsonl files",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output CSV path (default: results-dir/summary.csv)",
    )
    args = p.parse_args()

    out = args.out or (args.results_dir / "summary.csv")
    rows = aggregate(args.results_dir)
    write_csv(rows, out)
    print(f"Written: {out}")
    for r in rows:
        print(
            f"  {r['arm']:10s}  best={r['best_ms']:>10s} ms  "
            f"improvement={r['improvement_pct']:>6s}%  "
            f"passing={r['n_passing']}/{r['n_total']}"
        )


if __name__ == "__main__":
    main()
