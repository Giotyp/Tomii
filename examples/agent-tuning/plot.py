"""Generate a convergence plot comparing all four search arms.

Usage:
    python plot.py --results-dir results/run_20260513_120000
    python plot.py --results-dir results/run_20260513_120000 --out comparison.png
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use("Agg")

ARMS = ["random", "bayesian", "grid", "agent"]
COLORS = {
    "random": "#1f77b4",
    "bayesian": "#ff7f0e",
    "grid": "#2ca02c",
    "agent": "#d62728",
}
LABELS = {
    "random": "Random",
    "bayesian": "Bayesian (Optuna TPE)",
    "grid": "Grid",
    "agent": "Agent (Claude)",
}
JSONL_NAMES = {
    "random": "random_trials.jsonl",
    "bayesian": "bayesian_trials.jsonl",
    "grid": "grid_trials.jsonl",
    "agent": "agent_trials.jsonl",
}


def _load_best_so_far(path: Path) -> list[float]:
    """Return list of best-so-far ms_per_stream at each iteration index."""
    if not path.exists():
        return []
    best: float | None = None
    series: list[float] = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        ms = rec.get("ms_per_stream") if rec.get("verifier_ok") else None
        if ms is not None and (best is None or ms < best):
            best = ms
        if best is not None:
            series.append(best)
    return series


def plot(results_dir: Path, out_path: Path) -> None:
    baseline_ms: float | None = None
    baseline_file = results_dir / "baseline.json"
    if baseline_file.exists():
        try:
            data = json.loads(baseline_file.read_text())
            baseline_ms = data.get("baseline_ms_per_stream")
        except Exception:
            pass

    fig, ax = plt.subplots(figsize=(9, 5))

    for arm in ARMS:
        series = _load_best_so_far(results_dir / JSONL_NAMES[arm])
        if not series:
            continue
        xs = list(range(1, len(series) + 1))
        ax.plot(xs, series, color=COLORS[arm], label=LABELS[arm], linewidth=2)

    if baseline_ms is not None:
        ax.axhline(
            baseline_ms,
            color="gray",
            linestyle="--",
            linewidth=1.5,
            label=f"Baseline ({baseline_ms:.2f} ms)",
        )

    ax.set_xlabel("Iteration (passing trials only)")
    ax.set_ylabel("Best-so-far ms/stream")
    ax.set_title("Agent-tuning convergence — stream-analytics workload")
    ax.legend(loc="upper right")
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(str(out_path), dpi=150)
    plt.close(fig)
    print(f"Written: {out_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Plot agent-tuning convergence curves")
    p.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="directory containing *_trials.jsonl",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output PNG path (default: results-dir/comparison.png)",
    )
    args = p.parse_args()

    out = args.out or (args.results_dir / "comparison.png")
    plot(args.results_dir, out)


if __name__ == "__main__":
    main()
