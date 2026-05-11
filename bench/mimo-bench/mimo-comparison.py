#!/usr/bin/env python3
"""
mimo-comparison.py
==================
Tomii vs Taskflow comparison for the 4-node MIMO benchmark.

  fft → csi → beam → demul (decode dropped)
  4×4 antenna config, 256-pt FFT, 192 data SCs
  S concurrent slots in {1, 4, 16, 64}
  Fixed worker count W (default: 4)

Reads from:
  mimo-bench/tomii/results/mimo_sweep.csv
  mimo-bench/taskflow/build/tf_mimo_sweep.csv

Output:
  mimo-bench/mimo-comparison.png

Usage (from bench worktree root):
    python mimo-bench/mimo-comparison.py
    python mimo-bench/mimo-comparison.py --workers 8
"""

from __future__ import annotations

import argparse
import csv
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

SCRIPT_DIR         = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_TOMII_CSV = os.path.join(SCRIPT_DIR, "tomii", "results", "mimo_sweep.csv")
_DEFAULT_TF_CSV    = os.path.join(SCRIPT_DIR, "taskflow", "build", "tf_mimo_sweep.csv")

TOMII_CSV    = _DEFAULT_TOMII_CSV
TASKFLOW_CSV = _DEFAULT_TF_CSV

SLOTS = [1, 4, 16, 64]


def _load_ms(path: str, system_filter: str, workers: int,
             slots: list[int]) -> list[float | None]:
    data: dict[int, float] = {}
    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return [None] * len(slots)
    with open(path) as f:
        for row in csv.DictReader(f):
            try:
                if system_filter and row.get("system") != system_filter:
                    continue
                w = int(row["workers"])
                if w != workers:
                    continue
                s = int(row["slots"])
                ms = float(row["ms_per_slot"])
                if ms > 0.0:
                    if s not in data or ms < data[s]:
                        data[s] = ms
            except (KeyError, ValueError):
                continue
    return [data.get(s) for s in slots]


RC = {
    "font.family":    "serif",
    "font.size":       9,
    "axes.titlesize":  9,
    "axes.labelsize":  8,
    "legend.fontsize": 7.5,
    "xtick.labelsize": 7.5,
    "ytick.labelsize": 7.5,
    "axes.linewidth":  0.7,
    "grid.linewidth":  0.4,
    "grid.alpha":      0.35,
}


def plot_line(ax, slots, vals, label, color, ls, marker):
    pairs = [(s, v) for s, v in zip(slots, vals) if v is not None]
    if not pairs:
        print(f"  Warning: no data for {label}")
        return
    sx, sy = zip(*pairs)
    ax.plot(sx, sy, color=color, linestyle=ls, marker=marker,
            linewidth=1.4, markersize=4, label=label)


def figure_mimo(workers: int) -> None:
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=(3.6, 3.0))

    series = [
        ("Tomii",
         _load_ms(TOMII_CSV, "tomii", workers, SLOTS),
         "#1f77b4", "-", "o"),
        ("Taskflow",
         _load_ms(TASKFLOW_CSV, "taskflow", workers, SLOTS),
         "#d62728", "-", "s"),
    ]

    for label, vals, color, ls, marker in series:
        plot_line(ax, SLOTS, vals, label, color, ls, marker)

    ax.set_xscale("log", base=2)
    ax.set_xticks(SLOTS)
    ax.set_xticklabels([str(s) for s in SLOTS], fontsize=7.5)
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())

    ax.set_title(f"MIMO pipeline latency, 4×4, W={workers}", fontsize=9, pad=4)
    ax.set_xlabel("Concurrent slots (S)", fontsize=8)
    ax.set_ylabel("ms / slot, first-pkt→done (lower is better)", fontsize=8)
    ax.grid(True, which="major")
    ax.legend(loc="upper right", frameon=True, framealpha=1.0,
              edgecolor="#cccccc", fontsize=7.5, handlelength=2.4)

    out = os.path.join(SCRIPT_DIR, "mimo-comparison.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


def main() -> None:
    global TOMII_CSV, TASKFLOW_CSV
    p = argparse.ArgumentParser(
        description="Plot Tomii vs Taskflow MIMO comparison.")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--slots", type=int, nargs="+", default=SLOTS)
    p.add_argument("--tomii-csv", default=None)
    p.add_argument("--taskflow-csv", default=None)
    args = p.parse_args()

    if args.tomii_csv:
        TOMII_CSV = args.tomii_csv
    if args.taskflow_csv:
        TASKFLOW_CSV = args.taskflow_csv

    figure_mimo(args.workers)


if __name__ == "__main__":
    main()
