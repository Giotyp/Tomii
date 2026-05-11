#!/usr/bin/env python3
"""
tomii-vs-taskflow.py
====================
Tomii vs Taskflow comparison for the anti-diagonal wavefront benchmark.

  Anti-diagonal wavefront, N=512

Reads from:
  tomii/results/tomii_wavefront_n512_wf_cell_sweep.csv       (per-cell, Tiers 1-3)
  tomii/results/tomii_wavefront_n512_wf_cell_bulk_sweep.csv  (Tier 4 bulk)
  taskflow/build/tf_wavefront_sweep.csv

Output: anti-diag-bench/tomii-vs-taskflow-antidiag.png
"""

import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TOMII_RESULTS = os.path.join(SCRIPT_DIR, "tomii", "results")
TOMII_CELL_CSV  = os.path.join(TOMII_RESULTS, "tomii_wavefront_n512_wf_cell_sweep.csv")
TOMII_BULK_CSV  = os.path.join(TOMII_RESULTS, "tomii_wavefront_n512_wf_cell_bulk_sweep.csv")
TASKFLOW_CSV    = os.path.join(SCRIPT_DIR, "taskflow", "build", "tf_wavefront_sweep.csv")

WORKERS = [1, 2, 4, 8, 16, 32]
N = 512


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _load_csv_min(path, workers=WORKERS, n=N, system_filter=None):
    """Load best (min) ms/iter per worker count from a CSV file."""
    data = {}
    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return [None] * len(workers)
    with open(path) as f:
        for row in csv.DictReader(f):
            try:
                if int(row["n"]) != n:
                    continue
                if system_filter and row.get("system") != system_filter:
                    continue
                w = int(row["workers"])
                val = float(row["ms_per_iter"])
            except (KeyError, ValueError):
                continue
            if w not in data or val < data[w]:
                data[w] = val
    return [data.get(w) for w in workers]


def load_tomii_cell(workers=WORKERS, n=N):
    return _load_csv_min(TOMII_CELL_CSV, workers, n)


def load_tomii_bulk(workers=WORKERS, n=N):
    return _load_csv_min(TOMII_BULK_CSV, workers, n)


def load_taskflow_best(workers=WORKERS, n=N):
    return _load_csv_min(TASKFLOW_CSV, workers, n)


def load_taskflow_static(workers=WORKERS, n=N):
    # Accept both unpinned ("taskflow_static") and pinned ("taskflow_pinned_static") runs.
    # _load_csv_min takes min per worker count, so whichever label is present wins.
    pinned   = _load_csv_min(TASKFLOW_CSV, workers, n, system_filter="taskflow_pinned_static")
    unpinned = _load_csv_min(TASKFLOW_CSV, workers, n, system_filter="taskflow_static")
    return [
        min((v for v in (p, u) if v is not None), default=None)
        for p, u in zip(pinned, unpinned)
    ]


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------


def fmt_ms(x, _):
    if x >= 10:
        return f"{x:.0f}"
    elif x >= 1:
        return f"{x:.1f}"
    elif x >= 0.1:
        return f"{x:.2f}"
    else:
        return f"{x:.3f}"


def plot_line(ax, workers, vals, label, color, ls, marker):
    pairs = [(w, v) for w, v in zip(workers, vals) if v is not None]
    if not pairs:
        print(f"  Warning: no data for {label}")
        return
    wx, wy = zip(*pairs)
    ax.plot(wx, wy, color=color, linestyle=ls, marker=marker,
            linewidth=1.4, markersize=4, label=label)


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------

RC = {
    "font.family": "serif",
    "font.size": 9,
    "axes.titlesize": 9,
    "axes.labelsize": 8,
    "legend.fontsize": 7.5,
    "xtick.labelsize": 7.5,
    "ytick.labelsize": 7.5,
    "axes.linewidth": 0.7,
    "grid.linewidth": 0.4,
    "grid.alpha": 0.35,
}

SERIES = [
    ("Tomii (per-cell)",   load_tomii_cell,      "#1f77b4", "--", "o"),
    ("Tomii (bulk)",       load_tomii_bulk,       "#1f77b4", "-",  "o"),
    ("Taskflow (static)",  load_taskflow_static,  "#555555", "-",  "s"),
    ("Taskflow (best)",    load_taskflow_best,    "#555555", "--", "^"),
]


def figure_antidiag():
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=(3.2, 2.8))

    for label, loader, color, ls, marker in SERIES:
        vals = loader()
        plot_line(ax, WORKERS, vals, label, color, ls, marker)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(WORKERS)
    ax.set_xticklabels([str(w) for w in WORKERS], fontsize=7.5)
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5], numticks=10))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_ms))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_title(r"Anti-diagonal wavefront, $N=512$", fontsize=9, pad=4)
    ax.set_xlabel("Workers", fontsize=8)
    ax.set_ylabel("ms / sweep", fontsize=8)
    ax.grid(True, which="major")
    ax.grid(True, which="minor", linewidth=0.2, alpha=0.2)
    ax.legend(loc="upper left", frameon=True, framealpha=1.0,
               edgecolor="#cccccc", fontsize=7.5, handlelength=2.4)

    out = os.path.join(SCRIPT_DIR, "tomii-vs-taskflow-antidiag.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


if __name__ == "__main__":
    figure_antidiag()
