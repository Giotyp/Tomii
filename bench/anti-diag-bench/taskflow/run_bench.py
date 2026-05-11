#!/usr/bin/env python3
"""Run tf_wavefront across all partitioners and worker counts, then plot results."""

import subprocess
import argparse
import csv
import sys
from pathlib import Path
from collections import defaultdict

BINARY = Path(__file__).parent / "build" / "tf_wavefront"
CSV_OUT = Path(__file__).parent / "build" / "tf_wavefront_sweep.csv"

N = 512
ITERATIONS = 100
WARMUP = 10
WORKERS = [1, 2, 4, 8, 16, 32]
PARTITIONERS = ["static", "dynamic", "guided", "random"]


def run_benchmarks():
    CSV_OUT.unlink(missing_ok=True)

    total = len(PARTITIONERS) * len(WORKERS)
    done = 0
    for partitioner in PARTITIONERS:
        for workers in WORKERS:
            done += 1
            print(
                f"[{done}/{total}] partitioner={partitioner} workers={workers}",
                flush=True,
            )
            cmd = [
                str(BINARY),
                "--n",
                str(N),
                "--iterations",
                str(ITERATIONS),
                "--warmup",
                str(WARMUP),
                "--workers",
                str(workers),
                "--partitioner",
                partitioner,
                "--pin",
                "--output",
                str(CSV_OUT),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  ERROR: {result.stderr.strip()}", file=sys.stderr)
                sys.exit(1)
            # Print last line (summary)
            lines = result.stdout.strip().splitlines()
            if lines:
                print(f"  {lines[-1]}", flush=True)


def load_results():
    # Returns {partitioner: {workers: ms_per_iter}}
    data = defaultdict(dict)
    with open(CSV_OUT) as f:
        reader = csv.DictReader(f)
        for row in reader:
            system = row["system"]  # e.g. taskflow_static
            workers = int(row["workers"])
            ms_per_iter = float(row["ms_per_iter"])
            # Extract partitioner from system label
            prefix = "taskflow_"
            partitioner = system[len(prefix) :] if system.startswith(prefix) else system
            # If multiple rows for same key, take last (most recent run)
            data[partitioner][workers] = ms_per_iter
    return data


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

# Four distinguishable series in a black/gray palette
SERIES = [
    ("static", "#000000", "-", "o"),
    ("dynamic", "#444444", "--", "s"),
    ("guided", "#888888", "-.", "^"),
    ("random", "#bbbbbb", ":", "D"),
]


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
    ax.plot(
        wx,
        wy,
        color=color,
        linestyle=ls,
        marker=marker,
        linewidth=1.4,
        markersize=4,
        label=label,
    )


def plot(data):
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("matplotlib not available — skipping plot")
        return

    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=(3.2, 2.8))

    for partitioner, color, ls, marker in SERIES:
        vals = [data[partitioner].get(w) for w in WORKERS]
        plot_line(ax, WORKERS, vals, partitioner, color, ls, marker)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(WORKERS)
    ax.set_xticklabels([str(w) for w in WORKERS], fontsize=7.5)
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5], numticks=10))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_ms))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_title(f"Taskflow anti-diagonal, $N={N}$", fontsize=9, pad=4)
    ax.set_xlabel("Workers", fontsize=8)
    ax.set_ylabel("ms / sweep", fontsize=8)
    ax.grid(True, which="major")
    ax.grid(True, which="minor", linewidth=0.2, alpha=0.2)

    handles, labels = ax.get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=2,
        bbox_to_anchor=(0.5, -0.18),
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.4,
        columnspacing=1.2,
    )

    out_png = Path(__file__).parent / "build" / "tf_wavefront_sweep.png"
    fig.savefig(out_png, dpi=200, bbox_inches="tight")
    print(f"Plot saved to {out_png}")
    plt.close(fig)


def arg_parser():
    parser = argparse.ArgumentParser(description="Run tf_wavefront benchmarks")
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Skip benchmarks and only plot existing results",
    )
    return parser


if __name__ == "__main__":
    if not BINARY.exists():
        print(
            f"Binary not found: {BINARY}\nBuild first with cmake --build build/",
            file=sys.stderr,
        )
        sys.exit(1)

    args = arg_parser().parse_args()
    if not args.plot_only:
        run_benchmarks()

    data = load_results()

    print("\n--- Results (ms/sweep) ---")
    header = f"{'partitioner':<12}" + "".join(f"{w:>8}" for w in WORKERS)
    print(header)
    for partitioner in PARTITIONERS:
        row = f"{partitioner:<12}" + "".join(
            f"{data[partitioner].get(w, float('nan')):>8.2f}" for w in WORKERS
        )
        print(row)

    plot(data)
