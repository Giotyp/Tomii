#!/usr/bin/env python3
"""
tomii-vs-taskflow.py
====================
Three-panel Tomii vs Taskflow comparison for the largest configuration in
each benchmark:

  Panel 1: Anti-diagonal wavefront, N=512
  Panel 2: Block-DAG wavefront, N=512, tile=32
  Panel 3: Bilateral denoising, 8192×8192, tile=256

Output: paper/figs/tomii-vs-taskflow.png
"""

import os
import csv
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..", "benchmarks", "results", "csvs")
)
BENCH_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "bilateral-bench"))

WF_WORKERS = [1, 2, 4, 8, 16, 32]
BIL_WORKERS = [2, 4, 8, 16]


# ---------------------------------------------------------------------------
# Wavefront loaders (single N, returns list aligned to workers)
# ---------------------------------------------------------------------------


def load_wf_vals(filename_fn, system_filter, workers, n):
    vals = []
    for w in workers:
        path = os.path.join(CSV_DIR, filename_fn(n, w))
        best = None
        if os.path.exists(path):
            with open(path) as f:
                for row in csv.DictReader(f):
                    if system_filter and row.get("system") != system_filter:
                        continue
                    try:
                        val_ms = float(row["s_per_iter"]) * 1000.0
                    except (KeyError, ValueError):
                        continue
                    if best is None or val_ms < best:
                        best = val_ms
        vals.append(best)
    return vals


def load_antidiag_tomii(n=512):
    return load_wf_vals(
        lambda n, w: f"synstream_wavefront_n{n}_w{w}_st1_t1_result.csv",
        "synstream_st1_t1",
        WF_WORKERS,
        n,
    )


def load_antidiag_taskflow(n=512):
    return load_wf_vals(
        lambda n, w: f"taskflow_wavefront_n{n}_w{w}.csv",
        "taskflow_pinned",
        WF_WORKERS,
        n,
    )


def load_blockdag_tomii(n=512):
    return load_wf_vals(
        lambda n, w: f"synstream_wavefront_block_n{n}_w{w}_t32_result.csv",
        "synstream_block_t32",
        WF_WORKERS,
        n,
    )


def load_blockdag_taskflow(n=512):
    return load_wf_vals(
        lambda n, w: f"taskflow_block_wavefront_n{n}_w{w}.csv",
        "taskflow_block_pinned",
        WF_WORKERS,
        n,
    )


# ---------------------------------------------------------------------------
# Bilateral loaders
# ---------------------------------------------------------------------------


def _read_csvs(paths):
    rows = []
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                rows.extend(csv.DictReader(f))
    return rows


def load_bilateral_tomii(workers, img=8192, tile=256):
    rows = _read_csvs(
        [os.path.join(BENCH_DIR, "synstream", "results", "ss_bilateral_all.csv")]
    )
    data = {}
    for r in rows:
        sys = r.get("system", "")
        if not sys.startswith("synstream_w") or sys.count("_") != 2:
            continue
        try:
            key = (int(r["image_size"]), int(r["tile_size"]), int(r["workers"]))
            val = float(r["time_ms"])
        except (KeyError, ValueError):
            continue
        if key not in data or val < data[key]:
            data[key] = val
    return [data.get((img, tile, w)) for w in workers]


def load_bilateral_taskflow(workers, img=8192, tile=256):
    rows = _read_csvs(
        [
            os.path.join(BENCH_DIR, "taskflow", "results", "tf_bilateral_all.csv"),
            os.path.join(
                BENCH_DIR, "taskflow", "results", "tf_bilateral_8192_tile256.csv"
            ),
        ]
    )
    data = {}
    for r in rows:
        if not r.get("system", "").startswith("taskflow"):
            continue
        try:
            key = (int(r["image_size"]), int(r["tile_size"]), int(r["threads"]))
            val = float(r["time_ms"])
        except (KeyError, ValueError):
            continue
        if key not in data or val < data[key]:
            data[key] = val
    return [data.get((img, tile, w)) for w in workers]


# ---------------------------------------------------------------------------
# Plot helper
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


def plot_line(ax, workers, vals, label, color, ls, marker, add_legend):
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
        label=label if add_legend else "_nolegend_",
    )


# ---------------------------------------------------------------------------
# Main
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
    ("Tomii", "#000000", "-", "o"),
    ("Taskflow", "#555555", "-", "s"),
]

FIG_SIZE = (3.2, 2.8)


def add_legend(fig):
    handles, labels = fig.axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=2,
        bbox_to_anchor=(0.5, -0.12),
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.4,
        columnspacing=1.2,
    )


def save(fig, name):
    out = os.path.join(SCRIPT_DIR, name)
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Figure functions
# ---------------------------------------------------------------------------


def figure_antidiag():
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    vals_list = [load_antidiag_tomii(), load_antidiag_taskflow()]
    for (label, color, ls, marker), vals in zip(SERIES, vals_list):
        plot_line(ax, WF_WORKERS, vals, label, color, ls, marker, add_legend=True)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(WF_WORKERS)
    ax.set_xticklabels([str(w) for w in WF_WORKERS], fontsize=7.5)
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5], numticks=10))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_ms))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_title(r"Anti-diagonal, $N=512$", fontsize=9, pad=4)
    ax.set_xlabel("Workers", fontsize=8)
    ax.set_ylabel("ms / sweep", fontsize=8)
    ax.grid(True, which="major")
    ax.grid(True, which="minor", linewidth=0.2, alpha=0.2)
    add_legend(fig)
    save(fig, "tomii-vs-taskflow-antidiag.png")


def figure_blockdag():
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    vals_list = [load_blockdag_tomii(), load_blockdag_taskflow()]
    for (label, color, ls, marker), vals in zip(SERIES, vals_list):
        plot_line(ax, WF_WORKERS, vals, label, color, ls, marker, add_legend=True)
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(WF_WORKERS)
    ax.set_xticklabels([str(w) for w in WF_WORKERS], fontsize=7.5)
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5], numticks=10))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_ms))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_title(r"Block-DAG, $N=512$, tile=32", fontsize=9, pad=4)
    ax.set_xlabel("Workers", fontsize=8)
    ax.set_ylabel("ms / sweep", fontsize=8)
    ax.grid(True, which="major")
    ax.grid(True, which="minor", linewidth=0.2, alpha=0.2)
    add_legend(fig)
    save(fig, "tomii-vs-taskflow-blockdag.png")


def figure_bilateral():
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    vals_list = [load_bilateral_tomii(BIL_WORKERS), load_bilateral_taskflow(BIL_WORKERS)]
    for (label, color, ls, marker), vals in zip(SERIES, vals_list):
        plot_line(ax, BIL_WORKERS, vals, label, color, ls, marker, add_legend=True)
    ax.set_xscale("log", base=2)
    ax.set_xticks(BIL_WORKERS)
    ax.set_xticklabels([str(w) for w in BIL_WORKERS], fontsize=7.5)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    ax.set_title(r"Bilateral, $8192{\times}8192$, tile=256", fontsize=9, pad=4)
    ax.set_xlabel("Workers", fontsize=8)
    ax.set_ylabel("ms / filter pass", fontsize=8)
    ax.grid(True, which="major")
    add_legend(fig)
    save(fig, "tomii-vs-taskflow-bilateral.png")


def main():
    figure_antidiag()
    figure_blockdag()
    figure_bilateral()


if __name__ == "__main__":
    main()
