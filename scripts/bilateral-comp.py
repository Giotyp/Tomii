#!/usr/bin/env python3
"""
bilateral-comp.py
=================
Three-panel bilateral denoising comparison, extending fig-bilateral-comparison.tex
to include TBB flow_graph as a third baseline.

Panels (matching the paper figure):
  1. 4096×4096, tile=256 (16×16 grid), W = 1,2,4,8,16
  2. 4096×4096, tile=128 (32×32 grid), W = 1,2,4,8,16
  3. 8192×8192, tile=256 (32×32 grid), W = 2,4,8,16

Data sources (all read live from bilateral-bench/):
  Taskflow : taskflow/results/tf_bilateral_all.csv
             taskflow/results/tf_bilateral_8192_tile256.csv
  Tomii    : synstream/results/ss_bilateral_all.csv
             system == 'synstream_w{W}_st1'
  TBB flow : tbb/results/tbb_flow_bilateral_all.csv
             system == 'tbb_flow'

Output: paper/figs/bilateral-comp.png
"""

import argparse
import os
import csv
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BENCH_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "bilateral-bench"))

TF_CSVS = [
    os.path.join(BENCH_DIR, "taskflow", "results", "tf_bilateral_all.csv"),
    os.path.join(BENCH_DIR, "taskflow", "results", "tf_bilateral_8192_tile256.csv"),
]
SS_CSV = os.path.join(BENCH_DIR, "synstream", "results", "ss_bilateral_all.csv")
TBB_CSV = os.path.join(BENCH_DIR, "tbb", "results", "tbb_flow_bilateral_all.csv")

# ---------------------------------------------------------------------------
# Panel definitions
# ---------------------------------------------------------------------------
PANELS = [
    {
        "image_size": 4096,
        "tile_size": 128,
        "workers": [1, 2, 4, 8, 16],
        "title": r"$4096{\times}4096$, tile=128",
    },
    {
        "image_size": 4096,
        "tile_size": 256,
        "workers": [1, 2, 4, 8, 16],
        "title": r"$4096{\times}4096$, tile=256",
    },
    {
        "image_size": 8192,
        "tile_size": 256,
        "workers": [2, 4, 8, 16],
        "title": r"$8192{\times}8192$, tile=256",
    },
]

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _read_csvs(paths):
    rows = []
    for p in paths:
        if os.path.exists(p):
            with open(p) as f:
                rows.extend(csv.DictReader(f))
    return rows


def load_taskflow():
    """Returns {(image_size, tile_size, workers): time_ms}"""
    rows = _read_csvs(TF_CSVS)
    data = {}
    for r in rows:
        if not r.get("system", "").startswith("taskflow"):
            continue
        key = (int(r["image_size"]), int(r["tile_size"]), int(r["threads"]))
        val = float(r["time_ms"])
        if key not in data or val < data[key]:
            data[key] = val
    return data


def load_tomii():
    """Returns {(image_size, tile_size, workers): time_ms}
    Uses system == 'synstream_w{W}_st1' (base variant, no ic/dep suffixes)."""
    rows = _read_csvs([SS_CSV])
    data = {}
    for r in rows:
        sys = r.get("system", "")
        # Keep only plain synstream_w{N}_st1, exclude ic/dep variants
        if not sys.startswith("synstream_w") or sys.count("_") != 2:
            continue
        try:
            key = (int(r["image_size"]), int(r["tile_size"]), int(r["workers"]))
        except (KeyError, ValueError):
            continue
        val = float(r["time_ms"])
        if key not in data or val < data[key]:
            data[key] = val
    return data


def load_tbb_flow():
    """Returns {(image_size, tile_size, workers): time_ms}"""
    rows = _read_csvs([TBB_CSV])
    data = {}
    for r in rows:
        if r.get("system") != "tbb_flow":
            continue
        key = (int(r["image_size"]), int(r["tile_size"]), int(r["threads"]))
        val = float(r["time_ms"])
        if key not in data or val < data[key]:
            data[key] = val
    return data


def extract_panel(data_dict, panel):
    img, tile, workers = panel["image_size"], panel["tile_size"], panel["workers"]
    xs, ys = [], []
    for w in workers:
        v = data_dict.get((img, tile, w))
        if v is not None:
            xs.append(w)
            ys.append(v)
    return xs, ys


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------


def make_figure(tf_data, ss_data, tbb_data, include_tbb=False):
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 9,
            "axes.titlesize": 9,
            "axes.labelsize": 8,
            "legend.fontsize": 10,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "axes.linewidth": 0.7,
            "grid.linewidth": 0.4,
            "grid.alpha": 0.35,
        }
    )

    fig, axes = plt.subplots(1, 3, figsize=(10.0, 3.4), gridspec_kw={"wspace": 0.28})

    SERIES = [
        ("Tomii", ss_data, "#000000", "-", "o", 1.4, 4),
        ("Taskflow (C++)", tf_data, "#555555", "-", "s", 1.4, 3),
    ]
    if include_tbb:
        SERIES.append(("TBB flow_graph†", tbb_data, "#999999", "--", "D", 1.4, 2))

    for col, (ax, panel) in enumerate(zip(axes, PANELS)):
        workers = panel["workers"]
        for label, data_dict, color, ls, marker, lw, zo in SERIES:
            xs, ys = extract_panel(data_dict, panel)
            if not xs:
                continue
            kw = dict(
                color=color,
                linestyle=ls,
                marker=marker,
                linewidth=lw,
                markersize=4,
                zorder=zo,
                label=label if col == 0 else "_nolegend_",
            )
            ax.plot(xs, ys, **kw)

        ax.set_xscale("log", base=2)
        ax.set_xticks(workers)
        ax.set_xticklabels([str(w) for w in workers], fontsize=7.5)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.set_title(panel["title"], fontsize=9, pad=4)
        ax.set_xlabel("Workers", fontsize=8)
        ax.grid(True, which="major")
        if col == 0:
            ax.set_ylabel("ms / filter pass", fontsize=8)

    handles, labels = axes[0].get_legend_handles_labels()
    ncol = 3 if include_tbb else 2
    legend_y = -0.18 if include_tbb else -0.14
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=ncol,
        bbox_to_anchor=(0.5, legend_y),
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.4,
        columnspacing=1.2,
    )

    if include_tbb:
        fig.text(
            0.5,
            -0.30,
            "† TBB flow_graph: worker count via global_control (limits TBB global pool); "
            "no core pinning.\n"
            "  Results are informative but not directly comparable to pinned measurements.",
            ha="center",
            va="top",
            fontsize=6.5,
            color="#555555",
            style="italic",
        )

    return fig


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-tbb", action="store_true", help="Add TBB baseline series")
    args = parser.parse_args()

    tf_data = load_taskflow()
    ss_data = load_tomii()
    tbb_data = load_tbb_flow() if args.include_tbb else {}

    sources = [("Taskflow", tf_data), ("Tomii", ss_data)]
    if args.include_tbb:
        sources.append(("TBB", tbb_data))
    for name, data in sources:
        for panel in PANELS:
            for w in panel["workers"]:
                key = (panel["image_size"], panel["tile_size"], w)
                if key not in data:
                    print(f"  Warning: missing {name} {key}")

    fig = make_figure(tf_data, ss_data, tbb_data, include_tbb=args.include_tbb)
    out = os.path.join(SCRIPT_DIR, "bilateral-comp.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


if __name__ == "__main__":
    main()
