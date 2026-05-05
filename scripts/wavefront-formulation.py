#!/usr/bin/env python3
"""
wavefront-formulation.py
========================
Two side-by-side 2x2 figures from fresh benchmark CSVs:

  Figure 1 (wavefront-antidiag.png)
    Anti-diagonal / barrier formulation:
      Tomii barrier (tile=32), Taskflow anti-diag (pinned),
      TBB parallel_for (pinned)

  Figure 2 (wavefront-blockdag.png)
    Block-DAG formulation (tile=32):
      Tomii block-DAG, Taskflow block-DAG (pinned),
      TBB flow_graph† (global_control, no core pinning)

All data read live from benchmarks/results/csvs/.
When a CSV has multiple rows (repeated runs), the minimum s_per_iter is used.
For tbb_flow files the 'tbb_flow_pinned' rows (old arena-bypass run) are excluded.
"""

import argparse
import os
import csv
import math
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

WORKERS = [1, 2, 4, 8, 16, 32]
NS = [64, 128, 256, 512]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIR = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..", "benchmarks", "results", "csvs")
)


# ---------------------------------------------------------------------------
# Generic CSV loader — returns min s_per_iter in ms for each (N, W)
# ---------------------------------------------------------------------------


def load_series(filename_fn, system_filter=None):
    """
    filename_fn(n, w) -> relative path under CSV_DIR
    system_filter: if given, only rows with row['system'] == system_filter are used.
    Returns dict {n: [ms_or_None, ...]} for each N, indexed by WORKERS.
    """
    data = {}
    for n in NS:
        row_vals = []
        for w in WORKERS:
            path = os.path.join(CSV_DIR, filename_fn(n, w))
            best = None
            if os.path.exists(path):
                with open(path) as f:
                    for row in csv.DictReader(f):
                        if system_filter and row.get("system") != system_filter:
                            continue
                        try:
                            val_s = float(row["s_per_iter"])
                        except (KeyError, ValueError):
                            continue
                        val_ms = val_s * 1000.0
                        if best is None or val_ms < best:
                            best = val_ms
            row_vals.append(best)
        data[n] = row_vals
    return data


def load_tomii_antidiag():
    return load_series(
        lambda n, w: f"synstream_wavefront_n{n}_w{w}_st1_t1_result.csv",
        system_filter="synstream_st1_t1",
    )


def load_tomii_block():
    return load_series(
        lambda n, w: f"synstream_wavefront_block_n{n}_w{w}_t32_result.csv",
        system_filter="synstream_block_t32",
    )


def load_taskflow_antidiag():
    return load_series(
        lambda n, w: f"taskflow_wavefront_n{n}_w{w}.csv",
        system_filter="taskflow_pinned",
    )


def load_taskflow_block():
    return load_series(
        lambda n, w: f"taskflow_block_wavefront_n{n}_w{w}.csv",
        system_filter="taskflow_block_pinned",
    )


def load_tbb_pfor():
    return load_series(
        lambda n, w: f"tbb_wavefront_n{n}_w{w}.csv", system_filter="tbb_pinned"
    )


def load_tbb_flow():
    return load_series(
        lambda n, w: f"tbb_flow_wavefront_n{n}_w{w}.csv", system_filter="tbb_flow"
    )


# ---------------------------------------------------------------------------
# Plotting helpers
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


def apply_axes_style(ax, n, panel_idx):
    ax.set_yscale("log")
    ax.set_xscale("log", base=2)
    ax.set_xticks(WORKERS)
    ax.set_xticklabels([str(w) for w in WORKERS], fontsize=7.5)
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5], numticks=10))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_ms))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax.set_title(f"$N={n}$", fontsize=9, pad=4)
    ax.grid(True, which="major")
    ax.grid(True, which="minor", linewidth=0.2, alpha=0.2)
    if panel_idx in (2, 3):
        ax.set_xlabel("Workers", fontsize=8)
    if panel_idx in (0, 2):
        ax.set_ylabel("ms / sweep", fontsize=8)


def plot_series(ax, series_list, panel_idx):
    """series_list: list of (label, data_dict, color, ls, marker, lw, zorder)"""
    for label, data_dict, color, ls, marker, lw, zo in series_list:
        yvals = data_dict.get(data_dict and list(data_dict.keys())[0] and 0)
        n_key = NS[panel_idx % len(NS)]  # handled by caller
        yvals = data_dict.get(n_key, [None] * len(WORKERS))
        pairs = [(w, y) for w, y in zip(WORKERS, yvals) if y is not None]
        if not pairs:
            continue
        wx, wy = zip(*pairs)
        kw = dict(
            color=color,
            linestyle=ls,
            marker=marker,
            linewidth=lw,
            markersize=4,
            zorder=zo,
            label=label if panel_idx == 0 else "_nolegend_",
        )
        ax.plot(wx, wy, **kw)


def make_figure(series_list, footnote=None):
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.size": 9,
            "axes.titlesize": 9,
            "axes.labelsize": 8,
            "legend.fontsize": 12,
            "xtick.labelsize": 7.5,
            "ytick.labelsize": 7.5,
            "axes.linewidth": 0.7,
            "grid.linewidth": 0.4,
            "grid.alpha": 0.35,
        }
    )

    fig, axes = plt.subplots(
        2, 2, figsize=(7.5, 5.4),
        sharey="row",
        gridspec_kw={"hspace": 0.38, "wspace": 0.18},
    )
    ax_list = [axes[0, 0], axes[0, 1], axes[1, 0], axes[1, 1]]

    for panel_idx, (ax, n) in enumerate(zip(ax_list, NS)):
        for label, data_dict, color, ls, marker, lw, zo in series_list:
            yvals = data_dict.get(n, [None] * len(WORKERS))
            pairs = [(w, y) for w, y in zip(WORKERS, yvals) if y is not None]
            if not pairs:
                continue
            wx, wy = zip(*pairs)
            kw = dict(
                color=color,
                linestyle=ls,
                marker=marker,
                linewidth=lw,
                markersize=4,
                zorder=zo,
                label=label if panel_idx == 0 else "_nolegend_",
            )
            ax.plot(wx, wy, **kw)
        apply_axes_style(ax, n, panel_idx)

    handles, labels = ax_list[0].get_legend_handles_labels()
    legend_y = -0.10 if footnote is None else -0.13
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=3,
        bbox_to_anchor=(0.5, legend_y),
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.4,
        columnspacing=1.2,
    )

    if footnote:
        fig.text(
            0.5,
            -0.21,
            footnote,
            ha="center",
            va="top",
            fontsize=6.5,
            color="#555555",
            style="italic",
        )

    return fig


# ---------------------------------------------------------------------------
# Figure 1 — Anti-diagonal / barrier
# ---------------------------------------------------------------------------


def figure_antidiag(out_path, include_tbb=False):
    tomii = load_tomii_antidiag()
    taskflow = load_taskflow_antidiag()

    sources = [("Tomii", tomii), ("Taskflow", taskflow)]
    series = [
        ("Tomii, anti-diag (barrier)", tomii, "#000000", "-", "o", 1.4, 4),
        ("Taskflow, anti-diag (pinned)", taskflow, "#555555", "-", "s", 1.4, 3),
    ]

    if include_tbb:
        tbb_pfor = load_tbb_pfor()
        sources.append(("TBB", tbb_pfor))
        series.append(
            ("TBB parallel_for, anti-diag (pinned)", tbb_pfor, "#999999", "-", "^", 1.4, 2)
        )

    missing = [
        (name, n, w)
        for name, data in sources
        for n in NS
        for w, v in zip(WORKERS, data[n])
        if v is None
    ]
    if missing:
        print(f"  [antidiag] Missing data: {missing}")

    fig = make_figure(series)
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Figure 2 — Block-DAG
# ---------------------------------------------------------------------------


def figure_blockdag(out_path, include_tbb=False):
    tomii = load_tomii_block()
    taskflow = load_taskflow_block()

    sources = [("Tomii", tomii), ("Taskflow", taskflow)]
    series = [
        ("Tomii, block-DAG", tomii, "#000000", "-", "o", 1.4, 4),
        ("Taskflow, block-DAG (pinned)", taskflow, "#555555", "-", "s", 1.4, 3),
    ]
    footnote = None

    if include_tbb:
        tbb_flow = load_tbb_flow()
        sources.append(("TBB flow", tbb_flow))
        series.append(("TBB flow_graph, block-DAG†", tbb_flow, "#999999", "--", "D", 1.4, 2))
        footnote = (
            "† TBB flow_graph: worker count via global_control (limits TBB global pool); "
            "no core pinning.\n"
            "  Results are informative but not directly comparable to pinned measurements."
        )

    missing = [
        (name, n, w)
        for name, data in sources
        for n in NS
        for w, v in zip(WORKERS, data[n])
        if v is None
    ]
    if missing:
        print(f"  [blockdag] Missing data: {missing}")

    fig = make_figure(series, footnote=footnote)
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved: {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-tbb", action="store_true", help="Add TBB baseline series")
    args = parser.parse_args()

    figs_dir = os.path.dirname(os.path.abspath(__file__))
    figure_antidiag(os.path.join(figs_dir, "wavefront-antidiag.png"), include_tbb=args.include_tbb)
    figure_blockdag(os.path.join(figs_dir, "wavefront-blockdag.png"), include_tbb=args.include_tbb)
    print("Done.")
