#!/usr/bin/env python3
"""
pipeline-comparison.py
======================
Tomii vs Taskflow comparison for the 4-stage linear pipeline benchmark.

  Fan-out/fan-in pipeline, N=256 items per stream
  S concurrent slots/lines in {1, 4, 16, 64}
  Fixed worker count W=4 (override with --workers)

Reads from:
  pipeline-bench/tomii/results/pipeline_sweep.csv
  pipeline-bench/taskflow/build/tf_pipeline_sweep.csv

Output:
  pipeline-bench/pipeline-comparison.png

Usage (from bench worktree root):
    python pipeline-bench/pipeline-comparison.py
    python pipeline-bench/pipeline-comparison.py --workers 8
"""

from __future__ import annotations

import argparse
import csv
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_TOMII_CSV = os.path.join(
    SCRIPT_DIR, "tomii", "results", "pipeline_sweep_heavy.csv"
)
_DEFAULT_TF_CSV = os.path.join(
    SCRIPT_DIR, "taskflow", "build", "tf_pipeline_sweep_heavy.csv"
)
# Set at parse time; module-level names kept for backward compatibility.
TOMII_CSV = _DEFAULT_TOMII_CSV
TASKFLOW_CSV = _DEFAULT_TF_CSV

SLOTS = [1, 4, 16, 64]
N_ITEMS = 256
KERNEL_SIZES = [1, 512, 2048, 8192]  # TRANSFORM_ITERS values in kernel-sweep CSVs
# Approximate µs/task for each TRANSFORM_ITERS value (for axis labels).
_ITERS_LABEL = {1: "~0 µs", 512: "~4 µs", 2048: "~16 µs", 8192: "~64 µs"}


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _load_throughput(
    path: str,
    system_filter: str | None,
    workers: int,
    slots: list[int],
    n: int,
    transform_iters: int | None = None,
) -> list[float | None]:
    """Return throughput (streams/s) for each slot count at a fixed worker count.

    If transform_iters is given and the CSV has that column, only rows with that
    exact iters value are used (prevents mixing kernel sizes in combined CSVs).
    """
    data: dict[int, float] = {}
    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return [None] * len(slots)

    with open(path) as f:
        for row in csv.DictReader(f):
            try:
                if int(row.get("n", 0)) != n:
                    continue
                if system_filter and row.get("system") != system_filter:
                    continue
                if transform_iters is not None and "transform_iters" in row:
                    if int(row["transform_iters"]) != transform_iters:
                        continue
                w = int(row["workers"])
                if w != workers:
                    continue
                s = int(row["slots"])
                ms = float(row["ms_per_stream"])
                if ms > 0.0:
                    tp = 1000.0 / ms  # streams/s
                    # Keep best (highest) throughput if multiple rows match.
                    if s not in data or tp > data[s]:
                        data[s] = tp
            except (KeyError, ValueError):
                continue

    return [data.get(s) for s in slots]


def load_tomii(
    workers: int,
    slots: list[int] = SLOTS,
    n: int = N_ITEMS,
    transform_iters: int | None = None,
) -> list[float | None]:
    return _load_throughput(TOMII_CSV, "tomii", workers, slots, n, transform_iters)


def load_taskflow_clone(
    workers: int,
    slots: list[int] = SLOTS,
    n: int = N_ITEMS,
    transform_iters: int | None = None,
) -> list[float | None]:
    return _load_throughput(
        TASKFLOW_CSV, "taskflow_clone", workers, slots, n, transform_iters
    )


def load_taskflow_sequential(
    workers: int,
    slots: list[int] = SLOTS,
    n: int = N_ITEMS,
    transform_iters: int | None = None,
) -> list[float | None]:
    return _load_throughput(
        TASKFLOW_CSV, "taskflow_sequential", workers, slots, n, transform_iters
    )


def _load_ratio_vs_kernel(
    tomii_csv: str,
    tf_csv: str,
    workers: int,
    slots: int,
    iters_list: list[int],
    n: int = N_ITEMS,
) -> list[float | None]:
    """Return TF/Tomii throughput ratio for each kernel size at fixed (W, S)."""

    def _best(path, system_filter, w, s, iters):
        if not os.path.exists(path):
            return None
        best = None
        with open(path) as f:
            for row in csv.DictReader(f):
                try:
                    if int(row.get("n", 0)) != n:
                        continue
                    if system_filter and row.get("system") != system_filter:
                        continue
                    if int(row["workers"]) != w:
                        continue
                    if int(row["slots"]) != s:
                        continue
                    if (
                        "transform_iters" in row
                        and int(row["transform_iters"]) != iters
                    ):
                        continue
                    ms = float(row["ms_per_stream"])
                    if ms > 0.0:
                        tp = 1000.0 / ms
                        if best is None or tp > best:
                            best = tp
                except (KeyError, ValueError):
                    continue
        return best

    ratios = []
    for iters in iters_list:
        tomii_tp = _best(tomii_csv, "tomii", workers, slots, iters)
        tf_tp = _best(tf_csv, "taskflow_clone", workers, slots, iters)
        if tomii_tp and tf_tp and tomii_tp > 0:
            ratios.append(tf_tp / tomii_tp)
        else:
            ratios.append(None)
    return ratios


# ---------------------------------------------------------------------------
# Plot helpers
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


def plot_line(ax, slots, vals, label, color, ls, marker):
    pairs = [(s, v) for s, v in zip(slots, vals) if v is not None]
    if not pairs:
        print(f"  Warning: no data for {label}")
        return
    sx, sy = zip(*pairs)
    ax.plot(
        sx,
        sy,
        color=color,
        linestyle=ls,
        marker=marker,
        linewidth=1.4,
        markersize=4,
        label=label,
    )


def fmt_throughput(x, _):
    if x >= 1e6:
        return f"{x / 1e6:.1f}M"
    if x >= 1e3:
        return f"{x / 1e3:.0f}k"
    return f"{x:.0f}"


# ---------------------------------------------------------------------------
# Figure
# ---------------------------------------------------------------------------


def figure_pipeline(workers: int, transform_iters: int | None = None) -> None:
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=(3.6, 3.0))

    iters_label = (
        f", {_ITERS_LABEL[transform_iters]}/task"
        if transform_iters in _ITERS_LABEL
        else (f", iters={transform_iters}" if transform_iters else "")
    )

    series = [
        (
            "Tomii",
            load_tomii(workers, transform_iters=transform_iters),
            "#1f77b4",
            "-",
            "o",
        ),
        (
            "Taskflow (clone)",
            load_taskflow_clone(workers, transform_iters=transform_iters),
            "#d62728",
            "-",
            "s",
        ),
        (
            "Taskflow (sequential)",
            load_taskflow_sequential(workers, transform_iters=transform_iters),
            "#888888",
            "--",
            "^",
        ),
    ]

    for label, vals, color, ls, marker in series:
        plot_line(ax, SLOTS, vals, label, color, ls, marker)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(SLOTS)
    ax.set_xticklabels([str(s) for s in SLOTS], fontsize=7.5)
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10, subs=[1, 2, 5], numticks=10))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_throughput))
    ax.yaxis.set_minor_formatter(mticker.NullFormatter())

    ax.set_title(
        rf"Pipeline throughput, $N={N_ITEMS}$, $W={workers}${iters_label}",
        fontsize=9,
        pad=4,
    )
    ax.set_xlabel("Concurrent slots (S)", fontsize=8)
    ax.set_ylabel("Throughput (streams/s)", fontsize=8)
    ax.grid(True, which="major")
    ax.grid(True, which="minor", linewidth=0.2, alpha=0.2)
    ax.legend(
        loc="upper left",
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.4,
    )

    out = os.path.join(SCRIPT_DIR, "pipeline-comparison.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


def figure_kernel_sweep(
    workers: int, slots_to_plot: list[int], kernel_sizes: list[int] = KERNEL_SIZES
) -> None:
    """Plot TF/Tomii throughput ratio vs kernel size (TRANSFORM_ITERS) at fixed W.

    Each curve is one S value; x-axis shows TRANSFORM_ITERS on a log scale with
    µs labels; y=1 marks parity; y<1 means Tomii wins.
    """
    plt.rcParams.update(RC)
    fig, ax = plt.subplots(figsize=(4.0, 3.2))

    colors = ["#1f77b4", "#d62728", "#2ca02c", "#ff7f0e"]
    markers = ["o", "s", "^", "D"]

    for i, s in enumerate(slots_to_plot):
        ratios = _load_ratio_vs_kernel(
            TOMII_CSV,
            TASKFLOW_CSV,
            workers=workers,
            slots=s,
            iters_list=kernel_sizes,
        )
        pairs = [(k, r) for k, r in zip(kernel_sizes, ratios) if r is not None]
        if not pairs:
            print(f"  Warning: no ratio data for S={s}")
            continue
        kx, ky = zip(*pairs)
        ax.plot(
            kx,
            ky,
            color=colors[i % len(colors)],
            linestyle="-",
            marker=markers[i % len(markers)],
            linewidth=1.4,
            markersize=4,
            label=f"S={s}",
        )

    ax.axhline(
        1.0, color="black", linewidth=0.8, linestyle="--", label="parity (TF=Tomii)"
    )

    ax.set_xscale("log")
    ax.set_xticks(kernel_sizes)
    ax.set_xticklabels([_ITERS_LABEL.get(k, str(k)) for k in kernel_sizes], fontsize=7)
    ax.xaxis.set_minor_formatter(mticker.NullFormatter())
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}×"))

    ax.set_title(
        rf"Taskflow / Tomii throughput ratio vs kernel weight, $W={workers}$",
        fontsize=8.5,
        pad=4,
    )
    ax.set_xlabel("Kernel weight (TRANSFORM_ITERS)", fontsize=8)
    ax.set_ylabel("TF / Tomii ratio  (lower = Tomii closer to TF)", fontsize=7.5)
    ax.grid(True, which="major")
    ax.legend(
        loc="upper right",
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.2,
    )

    out = os.path.join(SCRIPT_DIR, "pipeline-kernel-sweep.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# High-S loaders
# ---------------------------------------------------------------------------


def _load_highS(
    path: str, system_filter: str | None, workers: int, n: int
) -> tuple[list[int], list[float | None], list[float | None]]:
    """Return (slots, ms_per_stream_list, rss_mb_list) from a high-S CSV.

    The high-S CSV has an extra peak_rss_kb column appended by the sweep
    script.  Falls back gracefully if the column is absent.
    """
    slots_out: list[int] = []
    ms_out: list[float | None] = []
    rss_out: list[float | None] = []

    if not os.path.exists(path):
        print(f"  Warning: {path} not found")
        return slots_out, ms_out, rss_out

    seen: dict[int, tuple[float | None, float | None]] = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            try:
                if int(row.get("n", 0)) != n:
                    continue
                if system_filter and row.get("system") != system_filter:
                    continue
                if int(row["workers"]) != workers:
                    continue
                s = int(row["slots"])
                ms_str = row.get("ms_per_stream", "")
                ms = float(ms_str) if ms_str and ms_str != "NaN" else None
                rss_str = row.get("peak_rss_kb", "")
                rss_mb = (
                    float(rss_str) / 1024.0
                    if rss_str and rss_str not in ("", "0")
                    else None
                )
                # Keep first valid row per slot.
                if s not in seen:
                    seen[s] = (ms, rss_mb)
            except (KeyError, ValueError):
                continue

    for s in sorted(seen):
        slots_out.append(s)
        ms_val, rss_val = seen[s]
        ms_out.append(ms_val)
        rss_out.append(rss_val)

    return slots_out, ms_out, rss_out


# ---------------------------------------------------------------------------
# High-S two-panel figure
# ---------------------------------------------------------------------------


def figure_highS(
    tomii_csv: str,
    tf_csv: str,
    workers: int = 4,
    n: int = N_ITEMS,
) -> None:
    """Two-panel plot: throughput (streams/s) and RSS (MB) vs S (log-log).

    Left panel: throughput — both systems, log-log.
    Right panel: peak RSS — both systems, log-log.
    Shows whether Tomii's slot memory scales better than Taskflow's clone-mode
    at high concurrent-stream counts.
    """
    plt.rcParams.update(RC)
    fig, (ax_tp, ax_rss) = plt.subplots(1, 2, figsize=(7.2, 3.0))

    tomii_slots, tomii_ms, tomii_rss = _load_highS(tomii_csv, "tomii", workers, n)
    tf_slots, tf_ms, tf_rss = _load_highS(tf_csv, "taskflow_clone", workers, n)

    def _tp(ms_list: list[float | None]) -> list[float | None]:
        return [1000.0 / ms if ms else None for ms in ms_list]

    # --- Throughput panel ---
    plot_line(ax_tp, tomii_slots, _tp(tomii_ms), "Tomii", "#1f77b4", "-", "o")
    plot_line(ax_tp, tf_slots, _tp(tf_ms), "Taskflow (clone)", "#d62728", "-", "s")

    ax_tp.set_xscale("log")
    ax_tp.set_yscale("log")
    ax_tp.set_xlabel("Concurrent slots (S)", fontsize=8)
    ax_tp.set_ylabel("Throughput (streams/s)", fontsize=8)
    ax_tp.set_title(rf"Throughput vs S, $N={n}$, $W={workers}$", fontsize=9, pad=4)
    ax_tp.yaxis.set_major_formatter(mticker.FuncFormatter(fmt_throughput))
    ax_tp.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax_tp.grid(True, which="major")
    ax_tp.legend(
        loc="lower left",
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.2,
    )

    # --- RSS panel ---
    def _valid_rss(slots, rss_list):
        pairs = [(s, r) for s, r in zip(slots, rss_list) if r is not None and r > 0]
        return zip(*pairs) if pairs else ([], [])

    tx, ty = list(_valid_rss(tomii_slots, tomii_rss))
    fx, fy = list(_valid_rss(tf_slots, tf_rss))
    tx, ty = list(tx), list(ty)
    fx, fy = list(fx), list(fy)

    if tx:
        ax_rss.plot(
            tx,
            ty,
            color="#1f77b4",
            linestyle="-",
            marker="o",
            linewidth=1.4,
            markersize=4,
            label="Tomii (measured)",
        )

    if fx:
        ax_rss.plot(
            fx,
            fy,
            color="#d62728",
            linestyle="-",
            marker="s",
            linewidth=1.4,
            markersize=4,
            label="Taskflow (clone)",
        )

    # Linear extrapolation of Tomii's trend beyond the measured cap.
    # Uses the measured slope (kB/slot) to project RSS at higher S values,
    # shown as a dashed line so the reader can see the projected crossover.
    if len(tx) >= 2 and fx:
        import numpy as np

        slope_kb = (ty[-1] * 1024 - ty[0] * 1024) / (tx[-1] - tx[0])  # kB/slot
        intercept_kb = ty[-1] * 1024 - slope_kb * tx[-1]
        s_max = max(fx)
        s_extrap = np.logspace(np.log10(tx[-1]), np.log10(s_max), 40)
        rss_extrap = (intercept_kb + slope_kb * s_extrap) / 1024.0
        ax_rss.plot(
            s_extrap,
            rss_extrap,
            color="#1f77b4",
            linestyle="--",
            linewidth=1.1,
            label="Tomii (extrapolated)",
            alpha=0.7,
        )
        # Mark the crossover if it falls within the extrapolation range.
        if len(fy) >= 2:
            # Fit a linear slope to Taskflow (in kB) over the full S range.
            tf_slope_kb = (fy[-1] * 1024 - fy[0] * 1024) / (fx[-1] - fx[0])
            tf_intercept_kb = fy[0] * 1024 - tf_slope_kb * fx[0]
            denom = tf_slope_kb - slope_kb
            if denom > 0:
                s_cross = (intercept_kb - tf_intercept_kb) / denom
                if tx[-1] < s_cross <= s_max:
                    ax_rss.axvline(s_cross, color="gray", linestyle=":", linewidth=0.8)
                    ax_rss.annotate(
                        f"≈S={int(s_cross)}",
                        xy=(s_cross, rss_extrap[np.argmin(np.abs(s_extrap - s_cross))]),
                        xytext=(
                            s_cross * 1.15,
                            rss_extrap[np.argmin(np.abs(s_extrap - s_cross))] * 0.7,
                        ),
                        fontsize=6.5,
                        color="gray",
                        arrowprops=dict(arrowstyle="-", color="gray", lw=0.7),
                    )

    ax_rss.set_xscale("log")
    ax_rss.set_yscale("log")
    ax_rss.set_xlabel("Concurrent slots (S)", fontsize=8)
    ax_rss.set_ylabel("Peak RSS (MB)", fontsize=8)
    ax_rss.set_title(rf"Memory vs S, $N={n}$, $W={workers}$", fontsize=9, pad=4)
    ax_rss.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda v, _: f"{v:.0f}" if v >= 1 else f"{v:.2f}")
    )
    ax_rss.yaxis.set_minor_formatter(mticker.NullFormatter())
    ax_rss.grid(True, which="major")
    ax_rss.legend(
        loc="upper left",
        frameon=True,
        framealpha=1.0,
        edgecolor="#cccccc",
        fontsize=7.5,
        handlelength=2.2,
    )

    fig.tight_layout(pad=0.8)
    out = os.path.join(SCRIPT_DIR, "pipeline-highS.png")
    fig.savefig(out, dpi=200, bbox_inches="tight")
    print(f"Saved: {out}")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_DEFAULT_HIGHS_TOMII_CSV = os.path.join(
    SCRIPT_DIR, "tomii", "results", "pipeline_highS.csv"
)
_DEFAULT_HIGHS_TF_CSV = os.path.join(
    SCRIPT_DIR, "taskflow", "build", "tf_pipeline_highS.csv"
)


def main() -> None:
    global TOMII_CSV, TASKFLOW_CSV
    p = argparse.ArgumentParser(
        description="Plot Tomii vs Taskflow pipeline comparison."
    )
    p.add_argument(
        "--workers", type=int, default=4, help="fixed worker count to plot (default 4)"
    )
    p.add_argument(
        "--slots",
        type=int,
        nargs="+",
        default=SLOTS,
        help="slot counts shown on x-axis",
    )
    p.add_argument(
        "--n", type=int, default=N_ITEMS, help="items per stream to filter on"
    )
    p.add_argument(
        "--tomii-csv",
        default=None,
        help=f"Tomii CSV path (default: {_DEFAULT_TOMII_CSV})",
    )
    p.add_argument(
        "--taskflow-csv",
        default=None,
        help=f"Taskflow CSV path (default: {_DEFAULT_TF_CSV})",
    )
    p.add_argument(
        "--kernel-sweep",
        action="store_true",
        help="also produce pipeline-kernel-sweep.png from combined "
        "kernel-sweep CSVs (requires transform_iters column)",
    )
    p.add_argument(
        "--transform-iters",
        type=int,
        default=None,
        help="filter throughput plot to a specific TRANSFORM_ITERS value",
    )
    # High-S two-panel plot
    p.add_argument(
        "--highS",
        action="store_true",
        help="produce pipeline-highS.png (throughput + RSS vs high S)",
    )
    p.add_argument(
        "--highS-tomii-csv",
        default=None,
        help=f"high-S Tomii CSV (default: {_DEFAULT_HIGHS_TOMII_CSV})",
    )
    p.add_argument(
        "--highS-taskflow-csv",
        default=None,
        help=f"high-S Taskflow CSV (default: {_DEFAULT_HIGHS_TF_CSV})",
    )
    args = p.parse_args()

    if args.tomii_csv:
        TOMII_CSV = args.tomii_csv
    if args.taskflow_csv:
        TASKFLOW_CSV = args.taskflow_csv

    figure_pipeline(args.workers, transform_iters=args.transform_iters)

    if args.kernel_sweep:
        figure_kernel_sweep(args.workers, args.slots)

    if args.highS:
        figure_highS(
            tomii_csv=args.highS_tomii_csv or _DEFAULT_HIGHS_TOMII_CSV,
            tf_csv=args.highS_taskflow_csv or _DEFAULT_HIGHS_TF_CSV,
            workers=args.workers,
            n=args.n,
        )


if __name__ == "__main__":
    main()
