"""Aggregate benchmark results and generate comparison plots.

Reads all ``*.csv`` files from --results-dir, merges them, and produces:
  - stream_comparison.png       — 2x2 grid of GB/s vs workers, best config per system
  - stream_design_choices.png   — 1x4 bar chart of SynStream design variants
  - pagerank_comparison.png     — wall-clock time vs workers per dataset, best config per system
  - pagerank_design_choices.png — bar chart of SynStream PageRank design variants (if data present)

Usage:
    python benchmarks/compare_results.py \\
        --results-dir benchmarks/results/csvs \\
        --output-dir  benchmarks/results/plots
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

# ── optional deps: gracefully degrade if matplotlib/pandas not installed ──────
try:
    import matplotlib

    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import numpy as np

    HAS_PLOT = True
except ImportError:
    HAS_PLOT = False
    print("[warn] matplotlib not found — skipping plots", file=sys.stderr)

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("[warn] pandas not found — printing raw tables only", file=sys.stderr)


# ── helpers ───────────────────────────────────────────────────────────────────


def load_stream_csv(path: Path) -> Optional["pd.DataFrame"]:
    """Load a single STREAM result CSV if it exists and has data rows."""
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path)
        if df.empty:
            return None
        return df
    except Exception:
        return None


def gather_stream_results(results_dir: Path) -> Optional["pd.DataFrame"]:
    """Collect all STREAM CSV files into a single DataFrame."""
    frames = []
    for f in results_dir.glob("*.csv"):
        df = load_stream_csv(f)
        if df is None:
            continue
        if "gb_s" in df.columns and "kernel" in df.columns:
            frames.append(df)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def gather_pagerank_results(results_dir: Path) -> Optional["pd.DataFrame"]:
    """Collect all PageRank CSV files into a single DataFrame."""
    frames = []
    for f in results_dir.glob("*.csv"):
        df = load_stream_csv(f)
        if df is None:
            continue
        if "s_per_iter" in df.columns and "dataset" in df.columns:
            frames.append(df)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def gather_wavefront_results(results_dir: Path) -> Optional["pd.DataFrame"]:
    """Collect all Wavefront CSV files (schema: system,n,workers,iterations,total_s,s_per_iter)."""
    frames = []
    for f in results_dir.glob("*.csv"):
        df = load_stream_csv(f)
        if df is None:
            continue
        if "s_per_iter" in df.columns and "n" in df.columns:
            frames.append(df)
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    # Keep only the last row per (system, n, workers) to handle append-mode duplicates
    # from repeated benchmark runs.
    combined = combined.groupby(["system", "n", "workers"], as_index=False).last()
    return combined


# ── style ─────────────────────────────────────────────────────────────────────

COLORS = {
    "synstream": "#1f77b4",
    "synstream_pooled": "#1f77b4",
    "synstream_init_pooled": "#1f77b4",  # same hue; main plot uses best variant
    "synstream_serial": "#aec7e8",  # light blue for PageRank base variant
    "synstream_st1": "#aec7e8",       # light blue — 1 system thread (untiled, legacy)
    "synstream_st1_t1": "#aec7e8",   # tile=1 (same as untiled)
    "synstream_st1_t8": "#6baed6",   # tile=8
    "synstream_st1_t32": "#1f77b4",  # tile=32 (best anti-diagonal config)
    "synstream_block_t32": "#2171b5",  # 2D block DAG, tile=32
    "synstream_st2": "#6baed6",    # medium blue — 2 system threads
    "synstream_st4": "#1f77b4",    # full blue — 4 system threads
    "timely": "#ff7f0e",
    "timely_pooled": "#ffbb78",
    "timely_pinned": "#d62728",
    "tbb": "#2ca02c",
    "tbb_pinned": "#98df8a",
    "taskflow_pinned": "#9467bd",  # purple
    "taskflow_block_pinned": "#7b2d8b",  # dark purple — block DAG variant
}
MARKERS = {
    "synstream": "o",
    "synstream_pooled": "o",
    "synstream_init_pooled": "o",
    "synstream_serial": "o",
    "synstream_st1": "o",
    "synstream_st1_t1": "o",
    "synstream_st1_t8": "o",
    "synstream_st1_t32": "o",
    "synstream_block_t32": "o",
    "synstream_st2": "o",
    "synstream_st4": "o",
    "timely": "s",
    "timely_pooled": "s",
    "timely_pinned": "^",
    "tbb": "D",
    "tbb_pinned": "D",
    "taskflow_pinned": "P",  # plus marker
    "taskflow_block_pinned": "P",
}
# Labels for main comparison plots (best config per system shown as the system name)
LABELS = {
    "synstream": "SynStream",
    "synstream_pooled": "SynStream",
    "synstream_init_pooled": "SynStream",
    "synstream_serial": "SynStream (serial)",
    "synstream_st1": "SynStream (no tiling)",
    "synstream_st1_t1": "SynStream (tile=1)",
    "synstream_st1_t8": "SynStream (tile=8)",
    "synstream_st1_t32": "SynStream (tile=32)",
    "synstream_block_t32": "SynStream (block DAG, tile=32)",
    "synstream_st2": "SynStream (2 sys-threads)",
    "synstream_st4": "SynStream (4 sys-threads)",
    "timely": "Timely",
    "timely_pooled": "Timely",
    "timely_pinned": "Timely (taskset-pinned)",
    "tbb": "Intel TBB",
    "tbb_pinned": "Intel TBB (pinned)",
    "taskflow_pinned": "Taskflow (pinned)",
    "taskflow_block_pinned": "Taskflow (block DAG, tile=32)",
}

# ── main comparison plot configuration ────────────────────────────────────────

# Best configuration per system — these are the only series shown in the main plots.
STREAM_ORDER = ["synstream_init_pooled", "timely_pooled", "tbb"]
PAGERANK_ORDER = ["synstream", "timely", "tbb"]
# Main comparison: best implementation per system
WAVEFRONT_ORDER = ["synstream_block_t32", "taskflow_block_pinned", "tbb_pinned", "timely"]
# SynStream internal variants for the programmability subplot
SYNSTREAM_WF_VARIANT_ORDER = ["synstream_st1_t1", "synstream_st1_t8", "synstream_st1_t32", "synstream_block_t32"]

LINESTYLES = {
    "synstream": "--",
    "synstream_pooled": "-",
    "synstream_init_pooled": "-",
    "synstream_st1": ":",
    "synstream_st1_t1": ":",
    "synstream_st1_t8": "--",
    "synstream_st1_t32": "-",
    "synstream_block_t32": "--",
    "synstream_st2": "--",
    "synstream_st4": "-",
    "timely": "-",
    "timely_pooled": "--",
    "tbb": "-",
    "tbb_pinned": "--",
    "taskflow_pinned": "-.",
    "taskflow_block_pinned": "--",
}

# ── SynStream design-choice variant configuration ─────────────────────────────

# STREAM variants ordered from least-optimised to best.
# Add more entries here as new variants are benchmarked.
SYNSTREAM_STREAM_VARIANTS_ORDER = [
    "synstream",
    "synstream_pooled",
    "synstream_init_pooled",
]
SYNSTREAM_STREAM_VARIANT_LABELS = {
    "synstream": "Base\n(per-stream alloc)",
    "synstream_pooled": "Pooled\n(Mutex pools)",
    "synstream_init_pooled": "Init-Pooled\n(per-worker init)",
}
SYNSTREAM_STREAM_VARIANT_COLORS = {
    "synstream": "#aec7e8",  # light blue
    "synstream_pooled": "#6baed6",  # medium blue
    "synstream_init_pooled": "#1f77b4",  # full blue
}

# Wavefront SynStream variants — for the programmability subplot.
SYNSTREAM_WF_VARIANT_LABELS = {
    "synstream_st1_t1":   "Anti-diag\n(tile=1)",
    "synstream_st1_t8":   "Anti-diag\n(tile=8)",
    "synstream_st1_t32":  "Anti-diag\n(tile=32)",
    "synstream_block_t32": "Block DAG\n(tile=32)",
}
SYNSTREAM_WF_VARIANT_COLORS = {
    "synstream_st1_t1":   "#c6dbef",
    "synstream_st1_t8":   "#6baed6",
    "synstream_st1_t32":  "#2171b5",
    "synstream_block_t32": "#08306b",
}

# PageRank variants.  The "synstream_serial" entry requires a separate CSV with
# system="synstream_serial" (old code: per-stream partition, serial gather/reduce).
# If absent from data the bar chart is silently skipped.
SYNSTREAM_PR_VARIANTS_ORDER = ["synstream_serial", "synstream"]
SYNSTREAM_PR_VARIANT_LABELS = {
    "synstream_serial": "Serial\n(per-stream partition)",
    "synstream": "Optimised\n(parallel gather+reduce)",
}
SYNSTREAM_PR_VARIANT_COLORS = {
    "synstream_serial": "#aec7e8",
    "synstream": "#1f77b4",
}


# ── main comparison plots ─────────────────────────────────────────────────────


def plot_stream(df: "pd.DataFrame", out_dir: Path, peak_bw: float = None) -> None:
    """2x2 line plot — best configuration per system."""
    kernels = ["copy", "scale", "add", "triad"]
    fig, axes = plt.subplots(2, 2, figsize=(12, 9), sharex=False, sharey=False)
    fig.suptitle(
        "STREAM Memory Bandwidth: SynStream vs Timely vs Intel TBB", fontsize=13
    )

    for ax, kernel in zip(axes.flat, kernels):
        sub = df[df["kernel"] == kernel]
        present = [s for s in STREAM_ORDER if s in sub["system"].values]
        for system in present:
            grp = sub[sub["system"] == system].sort_values("workers")
            ax.plot(
                grp["workers"],
                grp["gb_s"],
                label=LABELS.get(system, system),
                color=COLORS.get(system, "gray"),
                marker=MARKERS.get(system, "^"),
                linestyle=LINESTYLES.get(system, "-"),
                linewidth=1.8,
                markersize=6,
            )
        if peak_bw is not None:
            ax.plot(
                [],
                [],
                color="black",
                linestyle=":",
                linewidth=1.2,
                label=f"Peak ({peak_bw:.0f} GB/s)",
            )
        ax.set_title(f"STREAM {kernel.capitalize()}")
        ax.set_xlabel("Workers")
        ax.set_ylabel("GB/s")
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.3)
        worker_ticks = sorted(sub["workers"].unique())
        ax.set_xticks(worker_ticks)
        ax.set_xticklabels([str(w) for w in worker_ticks])

    plt.tight_layout()
    out_path = out_dir / "stream_comparison.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved {out_path}")


def plot_pagerank(df: "pd.DataFrame", out_dir: Path, peak_bw: float = None) -> None:
    """Line plot — best configuration per system, one panel per dataset."""
    datasets = df["dataset"].unique()
    ncols = max(1, len(datasets))
    fig, axes = plt.subplots(1, ncols, figsize=(6 * ncols, 5))
    if ncols == 1:
        axes = [axes]
    fig.suptitle("PageRank (COST): SynStream vs Timely vs Intel TBB", fontsize=13)

    for ax, dataset in zip(axes, sorted(datasets)):
        sub = df[df["dataset"] == dataset]
        present = [s for s in PAGERANK_ORDER if s in sub["system"].values]
        for system in present:
            grp = sub[sub["system"] == system].sort_values("workers")
            ax.plot(
                grp["workers"],
                grp["s_per_iter"],
                label=LABELS.get(system, system),
                color=COLORS.get(system, "gray"),
                marker=MARKERS.get(system, "^"),
                linestyle=LINESTYLES.get(system, "-"),
                linewidth=1.8,
                markersize=6,
            )
        ax.set_title(f"PageRank — {dataset}")
        ax.set_xlabel("Workers")
        ax.set_ylabel("Time per iteration (s)")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path = out_dir / "pagerank_comparison.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved {out_path}")


# ── design-choice bar charts ──────────────────────────────────────────────────


def _bar_group(
    ax, x, worker_counts, variants, variant_labels, variant_colors, df_sub, metric_col
):
    """Draw grouped bars for each variant onto ax.  Returns True if any data was plotted."""
    n = len(variants)
    width = 0.7 / n
    any_data = False
    for i, system in enumerate(variants):
        grp = df_sub[df_sub["system"] == system]
        vals = []
        for w in worker_counts:
            row = grp[grp["workers"] == w][metric_col]
            vals.append(float(row.mean()) if not row.empty else 0.0)
        offset = (i - n / 2 + 0.5) * width
        ax.bar(
            x + offset,
            vals,
            width,
            label=variant_labels.get(system, system),
            color=variant_colors.get(system, "gray"),
            alpha=0.88,
            edgecolor="white",
            linewidth=0.5,
        )
        if any(v > 0 for v in vals):
            any_data = True
    return any_data


def plot_stream_design_choices(df: "pd.DataFrame", out_dir: Path) -> None:
    """1x4 grouped bar chart comparing SynStream STREAM design variants.

    Shows all four kernels side-by-side; within each panel bars are grouped by
    worker count.  Silently skipped if fewer than two SynStream variants are
    present in the data.
    """
    variants = [s for s in SYNSTREAM_STREAM_VARIANTS_ORDER if s in df["system"].values]
    if len(variants) < 2:
        return

    kernels = ["copy", "scale", "add", "triad"]
    worker_counts = sorted(df["workers"].unique())
    x = np.arange(len(worker_counts))

    fig, axes = plt.subplots(2, len(kernels) // 2, figsize=(8, 8), sharey=False)
    fig.suptitle("SynStream Design Choices — STREAM Benchmark", fontsize=12)

    legend_done = False
    for ax, kernel in zip(axes.flat, kernels):
        sub = df[df["kernel"] == kernel]
        _bar_group(
            ax,
            x,
            worker_counts,
            variants,
            SYNSTREAM_STREAM_VARIANT_LABELS,
            SYNSTREAM_STREAM_VARIANT_COLORS,
            sub,
            "gb_s",
        )
        ax.set_title(f"{kernel.capitalize()}", fontsize=10)
        ax.set_xlabel("Workers")
        ax.set_ylabel("GB/s")
        ax.set_xticks(x)
        ax.set_xticklabels([str(w) for w in worker_counts])
        ax.grid(True, alpha=0.3, axis="y")
        if not legend_done:
            ax.legend(fontsize=8)
            legend_done = True

    plt.tight_layout()
    out_path = out_dir / "stream_design_choices.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved {out_path}")


def plot_wavefront(df: "pd.DataFrame", out_dir: Path) -> None:
    """Line plot — time per iteration vs workers, one panel per grid size N.

    Uses log-scale y-axis because SynStream is orders of magnitude slower than
    TBB/Timely/Taskflow on this fine-grained benchmark.
    """
    # Only plot N values that have at least one non-SynStream baseline result,
    # to avoid empty/partial subplots from ad-hoc test runs (e.g. N=8 debug runs).
    baseline_systems = {"tbb", "tbb_pinned", "taskflow", "taskflow_pinned",
                        "taskflow_block_pinned", "timely", "timely_pooled", "timely_pinned"}
    n_with_baselines = set(df[df["system"].isin(baseline_systems)]["n"].unique())
    n_vals = sorted(n for n in df["n"].unique() if n in n_with_baselines)
    nrows = (len(n_vals) + 1) // 2
    ncols = min(2, len(n_vals))
    fig, axes = plt.subplots(nrows, ncols, figsize=(14, 5.5 * nrows))
    if len(n_vals) == 1:
        axes = np.array([axes])
    axes_flat = axes.flat if hasattr(axes, 'flat') else [axes]
    fig.suptitle(
        "Wavefront: SynStream vs Timely vs Intel TBB vs Taskflow",
        fontsize=22,
    )

    for ai, (ax, n) in enumerate(zip(axes_flat, n_vals)):
        sub = df[df["n"] == n]
        present = [s for s in WAVEFRONT_ORDER if s in sub["system"].values]
        for system in present:
            grp = sub[sub["system"] == system].sort_values("workers")
            ax.plot(
                grp["workers"],
                grp["s_per_iter"] * 1000,  # Convert to milliseconds
                label=LABELS.get(system, system),
                color=COLORS.get(system, "gray"),
                marker=MARKERS.get(system, "^"),
                linestyle=LINESTYLES.get(system, "-"),
                linewidth=2.2,
                markersize=12,
            )
        ax.set_title(f"N={n}", fontsize=20, fontweight="bold")
        ax.set_xlabel("Workers", fontsize=20)
        if ai % ncols == 0:
            ax.set_ylabel("Time per sweep (ms)", fontsize=20)
        ax.set_yscale("log")
        ax.tick_params(axis="both", labelsize=18)
        ax.grid(True, alpha=0.3, which="both")
        worker_ticks = sorted(sub["workers"].unique())
        ax.set_xticks(worker_ticks)
        ax.set_xticklabels([str(w) for w in worker_ticks], fontsize=18)

    # Shared legend centred below all subplots (avoids per-panel clutter)
    handles, labels = [], []
    seen = set()
    for ax in fig.axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            if l not in seen:
                handles.append(h)
                labels.append(l)
                seen.add(l)
    if handles:
        fig.legend(
            handles,
            labels,
            loc="lower center",
            ncol=min(len(handles), 4),
            fontsize=18,
            frameon=True,
            bbox_to_anchor=(0.5, 0.0),
        )
    plt.tight_layout(rect=[0, 0.08, 1, 1])
    out_path = out_dir / "wavefront_comparison.png"
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def plot_wavefront_synstream_variants(df: "pd.DataFrame", out_dir: Path) -> None:
    """Grouped bar chart showing SynStream wavefront variant performance.

    Shows tile=1, tile=8, tile=32 (anti-diagonal) and block-DAG at fixed N values,
    across worker counts.  Illustrates programmability: different graph definitions
    yield 10-100× performance differences with no runtime changes.
    """
    variants = [s for s in SYNSTREAM_WF_VARIANT_ORDER if s in df["system"].values]
    if len(variants) < 2:
        return

    n_vals = [256, 512]
    n_vals = [n for n in n_vals if n in df["n"].unique()]
    if not n_vals:
        n_vals = sorted(df["n"].unique())[-2:]

    worker_counts = sorted(df[df["system"].isin(variants)]["workers"].unique())
    x = np.arange(len(worker_counts))
    width = 0.8 / len(variants)

    fig, axes = plt.subplots(1, len(n_vals), figsize=(8 * len(n_vals), 5.5))
    if len(n_vals) == 1:
        axes = [axes]
    fig.suptitle("SynStream Wavefront: Graph Configuration Comparison", fontsize=22)

    for ai, (ax, n) in enumerate(zip(axes, n_vals)):
        sub = df[df["n"] == n]
        for vi, system in enumerate(variants):
            grp = sub[sub["system"] == system].sort_values("workers")
            worker_map = {row.workers: row.s_per_iter * 1000 for row in grp.itertuples()}
            vals = [worker_map.get(w, float("nan")) for w in worker_counts]
            offset = (vi - (len(variants) - 1) / 2) * width
            ax.bar(
                x + offset,
                vals,
                width,
                label=SYNSTREAM_WF_VARIANT_LABELS.get(system, system),
                color=SYNSTREAM_WF_VARIANT_COLORS.get(system, "gray"),
                edgecolor="white",
                linewidth=0.5,
            )
        ax.set_title(f"N={n}", fontsize=20, fontweight="bold")
        ax.set_xlabel("Workers", fontsize=20)
        if ai == 0:
            ax.set_ylabel("Time per sweep (ms)", fontsize=20)
        ax.set_xticks(x)
        ax.set_xticklabels([str(w) for w in worker_counts], fontsize=18)
        ax.tick_params(axis="y", labelsize=18)
        ax.set_yscale("log")
        ax.grid(True, alpha=0.3, axis="y", which="both")

    # Shared legend centred below all subplots
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="lower center",
        ncol=min(len(handles), 4),
        fontsize=18,
        frameon=True,
        bbox_to_anchor=(0.5, 0.0),
    )
    plt.tight_layout(rect=[0, 0.12, 1, 1])
    out_path = out_dir / "wavefront_synstream_variants.png"
    fig.savefig(out_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def print_wavefront_table(df: "pd.DataFrame") -> None:
    print("\n── Wavefront Benchmark Summary ──")
    pivot = df.pivot_table(
        values="s_per_iter", index=["n", "workers"], columns="system", aggfunc="mean"
    )
    print(pivot.to_string())


def plot_pagerank_design_choices(df: "pd.DataFrame", out_dir: Path) -> None:
    """Grouped bar chart comparing SynStream PageRank design variants per dataset.

    Requires a CSV entry with system="synstream_serial" (per-stream partition,
    serial gather/reduce).  Silently skipped if fewer than two variants present.
    """
    variants = [s for s in SYNSTREAM_PR_VARIANTS_ORDER if s in df["system"].values]
    if len(variants) < 2:
        return

    datasets = sorted(df["dataset"].unique())
    worker_counts = sorted(df["workers"].unique())
    x = np.arange(len(worker_counts))

    fig, axes = plt.subplots(
        1, len(datasets), figsize=(6 * len(datasets), 4), sharey=False
    )
    if len(datasets) == 1:
        axes = [axes]
    fig.suptitle("SynStream Design Choices — PageRank Benchmark", fontsize=12)

    legend_done = False
    for ax, dataset in zip(axes, datasets):
        sub = df[df["dataset"] == dataset]
        _bar_group(
            ax,
            x,
            worker_counts,
            variants,
            SYNSTREAM_PR_VARIANT_LABELS,
            SYNSTREAM_PR_VARIANT_COLORS,
            sub,
            "s_per_iter",
        )
        ax.set_title(f"{dataset}", fontsize=10)
        ax.set_xlabel("Workers")
        ax.set_ylabel("Time per iteration (s)")
        ax.set_xticks(x)
        ax.set_xticklabels([str(w) for w in worker_counts])
        ax.grid(True, alpha=0.3, axis="y")
        if not legend_done:
            ax.legend(fontsize=8)
            legend_done = True

    plt.tight_layout()
    out_path = out_dir / "pagerank_design_choices.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved {out_path}")


# ── tables ────────────────────────────────────────────────────────────────────


def print_stream_table(df: "pd.DataFrame") -> None:
    print("\n── STREAM Benchmark Summary ──")
    pivot = df.pivot_table(
        values="gb_s", index=["kernel", "workers"], columns="system", aggfunc="mean"
    )
    print(pivot.to_string())


def print_pagerank_table(df: "pd.DataFrame") -> None:
    print("\n── PageRank Benchmark Summary ──")
    pivot = df.pivot_table(
        values="s_per_iter",
        index=["dataset", "workers"],
        columns="system",
        aggfunc="mean",
    )
    print(pivot.to_string())


# ── main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(description="Generate benchmark comparison plots")
    p.add_argument("--results-dir", type=Path, default=Path("benchmarks/results/csvs"))
    p.add_argument("--output-dir", type=Path, default=Path("benchmarks/results/plots"))
    p.add_argument(
        "--peak-bw",
        type=float,
        default=None,
        metavar="GB_S",
        help="theoretical peak memory bandwidth in GB/s (adds ceiling line to plots)",
    )
    args = p.parse_args()

    if not HAS_PANDAS:
        print("Install pandas to use this script:  pip install pandas matplotlib")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    stream_df = gather_stream_results(args.results_dir)
    pr_df = gather_pagerank_results(args.results_dir)
    wavefront_df = gather_wavefront_results(args.results_dir)

    if stream_df is not None and not stream_df.empty:
        print_stream_table(stream_df)
        if HAS_PLOT:
            plot_stream(stream_df, args.output_dir, peak_bw=args.peak_bw)
            plot_stream_design_choices(stream_df, args.output_dir)
    else:
        print("[info] No STREAM results found.")

    if pr_df is not None and not pr_df.empty:
        print_pagerank_table(pr_df)
        if HAS_PLOT:
            plot_pagerank(pr_df, args.output_dir, peak_bw=args.peak_bw)
            plot_pagerank_design_choices(pr_df, args.output_dir)
    else:
        print("[info] No PageRank results found.")

    if wavefront_df is not None and not wavefront_df.empty:
        print_wavefront_table(wavefront_df)
        if HAS_PLOT:
            plot_wavefront(wavefront_df, args.output_dir)
            plot_wavefront_synstream_variants(wavefront_df, args.output_dir)
    else:
        print("[info] No Wavefront results found.")


if __name__ == "__main__":
    main()
