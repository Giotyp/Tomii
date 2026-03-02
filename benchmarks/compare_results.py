"""Aggregate benchmark results and generate comparison plots.

Reads all ``*.csv`` files from --results-dir, merges them, and produces:
  - stream_comparison.png  — 2×2 grid of GB/s vs workers for each kernel
  - pagerank_comparison.png — wall-clock time vs workers per dataset

Usage:
    python benchmarks/compare_results.py \\
        --results-dir benchmarks/results \\
        --output-dir  benchmarks/results
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
    matplotlib.use("Agg")                          # non-interactive backend
    import matplotlib.pyplot as plt
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


# ── plots ─────────────────────────────────────────────────────────────────────

COLORS = {"synstream": "#1f77b4", "timely": "#ff7f0e", "timely_pinned": "#d62728", "tbb": "#2ca02c"}
MARKERS = {"synstream": "o", "timely": "s", "timely_pinned": "^", "tbb": "D"}
LABELS = {"synstream": "SynStream", "timely": "Timely (unpinned)", "timely_pinned": "Timely (taskset-pinned)", "tbb": "Intel TBB"}


def plot_stream(df: "pd.DataFrame", out_dir: Path) -> None:
    kernels = ["copy", "scale", "add", "triad"]
    fig, axes = plt.subplots(2, 2, figsize=(10, 8), sharex=False, sharey=False)
    fig.suptitle("STREAM Benchmark: SynStream vs Timely Dataflow", fontsize=13)

    for ax, kernel in zip(axes.flat, kernels):
        sub = df[(df["kernel"] == kernel) & (df["system"] != "timely_pinned")]
        for system, grp in sub.groupby("system"):
            grp_sorted = grp.sort_values("workers")
            ax.plot(
                grp_sorted["workers"],
                grp_sorted["gb_s"],
                label=LABELS.get(system, system),
                color=COLORS.get(system, "gray"),
                marker=MARKERS.get(system, "^"),
                linewidth=1.8,
                markersize=6,
            )
        ax.set_title(f"STREAM {kernel.capitalize()}")
        ax.set_xlabel("Workers")
        ax.set_ylabel("GB/s")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path = out_dir / "stream_comparison.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"Saved {out_path}")


def plot_pagerank(df: "pd.DataFrame", out_dir: Path) -> None:
    datasets = df["dataset"].unique()
    ncols = max(1, len(datasets))
    fig, axes = plt.subplots(1, ncols, figsize=(6 * ncols, 5))
    if ncols == 1:
        axes = [axes]
    fig.suptitle("PageRank (COST): SynStream vs Timely Dataflow", fontsize=13)

    for ax, dataset in zip(axes, sorted(datasets)):
        sub = df[df["dataset"] == dataset]
        for system, grp in sub.groupby("system"):
            grp_sorted = grp.sort_values("workers")
            ax.plot(
                grp_sorted["workers"],
                grp_sorted["s_per_iter"],
                label=system,
                color=COLORS.get(system, "gray"),
                marker=MARKERS.get(system, "^"),
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
        values="s_per_iter", index=["dataset", "workers"], columns="system", aggfunc="mean"
    )
    print(pivot.to_string())


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Generate benchmark comparison plots")
    p.add_argument("--results-dir", type=Path, default=Path("benchmarks/results"))
    p.add_argument("--output-dir",  type=Path, default=Path("benchmarks/results"))
    args = p.parse_args()

    if not HAS_PANDAS:
        print("Install pandas to use this script:  pip install pandas matplotlib")
        sys.exit(1)

    args.output_dir.mkdir(parents=True, exist_ok=True)

    stream_df = gather_stream_results(args.results_dir)
    pr_df     = gather_pagerank_results(args.results_dir)

    if stream_df is not None and not stream_df.empty:
        print_stream_table(stream_df)
        if HAS_PLOT:
            plot_stream(stream_df, args.output_dir)
    else:
        print("[info] No STREAM results found.")

    if pr_df is not None and not pr_df.empty:
        print_pagerank_table(pr_df)
        if HAS_PLOT:
            plot_pagerank(pr_df, args.output_dir)
    else:
        print("[info] No PageRank results found.")


if __name__ == "__main__":
    main()
