"""Plot bilateral denoising benchmark results: Taskflow vs SynStream.

Generates:
  1. speedup_vs_threads.pdf — thread-count speedup curves (primary comparison)
  2. time_vs_threads.pdf    — absolute wall-clock time curves
  3. psnr_table.csv         — PSNR correctness summary

Usage:
    python plot_results.py \\
        --tf-csv  taskflow/results/tf_bilateral_all.csv \\
        --ss-csv  synstream/results/ss_bilateral_all.csv \\
        --out-dir results/
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def filter_rows(rows: list[dict], **kwargs) -> list[dict]:
    result = []
    for row in rows:
        if all(str(row.get(k, "")) == str(v) for k, v in kwargs.items()):
            result.append(row)
    return result


# ---------------------------------------------------------------------------
# Speedup plot: primary comparison figure
# ---------------------------------------------------------------------------

def plot_speedup(
    tf_rows:  list[dict],
    ss_rows:  list[dict],
    image_size: int,
    tile_size:  int,
    kernel_radius: int,
    thread_counts: list[int],
    out_dir: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(6, 4))

    systems = [
        ("taskflow",   tf_rows, "C0", "o", "Taskflow"),
        ("synstream",  ss_rows, "C1", "s", "SynStream"),
    ]

    for sys_key, rows, color, marker, label in systems:
        # Filter to primary configuration
        filt = [
            r for r in rows
            if int(r.get("image_size", 0)) == image_size
            and int(r.get("tile_size",  0)) == tile_size
            and int(r.get("kernel_radius", 0)) == kernel_radius
        ]
        if not filt:
            continue

        # Build (threads → mean_time_ms) map
        time_by_threads: dict[int, float] = {}
        for r in filt:
            t = int(r.get("threads", r.get("workers", 0)))
            ms = float(r.get("time_ms", r.get("s_per_iter", 1)))
            # s_per_iter → ms if needed
            if ms < 0.001:
                ms *= 1000.0
            time_by_threads[t] = ms

        if 1 not in time_by_threads:
            continue

        baseline = time_by_threads[1]
        xs = sorted(time_by_threads.keys())
        ys = [baseline / time_by_threads[x] for x in xs]

        ax.plot(xs, ys, color=color, marker=marker, label=label, linewidth=1.5)

    # Ideal speedup reference
    xs_ref = thread_counts
    ax.plot(xs_ref, xs_ref, color="gray", linestyle="--", linewidth=1,
            label="Ideal", alpha=0.6)

    ax.set_xscale("log", base=2)
    ax.set_yscale("log", base=2)
    ax.set_xticks(thread_counts)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel("Threads")
    ax.set_ylabel("Speedup (relative to 1 thread)")
    ax.set_title(
        f"Bilateral Denoising Speedup\n"
        f"image={image_size}×{image_size}, tile={tile_size}, r={kernel_radius}"
    )
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()

    out = out_dir / f"speedup_img{image_size}_tile{tile_size}_kr{kernel_radius}.pdf"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out}")


# ---------------------------------------------------------------------------
# Absolute time plot
# ---------------------------------------------------------------------------

def plot_time(
    tf_rows:  list[dict],
    ss_rows:  list[dict],
    image_size: int,
    tile_size:  int,
    kernel_radius: int,
    thread_counts: list[int],
    out_dir: Path,
) -> None:
    fig, ax = plt.subplots(figsize=(6, 4))

    systems = [
        (tf_rows, "C0", "o", "Taskflow"),
        (ss_rows, "C1", "s", "SynStream"),
    ]

    for rows, color, marker, label in systems:
        filt = [
            r for r in rows
            if int(r.get("image_size", 0)) == image_size
            and int(r.get("tile_size",  0)) == tile_size
            and int(r.get("kernel_radius", 0)) == kernel_radius
        ]
        if not filt:
            continue

        time_by_threads: dict[int, float] = {}
        for r in filt:
            t  = int(r.get("threads", r.get("workers", 0)))
            ms = float(r.get("time_ms", r.get("s_per_iter", 1)))
            if ms < 0.001:
                ms *= 1000.0
            time_by_threads[t] = ms

        xs = sorted(time_by_threads.keys())
        ys = [time_by_threads[x] for x in xs]
        ax.plot(xs, ys, color=color, marker=marker, label=label, linewidth=1.5)

    ax.set_xscale("log", base=2)
    ax.set_xticks(thread_counts)
    ax.get_xaxis().set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_xlabel("Threads")
    ax.set_ylabel("Wall-clock time (ms)")
    ax.set_title(
        f"Bilateral Denoising Time\n"
        f"image={image_size}×{image_size}, tile={tile_size}, r={kernel_radius}"
    )
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, which="both", alpha=0.3)
    fig.tight_layout()

    out = out_dir / f"time_img{image_size}_tile{tile_size}_kr{kernel_radius}.pdf"
    fig.savefig(out, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved {out}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tf-csv",  type=Path, default=Path("taskflow/results/tf_bilateral_all.csv"))
    p.add_argument("--ss-csv",  type=Path, default=Path("synstream/results/ss_bilateral_all.csv"))
    p.add_argument("--out-dir", type=Path, default=Path("results"))
    p.add_argument("--image-sizes",   type=int, nargs="+", default=[1024, 4096, 8192])
    p.add_argument("--tile-sizes",    type=int, nargs="+", default=[256])
    p.add_argument("--kernel-radii",  type=int, nargs="+", default=[4])
    p.add_argument("--thread-counts", type=int, nargs="+", default=[1, 2, 4, 8, 16])
    args = p.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    tf_rows = load_csv(args.tf_csv)
    ss_rows = load_csv(args.ss_csv)

    if not tf_rows and not ss_rows:
        print("No result CSVs found — run the benchmarks first.")
        return

    for img in args.image_sizes:
        for tile in args.tile_sizes:
            for kr in args.kernel_radii:
                plot_speedup(tf_rows, ss_rows, img, tile, kr,
                             args.thread_counts, args.out_dir)
                plot_time(tf_rows, ss_rows, img, tile, kr,
                          args.thread_counts, args.out_dir)

    print("Done.")


if __name__ == "__main__":
    main()
