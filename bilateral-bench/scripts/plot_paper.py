"""Generate paper-quality figures for the bilateral denoising macro-benchmark.

Produces (in --out-dir, default paper/figs/):
  bilateral_comparison.png   — three-panel: 4096/tile=256, 4096/tile=128, 8192/tile=256
  bilateral_optims.png       — overhead vs Taskflow bar chart (4096/tile=256)

Run from bilateral-bench/:
    python scripts/plot_paper.py
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np

# ---------------------------------------------------------------------------
# Colour / style palette (match existing wavefront figures in the paper)
# ---------------------------------------------------------------------------
TF_COLOR = "#2166ac"  # blue  — Taskflow
SS_COLOR = "#d6604d"  # red   — SynStream baseline
SSIC_COLOR = "#1a9850"  # green — SynStream + IC
DEP_COLOR = "#f4a582"  # light orange — $dep only
IC_DEP_CLR = "#762a83"  # purple — IC+$dep

LABEL_TF = "Taskflow (C++)"
LABEL_SS = r"SynStream (baseline)"
LABEL_SSIC = r"SynStream (+IC)"

RC = {
    "font.family": "sans-serif",
    "font.size": 13,
    "axes.titlesize": 20,
    "axes.labelsize": 18,
    "xtick.labelsize": 18,
    "ytick.labelsize": 18,
    "legend.fontsize": 18,
    "lines.linewidth": 1.4,
    "lines.markersize": 6.0,
    "axes.linewidth": 0.7,
    "xtick.major.width": 0.7,
    "ytick.major.width": 0.7,
    "grid.linewidth": 0.4,
    "grid.alpha": 0.35,
    "figure.dpi": 200,
}

# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def load_csv(path: Path) -> list[dict]:
    if not path.exists():
        print(f"WARNING: {path} not found")
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def tf_time(
    rows: list[dict], img: int, tile: int, kr: int, workers: int
) -> float | None:
    for r in rows:
        if (
            int(r["image_size"]) == img
            and int(r["tile_size"]) == tile
            and int(r["kernel_radius"]) == kr
            and int(r["threads"]) == workers
        ):
            return float(r["time_ms"])
    return None


def ss_time(
    rows: list[dict],
    img: int,
    tile: int,
    kr: int,
    workers: int,
    st: int,
    ic: bool,
    dep: bool,
) -> float | None:
    ic_s = "_ic" if ic else ""
    dep_s = "_dep" if dep else ""
    label = f"synstream_w{workers}_st{st}{ic_s}{dep_s}"
    for r in rows:
        if (
            r["system"] == label
            and int(r["image_size"]) == img
            and int(r["tile_size"]) == tile
            and int(r["kernel_radius"]) == kr
        ):
            return float(r["time_ms"])
    return None


# ---------------------------------------------------------------------------
# Figure 1: three-panel time vs workers
#   Panel A: 4096×4096, tile=256 (primary comparison, 30-iter values)
#   Panel B: 4096×4096, tile=128 (SynStream advantage regime)
#   Panel C: 8192×8192, tile=256 (scaling, W=2–16 only)
# ---------------------------------------------------------------------------


def plot_comparison(tf_rows: list[dict], ss_rows: list[dict], out_path: Path) -> None:
    kr = 4
    panels = [
        # (img, tile, workers, title_extra)
        (4096, 256, [1, 2, 4, 8, 16], r"$4096{\times}4096$, tile=256  (16$\times$16)"),
        (4096, 128, [1, 2, 4, 8, 16], r"$4096{\times}4096$, tile=128  (32$\times$32)"),
        (8192, 256, [2, 4, 8, 16], r"$8192{\times}8192$, tile=256  (32$\times$32)"),
    ]

    with plt.rc_context(RC):
        fig, axes = plt.subplots(1, 3, figsize=(16, 5.5), sharey=False)

        for ax, (img, tile, workers, title) in zip(axes, panels):
            # Taskflow
            tf_pts = [(w, tf_time(tf_rows, img, tile, kr, w)) for w in workers]
            tf_pts = [(w, v) for w, v in tf_pts if v is not None]
            if tf_pts:
                wx, wy = zip(*tf_pts)
                ax.plot(wx, wy, color=TF_COLOR, marker="o", label=LABEL_TF, zorder=3)

            # SynStream baseline (st=1, no IC/dep)
            ss_pts = [
                (w, ss_time(ss_rows, img, tile, kr, w, 1, False, False))
                for w in workers
            ]
            ss_pts = [(w, v) for w, v in ss_pts if v is not None]
            if ss_pts:
                wx, wy = zip(*ss_pts)
                ax.plot(
                    wx,
                    wy,
                    color=SS_COLOR,
                    marker="s",
                    linestyle="--",
                    label=LABEL_SS,
                    zorder=2,
                    alpha=0.85,
                )

            ax.set_title(title, pad=3)
            ax.set_xscale("log", base=2)
            ax.set_xticks(workers)
            ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
            ax.set_xlabel("Workers")
            ax.grid(True, which="both")
            ax.tick_params(axis="both", which="both", direction="in")

        axes[0].set_ylabel("Wall-clock time (ms)")

        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(
            handles,
            labels,
            loc="lower center",
            ncol=2,
            bbox_to_anchor=(0.5, -0.05),
            frameon=True,
            framealpha=0.9,
        )

        fig.suptitle(
            "Bilateral Denoising: SynStream vs Taskflow  (kernel radius $r{=}4$)",
            fontsize=22,
            y=1.02,
        )
        fig.tight_layout(rect=[0, 0.12, 1, 1])
        fig.savefig(out_path, bbox_inches="tight", dpi=200)
        plt.close(fig)
    print(f"  Saved {out_path}")


# ---------------------------------------------------------------------------
# Figure 2: overhead bar chart (4096/tile=256, all four SS configs)
# ---------------------------------------------------------------------------


def plot_optims(tf_rows: list[dict], ss_rows: list[dict], out_path: Path) -> None:
    img, tile, kr = 4096, 256, 4
    workers = [1, 2, 4, 8, 16]

    configs = [
        ("Baseline", False, False, SS_COLOR),
        ("+IC", True, False, SSIC_COLOR),
        (r"+\$dep", False, True, DEP_COLOR),
        (r"+IC+\$dep", True, True, IC_DEP_CLR),
    ]

    x = np.arange(len(workers))
    n_cfg = len(configs)
    width = 0.16
    offsets = np.linspace(-(n_cfg - 1) / 2, (n_cfg - 1) / 2, n_cfg) * width

    with plt.rc_context(RC):
        fig, ax = plt.subplots(figsize=(12, 5))

        for (cfg_lbl, ic, dep, color), offset in zip(configs, offsets):
            overheads = []
            for w in workers:
                tf = tf_time(tf_rows, img, tile, kr, w)
                ss = ss_time(ss_rows, img, tile, kr, w, 1, ic, dep)
                if tf and ss:
                    overheads.append((ss - tf) / tf * 100.0)
                else:
                    overheads.append(float("nan"))

            valid_x = [x[i] + offset for i, v in enumerate(overheads) if not (v != v)]
            valid_v = [v for v in overheads if v == v]
            bars = ax.bar(
                valid_x,
                valid_v,
                width,
                label=cfg_lbl,
                color=color,
                alpha=0.88,
                zorder=3,
                edgecolor="white",
                linewidth=0.4,
            )
            for bar, val in zip(bars, valid_v):
                ypos = bar.get_height()
                sign = 1 if ypos >= 0 else -1
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    ypos + sign * 0.4,
                    f"{val:+.1f}%",
                    ha="center",
                    va="bottom" if ypos >= 0 else "top",
                    fontsize=11,
                    color="black",
                )

        ax.axhline(0, color="black", linewidth=0.8, zorder=4)
        ax.set_xticks(x)
        ax.set_xticklabels([f"W={w}" for w in workers])
        ax.set_xlabel("Worker count")
        ax.set_ylabel("Overhead vs Taskflow (%)")
        ax.set_title(
            r"SynStream optimisation variants — $4096{\times}4096$, tile=256, $r{=}4$",
            pad=3,
            fontsize=22,
        )
        ax.legend(loc="lower left", ncol=2, framealpha=0.9)
        ax.grid(True, axis="y", alpha=0.35)
        ax.tick_params(axis="both", direction="in")
        fig.tight_layout()
        fig.savefig(out_path, bbox_inches="tight", dpi=200)
        plt.close(fig)
    print(f"  Saved {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--bench-dir", type=Path, default=Path(__file__).resolve().parents[1]
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "paper" / "figs",
    )
    args = p.parse_args()

    tf_csv = args.bench_dir / "taskflow/results/tf_bilateral_combined.csv"
    ss_csv = args.bench_dir / "synstream/results/ss_bilateral_all.csv"

    tf_rows = load_csv(tf_csv)
    ss_rows = load_csv(ss_csv)
    if not tf_rows or not ss_rows:
        return

    args.out_dir.mkdir(parents=True, exist_ok=True)

    plot_comparison(tf_rows, ss_rows, args.out_dir / "bilateral_comparison.png")
    plot_optims(tf_rows, ss_rows, args.out_dir / "bilateral_optims.png")
    print("Done.")


if __name__ == "__main__":
    main()
