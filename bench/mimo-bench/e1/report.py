#!/usr/bin/env python3
"""Aggregate E1 cells (results/<tag>/<config>/<system>/<cell>/cell.json) into
markdown tables for RESULTS.md.

  report.py <tag> [--config 16x16] [--metric e2e|lat] [--by-S]

Every number is the MEDIAN across the passing runs of that cell (EVAL_PROTOCOL
"Statistics"); [min-max] is the across-run range of the per-run mean. A cell
with any failing run (drops, verifier mismatch, dependency violation, stall) is
printed as FAILED with its reasons and is not ranked.
"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"


def load(tag: str) -> list[dict]:
    out = []
    for p in sorted((RESULTS / tag).glob("*/*/*/cell.json")):
        c = json.loads(p.read_text())
        c["_sysdir"] = p.parent.parent.name
        c["_cell"] = p.parent.name
        out.append(c)
    return out


def f(x, nd=3):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{x:.{nd}f}"


def row(c: dict, metric: str) -> str:
    ok = c["reps_pass"] == c["reps"] and c["reps"] > 0
    drops = sum(c["dropped_per_rep"])
    n = c["frames"]
    status = f"{c['reps_pass']}/{c['reps']}"
    if not ok:
        reasons = sorted({r for rs in c["fail_reasons"] for r in rs})
        reasons = "; ".join(reasons)[:120]
        return (f"| {c['_sysdir']} | {c['workers']} | {c['slots']} | {c['frame_duration_us'] / 1000:g} | "
                f"FAILED {status} | - | - | - | - | - | - | {drops} | {reasons} |")
    m = metric
    rng = c.get(f"{m}_mean_range") or [float('nan'), float('nan')]
    return (f"| {c['_sysdir']} | {c['workers']} | {c['slots']} | {c['frame_duration_us'] / 1000:g} | "
            f"{status} | {f(c[f'{m}_mean'])} [{f(rng[0])}-{f(rng[1])}] | {f(c[f'{m}_p50'])} | "
            f"{f(c[f'{m}_p99'])} | {f(c[f'{m}_p999'])} | {f(c['tail_mean'])} / {f(c['tail_p99'])} | "
            f"{f(c['overlap_frac_mean'], 2)} | {drops} | {n - c['warmup']} measured/run |")


HDR = ("| system | W | S | P (ms) | runs ok | {m} mean [range] | p50 | p99 | p99.9 | tail mean / p99 | "
       "FFT overlap | dropped | notes |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("tag")
    ap.add_argument("--config", default=None)
    ap.add_argument("--metric", default="e2e", choices=["e2e", "lat"])
    ap.add_argument("--headline", action="store_true",
                    help="one row per (config, S): '<metric> mean / p99' per system dir")
    ap.add_argument("--systems", default="tomii,tf-orig,tf-dag,tf-async,tbb-dag,tbb-flow",
                    help="system dirs (columns) for --headline")
    a = ap.parse_args()
    cells = load(a.tag)
    if a.headline:
        cols = a.systems.split(",")
        by = {(c["config"], c["slots"], c["_sysdir"]): c for c in cells}
        keys = sorted({(c["config"], c["slots"], c["frame_duration_us"]) for c in cells},
                      key=lambda k: ({"4x4": 0, "16x16": 1, "64x16": 2}.get(k[0], 9), k[1]))
        print("| config | S | P (ms) | " + " | ".join(cols) + " |")
        print("|---|---|---|" + "---|" * len(cols))
        for cfg, s, p in keys:
            out = []
            for col in cols:
                c = by.get((cfg, s, col))
                if c is None:
                    out.append("-")
                elif c["reps_pass"] != c["reps"]:
                    out.append(f"FAIL {c['reps_pass']}/{c['reps']}")
                else:
                    out.append(f"{f(c[a.metric + '_mean'])} / {f(c[a.metric + '_p99'])}")
            print(f"| {cfg} | {s} | {p / 1000:g} | " + " | ".join(out) + " |")
        return
    if a.config:
        cells = [c for c in cells if c["config"] == a.config]
    order = {"tomii": 0, "tf-orig": 1, "tf-dag": 2, "tf-async": 3, "tbb-dag": 4, "tbb-flow": 5}
    cells.sort(key=lambda c: (c["config"], c["slots"], c["frame_duration_us"],
                              order.get(c["system"], 9), c["_sysdir"], c["workers"]))
    cur = None
    for c in cells:
        key = c["config"]
        if key != cur:
            cur = key
            print(f"\n#### {key}\n")
            print(HDR.format(m=a.metric))
        print(row(c, a.metric))


if __name__ == "__main__":
    main()
