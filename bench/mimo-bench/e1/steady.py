#!/usr/bin/env python3
"""Steady-state view of E1 cells: re-score every run excluding the first K
frames (default 200) instead of the campaign's 20-frame warm-up, to separate a
start-up transient from steady-state behaviour. Secondary analysis only: the
primary tables (report.py) keep the protocol's warm-up.

  steady.py <tag> [--skip 200] [--config 16x16] [--slots 16]

Prints, per cell: runs (all runs, pass or not, with their pass flag), the
median across runs of the sender-referenced latency (e2e) mean/p50/p99 and the
tail (done - last packet) mean/p99 over frames >= K. Frames never completed
are counted, not scored.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from e1 import pctl, read_frames  # noqa: E402


def score(run: Path, skip: int) -> dict | None:
    meta = json.loads((run / "meta.json").read_text())
    _, rows = read_frames(run / "frames.csv")
    if not rows or not (run / "tx_result.txt").exists():
        return None
    tx = [float(x) * 1e3 for x in (run / "tx_result.txt").read_text().split()]
    P = meta["frame_duration_us"] * 1e3
    N = meta["frames"]
    done = [r for r in rows if r["done_ns"] > 0 and skip <= r["frame"] < N]
    L = [(r["done_ns"] - (tx[r["frame"]] - P)) / 1e6 for r in done if r["frame"] < len(tx)]
    T = [(r["done_ns"] - r["last_rx_ns"]) / 1e6 for r in done]
    run_json = json.loads((run / "run.json").read_text())
    return {"pass": run_json["pass"], "undone": (N - skip) - len(done),
            "e2e_mean": statistics.fmean(L), "e2e_p50": pctl(L, 50), "e2e_p99": pctl(L, 99),
            "tail_mean": statistics.fmean(T), "tail_p99": pctl(T, 99)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("tag")
    ap.add_argument("--skip", type=int, default=200)
    ap.add_argument("--config")
    ap.add_argument("--slots", type=int)
    a = ap.parse_args()
    print(f"| config | system | S | P (ms) | runs (pass) | e2e mean | p50 | p99 | tail mean / p99 | undone |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for cell in sorted((HERE / "results" / a.tag).glob("*/*/*/cell.json")):
        c = json.loads(cell.read_text())
        if (a.config and c["config"] != a.config) or (a.slots and c["slots"] != a.slots):
            continue
        rs = [s for s in (score(r, a.skip) for r in sorted(cell.parent.glob("rep*"))) if s]
        if not rs:
            continue
        m = lambda k: statistics.median(r[k] for r in rs)  # noqa: E731
        print(f"| {c['config']} | {cell.parent.parent.name} | {c['slots']} | {c['frame_duration_us']/1000:g} | "
              f"{len(rs)} ({sum(r['pass'] for r in rs)}) | {m('e2e_mean'):.3f} | {m('e2e_p50'):.3f} | "
              f"{m('e2e_p99'):.3f} | {m('tail_mean'):.3f} / {m('tail_p99'):.3f} | "
              f"{[r['undone'] for r in rs]} |")


if __name__ == "__main__":
    main()
