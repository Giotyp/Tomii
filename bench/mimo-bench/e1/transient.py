#!/usr/bin/env python3
"""S=16 start-up transient summary per cell (P0 validation, RESULTS.md).

  transient.py <cell_dir> [<cell_dir> ...] [--first 200]

Per run, over frames [warm-up, first): number of frames whose post-arrival tail
(done - last packet) exceeds 20 ms, the max tail, the last frame with such a
tail, and frames not completed in the whole run. Prints the median across runs
plus the per-run lists.
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from e1 import read_frames  # noqa: E402


def one(run: Path, first: int) -> dict | None:
    meta = json.loads((run / "meta.json").read_text())
    _, rows = read_frames(run / "frames.csv")
    if not rows:
        return None
    K, N = meta["warmup"], meta["frames"]
    done = {r["frame"]: r for r in rows if r["done_ns"] > 0}
    early = [done[f] for f in range(K, min(first, N)) if f in done]
    tails = [((r["done_ns"] - r["last_rx_ns"]) / 1e6, r["frame"]) for r in early if r["last_rx_ns"]]
    slow = [(t, f) for t, f in tails if t > 20]
    return {"slow": len(slow), "max_tail": max((t for t, _ in tails), default=0.0),
            "last_slow": max((f for _, f in slow), default=-1), "undone": N - len(done)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cells", nargs="+", type=Path)
    ap.add_argument("--first", type=int, default=200)
    a = ap.parse_args()
    print("| cell | runs | frames with tail > 20 ms (median; per run) | max tail ms (median) | last such frame (median) | undone per run |")
    print("|---|---|---|---|---|---|")
    for c in a.cells:
        rs = [x for x in (one(r, a.first) for r in sorted(c.glob("rep*"), key=lambda p: int(p.name[3:]))) if x]
        if not rs:
            continue
        med = lambda k: statistics.median(r[k] for r in rs)  # noqa: E731
        print(f"| {c} | {len(rs)} | {med('slow'):g}; {[r['slow'] for r in rs]} | {med('max_tail'):.1f} | "
              f"{med('last_slow'):g} | {[r['undone'] for r in rs]} |")


if __name__ == "__main__":
    main()
