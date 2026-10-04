"""Arm 3: strided grid search over the generated knob space.

The full cross-product grid (every knob's materialised domain) is far larger
than any trial budget, so a lexicographic prefix only ever varies the last
few knobs and — as the pre-E8 study showed — can spend the whole budget in
one infeasible corner (e.g. workers=1, receiver_threads=0).  This arm instead
visits cells at a fixed stride through the grid's mixed-radix index:

    index_i = (offset + i * stride) mod N,   stride ~ N / budget, gcd(stride, N) = 1

so the budget is spread over the whole grid, every knob changes between
consecutive cells, and no cell repeats.  `offset` is drawn from the seed, so
seeds give different (equally spread) grids.
"""

from __future__ import annotations

import argparse
import math
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import (  # noqa: E402
    TrialRecord,
    add_common_args,
    establish_baseline,
    log_trial,
    run_trial,
    setup_arm,
    write_run_meta,
)

from tomii import knobs as tomii_knobs  # noqa: E402


def strided_cells(space: dict, budget: int, seed: int) -> list[dict]:
    names = [k["name"] for k in space["knobs"]]
    domains = [tomii_knobs.enumerate_domain(k["domain"]) for k in space["knobs"]]
    n = 1
    for d in domains:
        n *= len(d)
    budget = min(budget, n)
    stride = max(1, n // budget)
    while math.gcd(stride, n) != 1:
        stride += 1
    offset = random.Random(seed).randrange(n)
    cells = []
    for i in range(budget):
        idx = (offset + i * stride) % n
        cell = {}
        # mixed-radix decode, first knob most significant (as itertools.product)
        for name, dom in zip(reversed(names), reversed(domains)):
            idx, r = divmod(idx, len(dom))
            cell[name] = dom[r]
        cells.append({name: cell[name] for name in names})
    return cells


def main() -> None:
    p = argparse.ArgumentParser(description="strided grid search over the knob space")
    add_common_args(p)
    args = p.parse_args()

    t_start = time.monotonic()
    workload, space, results_dir = setup_arm(args)
    log_file = results_dir / "grid_trials.jsonl"

    baseline = establish_baseline(
        frames=args.frames,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    best_ms = float("inf")

    total_grid = tomii_knobs.grid_size(space)
    cells = strided_cells(space, args.iterations, args.seed)
    print(
        f"[grid] workload={workload.name} knob space v{space['version']}: "
        f"full grid = {total_grid} cells; evaluating {len(cells)} strided cells, "
        f"seed={args.seed}",
        flush=True,
    )

    for i, knobs in enumerate(cells):
        result = run_trial(workload, knobs, args, space)
        log_trial(
            TrialRecord(
                iteration=i, knobs=knobs, result=result, arm="grid",
                extra={"seed": args.seed},
            ),
            log_file,
        )
        if result.verifier_ok and result.ms_per_frame is not None:
            if result.ms_per_frame < best_ms:
                best_ms = result.ms_per_frame
                print(f"[grid {i}] new best: {best_ms:.4f} ms/frame", flush=True)
        else:
            print(f"[grid {i}] rejected — {result.rejection_reason}", flush=True)

    write_run_meta(
        results_dir, "grid", args, space, t_start,
        baseline_ms=baseline, best_ms=best_ms if best_ms < float("inf") else None,
        grid_size=total_grid,
    )
    print(f"\n[grid] done: best={best_ms:.4f} ms (baseline {baseline:.4f})")


if __name__ == "__main__":
    main()
