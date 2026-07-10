"""Arm 3: grid search over the generated knob space.

Iterates a bounded prefix of the full cross-product grid of the space from
`tomii.knob_space` (via harness.load_knob_space).  The full grid is large;
this arm caps at --iterations (default 50) and documents the gap.
"""

from __future__ import annotations

import argparse
import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import (  # noqa: E402
    TrialRecord,
    add_common_args,
    establish_baseline,
    log_trial,
    setup_arm,
)

from tomii import knobs as tomii_knobs  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(
        description="grid search over the knob space (budget-capped)"
    )
    add_common_args(p)
    args = p.parse_args()

    workload, space, results_dir = setup_arm(args)
    log_file = results_dir / "grid_trials.jsonl"

    baseline = establish_baseline(
        frames=args.frames,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")

    total_grid = tomii_knobs.grid_size(space)
    budget = min(args.iterations, total_grid)
    cells = list(itertools.islice(tomii_knobs.grid_cells(space), budget))

    print(
        f"[grid] workload={workload.name} full grid = {total_grid} cells; "
        f"evaluating {budget} cells ({budget / total_grid * 100:.1f}% coverage)",
        flush=True,
    )

    for i, knobs in enumerate(cells):
        result = workload.evaluate(
            knobs, frames=args.frames, warmup=args.warmup, space=space
        )
        record = TrialRecord(iteration=i, knobs=knobs, result=result, arm="grid")
        log_trial(record, log_file)

        if result.verifier_ok and result.ms_per_frame is not None:
            if result.ms_per_frame < best_ms:
                best_ms = result.ms_per_frame
                if baseline > 0.0:
                    delta_pct = (baseline - best_ms) / baseline * 100.0
                    print(
                        f"[grid {i}] new best: {best_ms:.4f} ms/frame "
                        f"(delta: {delta_pct:.1f}%)",
                        flush=True,
                    )
                else:
                    print(
                        f"[grid {i}] new best: {best_ms:.4f} ms/frame",
                        flush=True,
                    )
        else:
            reason = result.rejection_reason or "verifier failed"
            print(f"[grid {i}] rejected — {reason}", flush=True)

    if baseline > 0.0 and best_ms < float("inf"):
        improvement = (baseline - best_ms) / baseline * 100.0
        print(
            f"\nGrid search: baseline={baseline:.4f}, best={best_ms:.4f} ms "
            f"({improvement:.1f}% improvement) — {budget}/{total_grid} cells evaluated"
        )
    else:
        print(
            f"\nGrid search complete. best={best_ms:.4f} ms/frame "
            f"— {budget}/{total_grid} cells evaluated"
        )


if __name__ == "__main__":
    main()
