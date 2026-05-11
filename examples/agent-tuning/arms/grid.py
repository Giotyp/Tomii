"""Arm 3: grid search over the stream-analytics knob space.

Iterates a bounded cross-product of all knob values. The full grid is large
(4 × 4 × 2 × 2 × 2 × 2 × 2 × 4 = 2048 cells); this arm caps at --iterations
(default 50) and documents the gap.
"""

from __future__ import annotations

import argparse
import itertools
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import (  # noqa: E402
    KnobConfig,
    TrialRecord,
    establish_baseline,
    evaluate,
    log_trial,
)

WORKERS_OPTS: list[int] = [1, 2, 4, 8]
SLOTS_OPTS: list[int] = [1, 4, 16, 64]
BOOL_OPTS: list[bool] = [True, False]
BATCHING_OPTS: list[int] = [1, 4, 8, 16]


def _grid_cells() -> list[KnobConfig]:
    """Return all cells in the full cross-product grid."""
    cells = []
    for (
        workers,
        slots,
        inline_continuation,
        coalesce_barriers,
        fifo,
        custom,
        no_fanout_bulk,
        batching_size,
    ) in itertools.product(
        WORKERS_OPTS,
        SLOTS_OPTS,
        BOOL_OPTS,
        BOOL_OPTS,
        BOOL_OPTS,
        BOOL_OPTS,
        BOOL_OPTS,
        BATCHING_OPTS,
    ):
        cells.append(
            KnobConfig(
                workers=workers,
                slots=slots,
                inline_continuation=inline_continuation,
                coalesce_barriers=coalesce_barriers,
                fifo=fifo,
                custom=custom,
                no_fanout_bulk=no_fanout_bulk,
                batching_size=batching_size,
            )
        )
    return cells


def main() -> None:
    p = argparse.ArgumentParser(
        description="grid search over stream-analytics knobs (budget-capped)"
    )
    p.add_argument(
        "--iterations",
        type=int,
        default=50,
        help="maximum grid cells to evaluate (full grid = 2048)",
    )
    p.add_argument("--streams", type=int, default=500)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.results_dir / "grid_trials.jsonl"

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")

    all_cells = _grid_cells()
    total_grid = len(all_cells)
    budget = min(args.iterations, total_grid)
    cells = all_cells[:budget]

    print(
        f"[grid] full grid = {total_grid} cells; evaluating {budget} cells "
        f"({budget / total_grid * 100:.1f}% coverage)",
        flush=True,
    )

    for i, knobs in enumerate(cells):
        result = evaluate(knobs, streams=args.streams, warmup=args.warmup)
        record = TrialRecord(iteration=i, knobs=knobs, result=result, arm="grid")
        log_trial(record, log_file)

        if result.verifier_ok and result.ms_per_stream is not None:
            if result.ms_per_stream < best_ms:
                best_ms = result.ms_per_stream
                if baseline > 0.0:
                    delta_pct = (baseline - best_ms) / baseline * 100.0
                    print(
                        f"[grid {i}] new best: {best_ms:.4f} ms/stream "
                        f"(delta: {delta_pct:.1f}%)",
                        flush=True,
                    )
                else:
                    print(
                        f"[grid {i}] new best: {best_ms:.4f} ms/stream",
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
            f"\nGrid search complete. best={best_ms:.4f} ms/stream "
            f"— {budget}/{total_grid} cells evaluated"
        )


if __name__ == "__main__":
    main()
