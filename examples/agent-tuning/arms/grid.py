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
    establish_baseline,
    evaluate,
    load_knob_space,
    log_trial,
)

from tomii import knobs as tomii_knobs  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(
        description="grid search over the knob space (budget-capped)"
    )
    p.add_argument(
        "--iterations",
        type=int,
        default=50,
        help="maximum grid cells to evaluate",
    )
    p.add_argument("--streams", type=int, default=500)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    p.add_argument(
        "--no-graph-knobs",
        action="store_true",
        help="restrict the search to runtime (CLI) knobs",
    )
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.results_dir / "grid_trials.jsonl"

    space = load_knob_space()
    if args.no_graph_knobs:
        space = {**space, "knobs": [k for k in space["knobs"] if k["kind"] == "cli"]}

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")

    total_grid = tomii_knobs.grid_size(space)
    budget = min(args.iterations, total_grid)
    cells = list(itertools.islice(tomii_knobs.grid_cells(space), budget))

    print(
        f"[grid] full grid = {total_grid} cells; evaluating {budget} cells "
        f"({budget / total_grid * 100:.1f}% coverage)",
        flush=True,
    )

    for i, knobs in enumerate(cells):
        result = evaluate(knobs, streams=args.streams, warmup=args.warmup, space=space)
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
