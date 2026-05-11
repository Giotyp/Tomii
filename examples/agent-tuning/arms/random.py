"""Arm 1: random search over the stream-analytics knob space."""

from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

# Allow importing harness from the parent directory
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
BATCHING_OPTS: list[int] = [1, 4, 8, 16]


def main() -> None:
    p = argparse.ArgumentParser(description="random search over stream-analytics knobs")
    p.add_argument("--iterations", type=int, default=50)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--streams", type=int, default=500)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    args = p.parse_args()

    rng = random.Random(args.seed)
    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.results_dir / "random_trials.jsonl"

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")

    for i in range(args.iterations):
        knobs = KnobConfig(
            workers=rng.choice(WORKERS_OPTS),
            slots=rng.choice(SLOTS_OPTS),
            inline_continuation=rng.choice([True, False]),
            coalesce_barriers=rng.choice([True, False]),
            fifo=rng.choice([True, False]),
            custom=rng.choice([True, False]),
            no_fanout_bulk=rng.choice([True, False]),
            batching_size=rng.choice(BATCHING_OPTS),
        )
        result = evaluate(knobs, streams=args.streams, warmup=args.warmup)
        record = TrialRecord(iteration=i, knobs=knobs, result=result, arm="random")
        log_trial(record, log_file)

        if result.verifier_ok and result.ms_per_stream is not None:
            if result.ms_per_stream < best_ms:
                best_ms = result.ms_per_stream
                if baseline > 0.0:
                    delta_pct = (baseline - best_ms) / baseline * 100.0
                    print(
                        f"[random {i}] new best: {best_ms:.4f} ms/stream "
                        f"(delta: {delta_pct:.1f}%)",
                        flush=True,
                    )
                else:
                    print(
                        f"[random {i}] new best: {best_ms:.4f} ms/stream",
                        flush=True,
                    )
        else:
            reason = result.rejection_reason or "verifier failed"
            print(f"[random {i}] rejected — {reason}", flush=True)

    if baseline > 0.0 and best_ms < float("inf"):
        improvement = (baseline - best_ms) / baseline * 100.0
        print(
            f"\nRandom search: baseline={baseline:.4f}, best={best_ms:.4f} ms "
            f"({improvement:.1f}% improvement)"
        )
    else:
        print(f"\nRandom search complete. best={best_ms:.4f} ms/stream")


if __name__ == "__main__":
    main()
