"""Arm 2: Bayesian optimisation over the stream-analytics knob space.

Uses Optuna's TPE (Tree-structured Parzen Estimator) sampler.
Requires: pip install optuna
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    print(
        "ERROR: optuna is not installed. Install it with: pip install optuna",
        file=sys.stderr,
    )
    sys.exit(1)

from harness import (  # noqa: E402
    KnobConfig,
    TrialRecord,
    establish_baseline,
    evaluate,
    log_trial,
)


def main() -> None:
    p = argparse.ArgumentParser(
        description="Bayesian (Optuna TPE) search over stream-analytics knobs"
    )
    p.add_argument("--iterations", type=int, default=50)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--streams", type=int, default=500)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.results_dir / "bayesian_trials.jsonl"

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")
    trial_counter = [0]  # mutable reference for the closure

    def objective(trial: optuna.Trial) -> float:
        i = trial_counter[0]
        trial_counter[0] += 1

        knobs = KnobConfig(
            workers=int(trial.suggest_categorical("workers", [1, 2, 4, 8])),
            slots=int(trial.suggest_categorical("slots", [1, 4, 16, 64])),
            inline_continuation=bool(
                trial.suggest_categorical("inline_continuation", [True, False])
            ),
            coalesce_barriers=bool(
                trial.suggest_categorical("coalesce_barriers", [True, False])
            ),
            fifo=bool(trial.suggest_categorical("fifo", [True, False])),
            custom=bool(trial.suggest_categorical("custom", [True, False])),
            no_fanout_bulk=bool(
                trial.suggest_categorical("no_fanout_bulk", [True, False])
            ),
            batching_size=int(
                trial.suggest_categorical("batching_size", [1, 4, 8, 16])
            ),
        )
        result = evaluate(knobs, streams=args.streams, warmup=args.warmup)
        record = TrialRecord(iteration=i, knobs=knobs, result=result, arm="bayesian")
        log_trial(record, log_file)

        if not result.verifier_ok or result.ms_per_stream is None:
            reason = result.rejection_reason or "verifier failed"
            print(f"[bayesian {i}] rejected — {reason}", flush=True)
            raise optuna.TrialPruned()

        nonlocal best_ms
        ms = result.ms_per_stream
        if ms < best_ms:
            best_ms = ms
            if baseline > 0.0:
                delta_pct = (baseline - best_ms) / baseline * 100.0
                print(
                    f"[bayesian {i}] new best: {best_ms:.4f} ms/stream "
                    f"(delta: {delta_pct:.1f}%)",
                    flush=True,
                )
            else:
                print(
                    f"[bayesian {i}] new best: {best_ms:.4f} ms/stream",
                    flush=True,
                )
        return ms

    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=args.seed),
    )
    study.optimize(objective, n_trials=args.iterations)

    if baseline > 0.0 and best_ms < float("inf"):
        improvement = (baseline - best_ms) / baseline * 100.0
        print(
            f"\nBayesian search: baseline={baseline:.4f}, best={best_ms:.4f} ms "
            f"({improvement:.1f}% improvement)"
        )
    else:
        print(f"\nBayesian search complete. best={best_ms:.4f} ms/stream")

    best_trial = study.best_trial if study.trials else None
    if best_trial is not None and best_trial.value is not None:
        print(f"Best params: {best_trial.params}")


if __name__ == "__main__":
    main()
