"""Arm 2: Bayesian optimisation over the generated knob space.

Uses Optuna's TPE (Tree-structured Parzen Estimator) sampler; the search
space comes entirely from `tomii.knob_space` via harness.load_knob_space —
no hand-written option lists.
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
    TrialRecord,
    add_common_args,
    establish_baseline,
    log_trial,
    setup_arm,
)

from tomii import knobs as tomii_knobs  # noqa: E402


def main() -> None:
    p = argparse.ArgumentParser(
        description="Bayesian (Optuna TPE) search over the knob space"
    )
    add_common_args(p)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    workload, space, results_dir = setup_arm(args)
    log_file = results_dir / "bayesian_trials.jsonl"
    print(
        f"[bayesian] workload={workload.name} knob space: {len(space['knobs'])} knobs",
        flush=True,
    )

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")
    trial_counter = [0]  # mutable reference for the closure

    def objective(trial: optuna.Trial) -> float:
        i = trial_counter[0]
        trial_counter[0] += 1

        knobs = tomii_knobs.suggest_optuna(space, trial)
        result = workload.evaluate(
            knobs, streams=args.streams, warmup=args.warmup, space=space
        )
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
