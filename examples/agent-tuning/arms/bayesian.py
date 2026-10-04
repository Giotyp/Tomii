"""Arm 2: Bayesian optimisation (Optuna TPE) over the generated knob space.

The search space comes entirely from `tomii.knob_space` — every knob is a
categorical over its materialised domain, so TPE sees exactly the points the
random and grid arms see.

Rejected trials (verifier / soundness gate failed, crash, timeout) are NOT
pruned: TPE receives a penalty objective so it learns to avoid infeasible
regions (the pre-E8 arm pruned them, i.e. gave TPE no signal at all).
Penalty = PENALTY_FACTOR x max(worst feasible latency seen so far, default
configuration latency) — always worse than any feasible point seen.

Requires: pip install optuna
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import optuna

    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    print("ERROR: optuna is not installed (pip install optuna)", file=sys.stderr)
    sys.exit(1)

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

PENALTY_FACTOR = 2.0


def main() -> None:
    p = argparse.ArgumentParser(description="Bayesian (Optuna TPE) knob search")
    add_common_args(p)
    args = p.parse_args()

    t_start = time.monotonic()
    workload, space, results_dir = setup_arm(args)
    log_file = results_dir / "bayesian_trials.jsonl"
    print(
        f"[bayesian] workload={workload.name} knob space v{space['version']}: "
        f"{len(space['knobs'])} knobs, seed={args.seed}",
        flush=True,
    )

    baseline = establish_baseline(
        frames=args.frames,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    state = {"best": float("inf"), "worst": 0.0, "i": 0}

    def objective(trial: optuna.Trial) -> float:
        i = state["i"]
        state["i"] += 1
        knobs = tomii_knobs.suggest_optuna(space, trial)
        result = run_trial(workload, knobs, args, space)

        if result.verifier_ok and result.ms_per_frame is not None:
            ms = result.ms_per_frame
            state["worst"] = max(state["worst"], ms)
            value, penalised = ms, False
            if ms < state["best"]:
                state["best"] = ms
                print(f"[bayesian {i}] new best: {ms:.4f} ms/frame", flush=True)
        else:
            value = PENALTY_FACTOR * max(state["worst"], baseline, 1e-3)
            penalised = True
            print(
                f"[bayesian {i}] rejected — {result.rejection_reason} "
                f"(penalty {value:.4f})",
                flush=True,
            )
        log_trial(
            TrialRecord(
                iteration=i, knobs=knobs, result=result, arm="bayesian",
                extra={"seed": args.seed, "tpe_value": value, "penalised": penalised},
            ),
            log_file,
        )
        return value

    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=args.seed),
    )
    study.optimize(objective, n_trials=args.iterations)

    best = state["best"]
    write_run_meta(
        results_dir, "bayesian", args, space, t_start,
        baseline_ms=baseline, best_ms=best if best < float("inf") else None,
        penalty_factor=PENALTY_FACTOR,
    )
    print(f"\n[bayesian] done: best={best:.4f} ms (baseline {baseline:.4f})")


if __name__ == "__main__":
    main()
