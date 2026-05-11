#!/usr/bin/env python3
"""Locked analysis script for agent-eval N=20 results.

Usage:
    python analyze.py --results-dir results/
    python analyze.py --results-dir results/ --q-tomii 5471 --q-taskflow 3785

Loads result.json + result_enriched.json for each trial. Groups by condition
(T-full / T-bare / TF-bare). Reports pre-registered metrics.

Primary metric (pre-registered): tokens_to_q (trajectory-based from enriched).
Secondary: first_action_improvement, build_failures_agent, invalid_tune_rejections.

Condition mapping (from result.json):
    framework=tomii,  skills=full → T-full
    framework=tomii,  skills≠full → T-bare
    framework=taskflow, any        → TF-bare
"""
from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from pathlib import Path

Q_TOMII_US    = 5471.0
Q_TASKFLOW_US = 3785.0


# ── Helpers ────────────────────────────────────────────────────────────────────

def _condition(r: dict) -> str:
    fw = r.get("framework", "unknown")
    sk = r.get("skills", "bare")
    if fw == "tomii" and sk == "full":
        return "T-full"
    if fw == "tomii":
        return "T-bare"
    return "TF-bare"


def bootstrap_ci(values: list[float], n_boot: int = 2000, ci: float = 0.95) -> tuple[float, float]:
    n = len(values)
    if n == 0:
        return float("nan"), float("nan")
    if n == 1:
        return values[0], values[0]
    boot_means = sorted(
        statistics.mean(random.choices(values, k=n)) for _ in range(n_boot)
    )
    lo_idx = int((1 - ci) / 2 * n_boot)
    hi_idx = int((1 + ci) / 2 * n_boot) - 1
    return boot_means[lo_idx], boot_means[hi_idx]


def _mean(vals: list[float]) -> float | None:
    return statistics.mean(vals) if vals else None


def _median(vals: list[float]) -> float | None:
    return statistics.median(vals) if vals else None


def _fmt(v: float | None, digits: int = 1, suffix: str = "") -> str:
    return f"{v:.{digits}f}{suffix}" if v is not None else "—"


# ── Data loading ───────────────────────────────────────────────────────────────

def _load_trial(trial_dir: Path) -> dict | None:
    rj = trial_dir / "result.json"
    if not rj.exists():
        return None
    try:
        r = json.loads(rj.read_text())
    except Exception:
        return None
    r["_condition"] = _condition(r)
    r["_dir"] = str(trial_dir)

    ej = trial_dir / "result_enriched.json"
    if ej.exists():
        try:
            e = json.loads(ej.read_text())
            r["_enriched"] = e
        except Exception:
            r["_enriched"] = {}
    else:
        r["_enriched"] = {}
    return r


def load_results(results_dir: Path) -> list[dict]:
    records: list[dict] = []
    for run_dir in sorted(results_dir.iterdir()):
        if not run_dir.is_dir():
            continue
        # Flat trial_* directories directly under results_dir
        if run_dir.name.startswith("trial_") and (run_dir / "result.json").exists():
            t = _load_trial(run_dir)
            if t:
                records.append(t)
            continue
        # Nested: results_dir/run_name/trial_*/
        for trial_dir in sorted(run_dir.iterdir()):
            if trial_dir.is_dir() and trial_dir.name.startswith("trial_"):
                t = _load_trial(trial_dir)
                if t:
                    records.append(t)
    return records


def group_by_condition(records: list[dict]) -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    for r in records:
        c = r["_condition"]
        groups.setdefault(c, []).append(r)
    return groups


# ── Kaplan-Meier and log-rank ──────────────────────────────────────────────────

def log_rank_test(group_a: list[dict], group_b: list[dict],
                  cap: int = 50_000_000) -> float | None:
    """One-sided log-rank p-value for tokens-to-Q between two groups."""
    try:
        from scipy import stats as _stats
    except ImportError:
        print("[analyze] scipy not installed — log-rank p-value skipped.")
        return None

    def _events(records: list[dict]) -> tuple[list[int], list[int]]:
        durations, flags = [], []
        for r in records:
            tok = _enr(r, "tokens_to_q") or (r.get("tokens_input", 0) + r.get("tokens_output", 0))
            durations.append(tok if tok else cap)
            flags.append(1 if r.get("reached_q") else 0)
        return durations, flags

    da, ea = _events(group_a)
    db, eb = _events(group_b)

    all_times = sorted(set(da + db))
    O_a = E_a = 0.0
    for t in all_times:
        ar_a = sum(1 for d in da if d >= t)
        ar_b = sum(1 for d in db if d >= t)
        n_t = ar_a + ar_b
        if n_t == 0:
            continue
        d_a = sum(1 for d, e in zip(da, ea) if d == t and e)
        d_b = sum(1 for d, e in zip(db, eb) if d == t and e)
        d_t = d_a + d_b
        O_a += d_a
        E_a += d_t * ar_a / n_t
    if E_a == 0:
        return None
    V = E_a * (1 - E_a / max(O_a + sum(eb), 1))
    if V <= 0:
        return None
    chi2 = (O_a - E_a) ** 2 / V
    try:
        return float(_stats.chi2.sf(chi2, df=1))
    except Exception:
        return None


# ── Summary tables ─────────────────────────────────────────────────────────────

def _enr(r: dict, key: str, default=None):
    return r.get("_enriched", {}).get(key, default)


def print_primary_table(groups: dict[str, list[dict]], q_by_cond: dict[str, float]) -> None:
    """Pre-registered primary metric: tokens_to_q (trajectory-based)."""
    print("\n=== Primary: tokens_to_Q (trajectory-based) ===")
    hdr = f"{'Cond':<8} {'N':>3} {'pass%':>6} {'q%':>5} {'tok_to_Q med':>13} {'CI 95%':>18}"
    print(hdr)
    print("-" * len(hdr))
    cond_order = ["T-full", "T-bare", "TF-bare"]
    for cond in cond_order:
        ts = groups.get(cond, [])
        if not ts:
            continue
        n = len(ts)
        pass_rate = 100 * sum(1 for t in ts if t.get("verify_pass") or t.get("passed")) / n
        q_rate    = 100 * sum(1 for t in ts if t.get("reached_q")) / n
        tok_q = [_enr(t, "tokens_to_q") for t in ts if _enr(t, "tokens_to_q") is not None]
        med = _median(tok_q)
        lo, hi = bootstrap_ci(tok_q) if tok_q else (None, None)
        ci_str = f"[{lo:>7.0f}, {hi:>7.0f}]" if lo is not None else "—"
        print(f"{cond:<8} {n:>3} {pass_rate:>5.1f}% {q_rate:>4.1f}%"
              f" {_fmt(med, 0):>13} {ci_str:>18}")


def print_secondary_table(groups: dict[str, list[dict]]) -> None:
    """Secondary metrics from result_enriched.json."""
    print("\n=== Secondary metrics (means) ===")
    hdr = (f"{'Cond':<8} {'N':>3} {'1st_impr%':>10} "
           f"{'build_fail':>11} {'tune_rej':>9} {'opt_iter':>9} {'final_us':>9}")
    print(hdr)
    print("-" * len(hdr))
    cond_order = ["T-full", "T-bare", "TF-bare"]
    for cond in cond_order:
        ts = groups.get(cond, [])
        if not ts:
            continue
        n = len(ts)

        def _em(key: str) -> list[float]:
            return [_enr(t, key) for t in ts if _enr(t, key) is not None]

        fai  = [v * 100 for v in _em("first_action_improvement")]
        bfa  = _em("build_failures_agent")
        rej  = _em("invalid_tune_rejections")
        oi   = _em("optimization_iterations")
        fin  = _em("final_latency_us")

        print(f"{cond:<8} {n:>3}"
              f" {_fmt(_mean(fai), 1, '%'):>10}"
              f" {_fmt(_mean(bfa), 1):>11}"
              f" {_fmt(_mean(rej), 1):>9}"
              f" {_fmt(_mean(oi), 1):>9}"
              f" {_fmt(_median(fin), 0, 'µs'):>9}")


def print_interface_effect(groups: dict[str, list[dict]]) -> None:
    """T-full vs T-bare: isolates interface effect (same framework)."""
    tf = groups.get("T-full", [])
    tb = groups.get("T-bare", [])
    if not tf or not tb:
        return
    print("\n=== Interface effect: T-full vs T-bare (same framework) ===")

    def _em(ts: list[dict], key: str) -> list[float]:
        return [_enr(t, key) for t in ts if _enr(t, key) is not None]

    metrics = [
        ("tokens_to_q",               "tokens_to_Q"),
        ("first_action_improvement",   "1st_action_improve"),
        ("build_failures_agent",       "build_failures_agent"),
        ("invalid_tune_rejections",    "tune_rejections"),
        ("optimization_iterations",    "opt_iterations"),
    ]
    for key, label in metrics:
        vf = _mean(_em(tf, key))
        vb = _mean(_em(tb, key))
        if vf is not None and vb is not None and vb != 0:
            ratio = vf / vb
            print(f"  {label:<25}: T-full={_fmt(vf, 1)} / T-bare={_fmt(vb, 1)} (ratio {ratio:.2f}×)")
        else:
            print(f"  {label:<25}: T-full={_fmt(vf, 1)} / T-bare={_fmt(vb, 1)}")

    p_val = log_rank_test(tf, tb)
    p_str = f"{p_val:.4f}" if p_val is not None else "n/a"
    print(f"  {'log-rank p (tok_to_Q)':<25}: {p_str}")


def print_framework_comparison(groups: dict[str, list[dict]]) -> None:
    """T-full vs TF-bare: cross-framework (familiarity confound present)."""
    tf = groups.get("T-full", [])
    tfc = groups.get("TF-bare", [])
    if not tf or not tfc:
        return
    print("\n=== Cross-framework: T-full vs TF-bare (note: familiarity confound) ===")
    p_val = log_rank_test(tf, tfc)
    p_str = f"{p_val:.4f}" if p_val is not None else "n/a"
    print(f"  log-rank p (tok_to_Q): {p_str}")

    def _em(ts: list[dict], key: str) -> list[float]:
        return [_enr(t, key) for t in ts if _enr(t, key) is not None]

    for key, label in [("first_action_improvement", "1st_action_improve"),
                        ("build_failures_agent",     "build_failures_agent"),
                        ("optimization_iterations",  "opt_iterations")]:
        vf = _mean(_em(tf, key))
        vt = _mean(_em(tfc, key))
        print(f"  {label:<25}: T-full={_fmt(vf, 1)} / TF-bare={_fmt(vt, 1)}")


def print_signal_usage(groups: dict[str, list[dict]]) -> None:
    """Tomii signal usage (report reads, knobs calls, skill invocations)."""
    print("\n=== Signal usage (mean per trial) ===")
    hdr = f"{'Cond':<8} {'N':>3} {'report_reads':>13} {'knobs_calls':>12} {'skill_invocs':>13}"
    print(hdr)
    print("-" * len(hdr))
    cond_order = ["T-full", "T-bare", "TF-bare"]
    for cond in cond_order:
        ts = groups.get(cond, [])
        if not ts:
            continue
        def _sig(key: str) -> float:
            vs = [t.get(key, 0) for t in ts if not t.get("censored")]
            return statistics.mean(vs) if vs else 0.0
        print(f"{cond:<8} {len(ts):>3}"
              f" {_sig('signal_report_reads'):>13.1f}"
              f" {_sig('signal_knobs_calls'):>12.1f}"
              f" {_sig('signal_skill_reads'):>13.1f}")


# ── Main ────────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results-dir", default="results",
                   help="Directory containing run_*/trial_* subdirectories")
    p.add_argument("--q-tomii",    type=float, default=Q_TOMII_US)
    p.add_argument("--q-taskflow", type=float, default=Q_TASKFLOW_US)
    p.add_argument("--output",     default="", help="Write JSON summary to file")
    args = p.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Results dir not found: {results_dir}")
        return

    records = load_results(results_dir)
    if not records:
        print("No trials found.")
        return

    q_by_cond = {
        "T-full":  args.q_tomii,
        "T-bare":  args.q_tomii,
        "TF-bare": args.q_taskflow,
    }

    print(f"Loaded {len(records)} trials from {results_dir}")
    groups = group_by_condition(records)
    for cond, ts in sorted(groups.items()):
        print(f"  {cond}: {len(ts)} trials")

    print_primary_table(groups, q_by_cond)
    print_secondary_table(groups)
    print_interface_effect(groups)
    print_framework_comparison(groups)
    print_signal_usage(groups)

    if args.output:
        summary = {}
        for cond, ts in groups.items():
            def _em(key: str) -> list:
                return [_enr(t, key) for t in ts if _enr(t, key) is not None]
            summary[cond] = {
                "n": len(ts),
                "pass_rate": sum(1 for t in ts if t.get("verify_pass") or t.get("passed")) / len(ts),
                "q_rate": sum(1 for t in ts if t.get("reached_q")) / len(ts),
                "tokens_to_q_values": _em("tokens_to_q"),
                "tokens_to_q_median": _median(_em("tokens_to_q")),
                "first_action_improvement_mean": _mean(_em("first_action_improvement")),
                "build_failures_agent_mean": _mean(_em("build_failures_agent")),
                "invalid_tune_rejections_mean": _mean(_em("invalid_tune_rejections")),
                "optimization_iterations_mean": _mean(_em("optimization_iterations")),
                "final_latency_us_median": _median(_em("final_latency_us")),
                "q_threshold_us": q_by_cond.get(cond),
            }
        Path(args.output).write_text(json.dumps(summary, indent=2))
        print(f"\nJSON summary → {args.output}")


if __name__ == "__main__":
    main()
