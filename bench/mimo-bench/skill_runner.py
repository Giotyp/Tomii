#!/usr/bin/env python3
"""
Phase A: structured knob-search following SKILLS/knob-search.md.

Protocol (4 iterations):
  1. Boolean toggles: coalesce_barriers, inline_continuation (both expected OFF for MIMO)
  2. Doubling sweep batching_size (1,2,4,...,512) — pick knee
  3. Sweep batching_limit (1,2,4,10 µs)
  4. 3× verification trials; escalate max_streams if std > 10% mean

Workers, system_threads, and receiver_threads are fixed (not tuned) — set via config.

Exposes run_phase_a(run_benchmark, base_cfg, run_dir, opt) -> (best_config, trial_log)
where run_benchmark(config, run_script, report_path, timeout_s) -> bool.
"""

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


@dataclass
class TrialEntry:
    phase: str
    knob: str
    value: object
    avg_latency_us: float
    delta_pct: float
    error: bool = False


def _run(
    run_benchmark: Callable,
    cfg: dict,
    run_script: str,
    report_path: str,
    timeout_s: int,
) -> float | None:
    ok = run_benchmark(cfg, run_script, report_path, timeout_s)
    if not ok:
        return None
    with open(report_path) as f:
        return json.load(f)["summary"]["avg_latency_us"]


def run_phase_a(
    run_benchmark: Callable,
    base_cfg: dict,
    run_dir: Path,
    opt,
) -> tuple[dict, list[TrialEntry]]:
    """
    Runs the 5-iteration knob-search skill.
    Returns (best_config, trial_log).
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    trial_log: list[TrialEntry] = []
    best_cfg = dict(base_cfg)

    # Load baseline latency by running base_cfg once
    iter_dir = run_dir / "iter0_baseline"
    iter_dir.mkdir(exist_ok=True)
    baseline_latency = _run(
        run_benchmark,
        best_cfg,
        opt.run_script,
        str(iter_dir / "report.json"),
        opt.run_timeout_s,
    )
    if baseline_latency is None:
        print("[WARN] Baseline run failed in Phase A — using inf as baseline")
        baseline_latency = float("inf")
    print(f"[Phase A] Baseline avg_latency_us = {baseline_latency:.1f}")
    best_latency = baseline_latency

    # ── Iter 1: Boolean knobs ──────────────────────────────────────────────────
    print("[Phase A] Iter 1: boolean knobs")
    iter1_dir = run_dir / "iter1_booleans"
    # Note: inline_continuation and coalesce_barriers are documented as OFF for MIMO
    # in tomii-core/src/bin/main.rs. We still verify empirically.
    bool_knobs = [
        ("coalesce_barriers", 1),
        ("inline_continuation", 1),
    ]
    for knob, val in bool_knobs:
        trial_cfg = dict(best_cfg)
        trial_cfg[knob] = val
        d = iter1_dir / knob
        d.mkdir(parents=True, exist_ok=True)
        lat = _run(
            run_benchmark,
            trial_cfg,
            opt.run_script,
            str(d / "report.json"),
            opt.run_timeout_s,
        )
        if lat is None:
            trial_log.append(
                TrialEntry("iter1", knob, val, float("inf"), float("nan"), error=True)
            )
            continue
        delta_pct = (lat - best_latency) / best_latency * 100
        trial_log.append(TrialEntry("iter1", knob, val, lat, delta_pct))
        print(f"  {knob}=1 → {lat:.1f} µs ({delta_pct:+.1f}%)")
        if lat < best_latency * 0.99:  # >1% improvement gate
            best_latency = lat
            best_cfg = dict(trial_cfg)
    (iter1_dir / "config.json").write_text(json.dumps(best_cfg, indent=2))

    # ── Iter 2: Binary-search batching_size ───────────────────────────────────
    print("[Phase A] Iter 2: batching_size sweep")
    iter2_dir = run_dir / "iter2_batching_size"
    iter2_dir.mkdir(exist_ok=True)
    bs_candidates = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]
    bs_results: list[tuple[int, float]] = []
    for bs in bs_candidates:
        trial_cfg = dict(best_cfg)
        trial_cfg["batching_size"] = bs
        lat = _run(
            run_benchmark,
            trial_cfg,
            opt.run_script,
            str(iter2_dir / f"bs_{bs}.json"),
            opt.run_timeout_s,
        ) or float("inf")
        delta_pct = (lat - best_latency) / best_latency * 100
        trial_log.append(TrialEntry("iter2", "batching_size", bs, lat, delta_pct))
        bs_results.append((bs, lat))
        print(f"  batching_size={bs} → {lat:.1f} µs")
    best_bs, best_bs_lat = min(bs_results, key=lambda x: x[1])
    if best_bs_lat < best_latency * 0.99:
        best_latency = best_bs_lat
        best_cfg["batching_size"] = best_bs
    (iter2_dir / "config.json").write_text(json.dumps(best_cfg, indent=2))

    # ── Iter 3: Sweep batching_limit ──────────────────────────────────────────
    print("[Phase A] Iter 3: batching_limit sweep")
    iter3_dir = run_dir / "iter3_batching_limit"
    iter3_dir.mkdir(exist_ok=True)
    bl_candidates = [1, 2, 4, 10]
    bl_results: list[tuple[int, float]] = []
    for bl in bl_candidates:
        trial_cfg = dict(best_cfg)
        trial_cfg["batching_limit"] = bl
        lat = _run(
            run_benchmark,
            trial_cfg,
            opt.run_script,
            str(iter3_dir / f"bl_{bl}.json"),
            opt.run_timeout_s,
        ) or float("inf")
        delta_pct = (lat - best_latency) / best_latency * 100
        trial_log.append(TrialEntry("iter3", "batching_limit", bl, lat, delta_pct))
        bl_results.append((bl, lat))
        print(f"  batching_limit={bl} → {lat:.1f} µs")
    best_bl, best_bl_lat = min(bl_results, key=lambda x: x[1])
    if best_bl_lat < best_latency * 0.99:
        best_latency = best_bl_lat
        best_cfg["batching_limit"] = best_bl
    (iter3_dir / "config.json").write_text(json.dumps(best_cfg, indent=2))

    # ── Iter 4: Verify best config (3 trials) ─────────────────────────────────
    print("[Phase A] Iter 4: verification (3 trials)")
    iter4_dir = run_dir / "iter4_verify"
    iter4_dir.mkdir(exist_ok=True)
    verify_cfg = dict(best_cfg)
    verify_streams = opt.max_streams
    for attempt in range(2):  # escalate streams once if variance too high
        lats = []
        for t in range(3):
            trial_cfg = dict(verify_cfg)
            lat = _run(
                run_benchmark,
                trial_cfg,
                opt.run_script,
                str(iter4_dir / f"trial_{t}_streams{verify_streams}.json"),
                opt.run_timeout_s * 2,
            )
            if lat is not None:
                lats.append(lat)
        if not lats:
            break
        mean_lat = statistics.mean(lats)
        std_lat = statistics.stdev(lats) if len(lats) > 1 else 0.0
        print(
            f"  verify: mean={mean_lat:.1f} std={std_lat:.1f} (streams={verify_streams})"
        )
        if std_lat <= 0.1 * mean_lat or verify_streams >= 50:
            if mean_lat < best_latency:
                best_latency = mean_lat
            break
        verify_streams = max(50, verify_streams)
        print(
            f"  high variance ({std_lat:.1f} > 10% of mean) — escalating to {verify_streams} streams"
        )

    (iter4_dir / "config.json").write_text(json.dumps(best_cfg, indent=2))
    (run_dir / "best_config.json").write_text(json.dumps(best_cfg, indent=2))

    trial_log_data = [
        {
            "phase": e.phase,
            "knob": e.knob,
            "value": e.value,
            "avg_latency_us": e.avg_latency_us,
            "delta_pct": e.delta_pct,
            "error": e.error,
        }
        for e in trial_log
    ]
    (run_dir / "trial_log.json").write_text(json.dumps(trial_log_data, indent=2))

    print(f"[Phase A] Done. Best config: {best_cfg}  latency={best_latency:.1f} µs")
    return best_cfg, trial_log
