#!/usr/bin/env python3
"""
MIMO Agent Optimization Harness — two-phase (Phase A: SKILLS knob-search + Phase B: LLM loop)

Phase A: structured deterministic sweep following SKILLS/knob-search.md (~5 benchmark runs).
Phase B: LLM-guided loop seeded from Phase A's best config, with per-iteration diagnose gating.

Usage:
    python harness.py [OPTIONS]

    --reference-report PATH   Path to baseline report.json (default: from config.py)
    --llm-iters N             LLM iterations for Phase B (default: 15)
    --verify-trials N         Verification trials at end (default: 3)
    --output-dir DIR          Where to store per-iteration results
    --model MODEL             Claude model for Phase B (default: claude-opus-4-6)
    --budget USD              Per-LLM-call budget in USD (default: 1.00)
    --workload LABEL          Workload label appended to output dir (e.g. 16x16)
    --skip-phase-a            Skip SKILLS sweep; start Phase B from default config
    --phase-a-only            Run Phase A only (useful for smoke testing)
"""

import argparse
import json
import os
import select
import shutil
import signal
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from config import MimoOptConfig
from diagnose import diagnose
from skill_runner import run_phase_a

SCRIPT_DIR = Path(__file__).parent
STATIC_PROMPT_PATH = SCRIPT_DIR / "prompts" / "optimize_mimo.md"


# ── Report parsing ────────────────────────────────────────────────────────────


def load_report(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def extract_key_metrics(report: dict) -> dict:
    summary = report.get("summary", {})
    diag = summary.get("scheduling_overhead_diagnostic", {})
    cp = report.get("critical_path", {})
    util = report.get("resource_utilization", {})
    return {
        "avg_latency_us": summary.get("avg_latency_us"),
        "p99_latency_us": summary.get("p99_latency_us"),
        "total_streams": summary.get("total_streams"),
        "overhead_pct": diag.get("overhead_pct"),
        "critical_path_us": cp.get("estimated_latency_us"),
        "worker_busy_pct": util.get("worker_busy_pct", []),
    }


# ── Prompt building ───────────────────────────────────────────────────────────


def build_prompt(
    static_prompt: str,
    reference_report: dict,
    current_report: dict | None,
    history: list[dict],
    current_config: dict,
    diag: dict | None = None,
) -> str:
    parts = [static_prompt.rstrip(), ""]

    ref_metrics = extract_key_metrics(reference_report)
    parts.append("## Reference Baseline")
    parts.append(f"```json\n{json.dumps(ref_metrics, indent=2)}\n```")
    parts.append("")

    if current_report is not None:
        parts.append("## Current Performance Report")
        parts.append(f"```json\n{json.dumps(current_report, indent=2)}\n```")
        parts.append("")
    else:
        parts.append("## Current Performance Report (= Reference Baseline)")
        parts.append(f"```json\n{json.dumps(reference_report, indent=2)}\n```")
        parts.append("")

    if diag is not None:
        parts.append("## Bottleneck Diagnosis")
        parts.append(f"Bottleneck class: **{diag['bottleneck_class']}**")
        parts.append(f"- overhead_pct={diag['overhead_pct']:.1f}%")
        parts.append(
            f"- worker_busy: min={diag['worker_busy_min']:.1f}% max={diag['worker_busy_max']:.1f}% spread={diag['worker_busy_spread']:.1f}pp"
        )
        parts.append(
            f"- critical_path: {diag['critical_path_nodes']} nodes, {diag['critical_path_latency_us']:.1f}µs"
        )
        parts.append("Actions suggested:")
        for a in diag["actions"]:
            parts.append(f"  - {a}")
        parts.append("")

    parts.append("## Current Knob Configuration")
    parts.append(f"```json\n{json.dumps(current_config, indent=2)}\n```")
    parts.append("")

    if history:
        parts.append("## Iteration History (most recent last)")
        for i, h in enumerate(history):
            parts.append(
                f"  Iter {i}: config={h['config']}  "
                f"avg_latency_us={h['avg_latency_us']:.1f}"
            )
        parts.append("")

    parts.append(
        "Based on the above, output ONLY the JSON object with the next "
        "knob configuration to try."
    )
    return "\n".join(parts)


# ── Claude invocation ─────────────────────────────────────────────────────────


def invoke_claude(prompt: str, model: str, budget_usd: float) -> str:
    cmd = ["claude", "-p", "--model", model, "--max-budget-usd", str(budget_usd)]
    result = subprocess.run(
        cmd, input=prompt, capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        print(f"[WARN] claude exited {result.returncode}: {result.stderr[:200]}")
    return result.stdout


def extract_json_config(text: str) -> dict | None:
    """Brace-balanced JSON extractor — handles nested objects unlike the old regex."""
    depth = 0
    start = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(text[start : i + 1])
                except json.JSONDecodeError:
                    start = None
    return None


def validate_config(cfg: dict, opt_config: MimoOptConfig) -> dict:
    ranges = opt_config.KNOB_RANGES
    validated = {}
    for knob, meta in ranges.items():
        val = cfg.get(knob, getattr(opt_config, knob))
        if meta["type"] == "bool":
            val = 1 if int(val) else 0
        else:
            val = max(meta["min"], min(meta["max"], int(val)))
        validated[knob] = val
    return validated


# ── Benchmark execution ───────────────────────────────────────────────────────


def _kill_port(port: int) -> None:
    """Kill any process holding the given UDP port to prevent EADDRINUSE."""
    try:
        result = subprocess.run(
            ["fuser", f"{port}/udp"],
            capture_output=True,
            text=True,
        )
        pids = result.stdout.split()
        for pid in pids:
            try:
                subprocess.run(["kill", "-9", pid], check=False)
            except Exception:
                pass
        if pids:
            time.sleep(0.5)  # brief wait for the kernel to release the port
    except FileNotFoundError:
        pass  # fuser not available


def run_benchmark(
    config: dict,
    run_script: str,
    report_path: str,
    timeout_s: int,
) -> bool:
    _kill_port(8000)
    env = os.environ.copy()
    env["MIMO_CLEANUP"] = "0"
    env["MIMO_SKIP_VIZ"] = "1"
    env["MIMO_REPORT_FILE"] = report_path
    env["MIMO_WORKERS"] = str(config.get("workers", 26))
    env["MIMO_SYSTEM_THREADS"] = str(config.get("system_threads", 8))
    env["MIMO_RECEIVER_THREADS"] = str(config.get("receiver_threads", 4))
    env["MIMO_BATCHING_SIZE"] = str(config.get("batching_size", 32))
    env["MIMO_BATCHING_LIMIT"] = str(config.get("batching_limit", 10))
    env["MIMO_SCHED_FLUSH_THRESHOLD"] = str(config.get("sched_flush_threshold", 32))
    env["MIMO_SPIN_ITERATIONS"] = str(config.get("spin_iterations", 32))
    env["MIMO_SPIN_WAIT_SPIN_ITERS"] = str(config.get("spin_wait_spin_iters", 64))
    env["MIMO_SPIN_WAIT_YIELD_ITERS"] = str(config.get("spin_wait_yield_iters", 256))
    env["MIMO_SPIN_WAIT_PARK_NS"] = str(config.get("spin_wait_park_ns", 100))

    proc = subprocess.Popen(
        ["bash", run_script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )

    completed = False
    deadline = time.monotonic() + timeout_s
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                print("[WARN] Benchmark timed out")
                break
            ready, _, _ = select.select([proc.stdout], [], [], min(remaining, 2.0))
            if not ready:
                continue
            line = proc.stdout.readline()
            if not line:
                break
            sys.stdout.write(f"  [run] {line}")
            sys.stdout.flush()
            if "RUN COMPLETED" in line:
                completed = True
                break
    finally:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait()

    return completed and os.path.exists(report_path)


# ── Verify phase ──────────────────────────────────────────────────────────────


def run_verify(
    best_cfg: dict, opt: MimoOptConfig, verify_dir: Path, n_trials: int
) -> dict:
    verify_dir.mkdir(parents=True, exist_ok=True)
    lats = []
    for t in range(n_trials):
        rpath = str(verify_dir / f"trial_{t}" / "report.json")
        Path(rpath).parent.mkdir(parents=True, exist_ok=True)
        ok = run_benchmark(best_cfg, opt.run_script, rpath, opt.run_timeout_s * 2)
        if ok:
            rep = load_report(rpath)
            lats.append(rep["summary"]["avg_latency_us"])
    if not lats:
        return {"trials": n_trials, "success": 0, "mean": None, "std": None}
    mean_lat = statistics.mean(lats)
    std_lat = statistics.stdev(lats) if len(lats) > 1 else 0.0
    stats = {
        "trials": n_trials,
        "success": len(lats),
        "mean": mean_lat,
        "std": std_lat,
        "values": lats,
    }
    (verify_dir / "stats.json").write_text(json.dumps(stats, indent=2))
    print(
        f"[Verify] mean={mean_lat:.1f} µs  std={std_lat:.1f} µs  ({len(lats)}/{n_trials} succeeded)"
    )
    return stats


# ── Main ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="MIMO two-phase optimization harness")
    parser.add_argument("--reference-report", default="")
    parser.add_argument("--llm-iters", type=int, default=15)
    parser.add_argument("--verify-trials", type=int, default=3)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--budget", type=float, default=0.0)
    parser.add_argument("--workload", default="", help="Workload label, e.g. 16x16")
    parser.add_argument("--skip-phase-a", action="store_true")
    parser.add_argument("--phase-a-only", action="store_true")
    args = parser.parse_args()

    opt = MimoOptConfig()
    if args.reference_report:
        opt.reference_report = args.reference_report
    if args.output_dir:
        opt.output_dir = args.output_dir
    if args.model:
        opt.model = args.model
    if args.budget:
        opt.budget_usd = args.budget

    if not os.path.exists(opt.reference_report):
        print(f"[ERROR] Reference report not found: {opt.reference_report}")
        sys.exit(1)

    reference_report = load_report(opt.reference_report)
    ref_latency = reference_report["summary"]["avg_latency_us"]
    print(f"[INFO] Reference baseline avg_latency_us = {ref_latency:.1f} µs")

    workload_label = args.workload or "default"
    run_label = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    output_dir = Path(opt.output_dir) / f"run_{workload_label}" / run_label
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(opt.reference_report, output_dir / "reference_report.json")
    print(f"[INFO] Results → {output_dir}")

    static_prompt = STATIC_PROMPT_PATH.read_text()

    default_config = {
        "workers": opt.workers,
        "system_threads": opt.system_threads,
        "receiver_threads": opt.receiver_threads,
        "batching_size": opt.batching_size,
        "batching_limit": opt.batching_limit,
        "sched_flush_threshold": opt.sched_flush_threshold,
        "spin_iterations": opt.spin_iterations,
        "spin_wait_spin_iters": opt.spin_wait_spin_iters,
        "spin_wait_yield_iters": opt.spin_wait_yield_iters,
        "spin_wait_park_ns": opt.spin_wait_park_ns,
    }

    # ── Phase A: structured knob-search ──────────────────────────────────────
    phase_a_best = dict(default_config)
    if not args.skip_phase_a:
        print(f"\n{'=' * 60}")
        print("[Phase A] Starting structured knob-search (SKILLS/knob-search.md)")
        phase_a_dir = output_dir / "phase_a"

        # Adapt run_benchmark signature for skill_runner (script path baked in opt)
        def _run_bench(cfg, script, report_path, timeout_s):
            return run_benchmark(cfg, script, report_path, timeout_s)

        phase_a_best, _ = run_phase_a(_run_bench, default_config, phase_a_dir, opt)
        print(f"[Phase A] Best config: {phase_a_best}")
    else:
        print("[INFO] Skipping Phase A")

    if args.phase_a_only:
        print("[INFO] --phase-a-only set, stopping after Phase A.")
        return

    # ── Phase B: LLM-guided loop ──────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"[Phase B] Starting LLM loop ({args.llm_iters} iters, model={opt.model})")

    phase_b_dir = output_dir / "phase_b"
    phase_b_dir.mkdir(exist_ok=True)

    current_config = dict(phase_a_best)
    current_report: dict | None = None
    history: list[dict] = []
    best_config = dict(current_config)
    best_latency = ref_latency
    consecutive_high_overhead = 0

    for iteration in range(args.llm_iters):
        print(f"\n{'=' * 60}")
        print(f"[Phase B] Iteration {iteration + 1} / {args.llm_iters}")

        iter_dir = phase_b_dir / f"iter_{iteration}"
        iter_dir.mkdir(exist_ok=True)

        # Diagnose current state before asking Claude
        report_for_diag = current_report or reference_report
        tmp = iter_dir / "_diag_input.json"
        tmp.write_text(json.dumps(report_for_diag))
        diag = diagnose(str(tmp))
        (iter_dir / "diagnosis.json").write_text(json.dumps(diag, indent=2))
        print(
            f"[Phase B] Bottleneck: {diag['bottleneck_class']}  overhead={diag['overhead_pct']:.1f}%"
        )

        if diag["overhead_pct"] > 60.0:
            consecutive_high_overhead += 1
        else:
            consecutive_high_overhead = 0

        if consecutive_high_overhead >= 2:
            print(
                "[Phase B] overhead_pct > 60% for 2 consecutive iters — writing needs_coarsen.json"
            )
            (output_dir / "needs_coarsen.json").write_text(
                json.dumps(
                    {
                        "reason": "overhead_pct > 60% for 2 consecutive Phase B iterations",
                        "last_overhead_pct": diag["overhead_pct"],
                        "last_config": current_config,
                        "action": "Run SKILLS/graph-coarsen.md on examples/mimolib/graphs/graph_per_symbol.json",
                    },
                    indent=2,
                )
            )
            print("[Phase B] Exiting early — graph-coarsen required.")
            break

        prompt = build_prompt(
            static_prompt,
            reference_report,
            current_report,
            history,
            current_config,
            diag,
        )
        (iter_dir / "prompt.md").write_text(prompt)

        print("[Phase B] Invoking Claude...")
        raw_response = invoke_claude(prompt, opt.model, opt.budget_usd)
        (iter_dir / "response.txt").write_text(raw_response)

        next_cfg = extract_json_config(raw_response)
        if next_cfg is None:
            print(f"[WARN] Could not parse JSON config:\n{raw_response[:300]}")
            continue

        next_cfg = validate_config(next_cfg, opt)
        print(f"[Phase B] Claude suggests: {next_cfg}")
        (iter_dir / "config.json").write_text(json.dumps(next_cfg, indent=2))

        report_path = str(iter_dir / "report.json")
        print("[Phase B] Running benchmark...")
        success = run_benchmark(
            next_cfg, opt.run_script, report_path, opt.run_timeout_s
        )

        if not success:
            print("[WARN] Benchmark did not complete")
            history.append(
                {"config": next_cfg, "avg_latency_us": float("inf"), "error": True}
            )
            continue

        iter_report = load_report(report_path)
        iter_latency = iter_report["summary"]["avg_latency_us"]
        iter_p99 = iter_report["summary"].get("p99_latency_us", 0)
        overhead = (
            iter_report["summary"]
            .get("scheduling_overhead_diagnostic", {})
            .get("overhead_pct", 0)
        )

        print(
            f"[Phase B] avg={iter_latency:.1f} µs  p99={iter_p99:.1f} µs  overhead={overhead:.1f}%"
        )
        print(
            f"  vs baseline: {ref_latency:.1f} µs  "
            f"({'↓' if iter_latency < ref_latency else '↑'}"
            f" {abs(iter_latency - ref_latency) / ref_latency * 100:.1f}%)"
        )

        history.append(
            {
                "config": next_cfg,
                "avg_latency_us": iter_latency,
                "p99_latency_us": iter_p99,
                "overhead_pct": overhead,
            }
        )

        if iter_latency < best_latency:
            best_latency = iter_latency
            best_config = dict(next_cfg)
            print(f"[Phase B] *** New best: {iter_latency:.1f} µs ***")

        current_config = next_cfg
        current_report = iter_report

    # ── Verify ────────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"[Verify] Running {args.verify_trials} verification trials on best config")
    verify_stats = run_verify(
        best_config, opt, output_dir / "verify", args.verify_trials
    )

    # Load prior-round best for comparison (best-effort: most recent summary.json)
    prior_best = None
    prior_results = SCRIPT_DIR / "results"
    if prior_results.exists():
        summaries = sorted(
            prior_results.glob("**/summary.json"), key=lambda p: p.stat().st_mtime
        )
        if summaries:
            try:
                prior_best = json.loads(summaries[-1].read_text()).get(
                    "best_latency_us"
                )
            except Exception:
                pass

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 60}")
    print(f"[DONE] Optimization complete")
    print(f"  Baseline:        {ref_latency:.1f} µs")
    print(f"  Phase B best:    {best_latency:.1f} µs")
    if best_latency < ref_latency:
        print(f"  Improvement:     {ref_latency / best_latency:.2f}×")
    if prior_best:
        delta = (best_latency - prior_best) / prior_best * 100
        print(f"  vs prior round:  {prior_best:.1f} µs → {delta:+.1f}%")
    print(f"  Best config:     {best_config}")

    phase_a_best_path = output_dir / "phase_a" / "best_config.json"
    phase_a_final = (
        json.loads(phase_a_best_path.read_text())
        if phase_a_best_path.exists()
        else phase_a_best
    )

    summary = {
        "workload": workload_label,
        "reference_latency_us": ref_latency,
        "phase_a_best_config": phase_a_final,
        "phase_b_best_latency_us": best_latency,
        "phase_b_best_config": best_config,
        "improvement_ratio": ref_latency / best_latency if best_latency > 0 else None,
        "prior_round_best_latency_us": prior_best,
        "verify_mean_us": verify_stats.get("mean"),
        "verify_std_us": verify_stats.get("std"),
        "phase_b_iterations": history,
        "model": opt.model,
        "timestamp": run_label,
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"  Summary →        {summary_path}")


if __name__ == "__main__":
    main()
