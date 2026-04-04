#!/usr/bin/env python3
"""
MIMO Agent Optimization Harness

Runs an iterative agent loop to minimize avg end-to-end latency for the
examples/mimolib benchmark by tuning runtime knobs via Claude.

Usage:
    python harness.py [OPTIONS]

    --reference-report PATH   Path to report.json from the reference run
                              (default: from config.py)
    --max-iterations N        Max optimization iterations (default: 10)
    --output-dir DIR          Where to store per-iteration results
    --model MODEL             Claude model to use (default: claude-opus-4-6)
    --budget USD              Per-iteration Claude budget in USD (default: 1.00)
"""

import argparse
import json
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from config import MimoOptConfig

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
) -> str:
    parts = [static_prompt.rstrip(), ""]

    ref_metrics = extract_key_metrics(reference_report)
    parts.append("## Reference Baseline")
    parts.append(f"```json\n{json.dumps(ref_metrics, indent=2)}\n```")
    parts.append("")

    if current_report is not None:
        cur_metrics = extract_key_metrics(current_report)
        parts.append("## Current Performance Report")
        parts.append(f"```json\n{json.dumps(current_report, indent=2)}\n```")
        parts.append("")
    else:
        # First iteration: use reference as the current report
        current_report = reference_report
        parts.append("## Current Performance Report (= Reference Baseline)")
        parts.append(f"```json\n{json.dumps(reference_report, indent=2)}\n```")
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
    """
    Invoke `claude -p` with the prompt on stdin to get a response.
    Returns raw stdout text.
    """
    cmd = [
        "claude",
        "-p",
        "--model", model,
        "--max-budget-usd", str(budget_usd),
    ]
    result = subprocess.run(cmd, input=prompt, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"[WARN] claude exited {result.returncode}: {result.stderr[:200]}")
    return result.stdout


def extract_json_config(text: str) -> dict | None:
    """Extract the first JSON object from Claude's response."""
    # Try to find a {...} block (possibly across multiple lines)
    match = re.search(r'\{[^{}]*\}', text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group())
    except json.JSONDecodeError:
        return None


def validate_config(cfg: dict, opt_config: MimoOptConfig) -> dict:
    """Clamp all knob values to their allowed ranges."""
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

def run_benchmark(
    config: dict,
    run_script: str,
    report_path: str,
    timeout_s: int,
) -> bool:
    """
    Run run_mimo.sh with the given knob config.
    Streams stdout line-by-line; returns True when 'RUN COMPLETED' is seen.
    """
    env = os.environ.copy()
    env["MIMO_CLEANUP"] = "0"
    env["MIMO_SKIP_VIZ"] = "1"
    env["MIMO_REPORT_FILE"] = report_path
    env["MIMO_BATCHING_SIZE"] = str(config["batching_size"])
    env["MIMO_BATCHING_LIMIT"] = str(config["batching_limit"])
    env["MIMO_SCHED_FLUSH_THRESHOLD"] = str(config["sched_flush_threshold"])
    env["MIMO_SPIN_ITERATIONS"] = str(config["spin_iterations"])
    env["MIMO_SPIN_WAIT_SPIN_ITERS"] = str(config["spin_wait_spin_iters"])
    env["MIMO_SPIN_WAIT_YIELD_ITERS"] = str(config["spin_wait_yield_iters"])
    env["MIMO_SPIN_WAIT_PARK_NS"] = str(config["spin_wait_park_ns"])

    proc = subprocess.Popen(
        ["bash", run_script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,  # new process group so we can kill the whole tree
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
                continue  # no data yet; loop back to check deadline
            line = proc.stdout.readline()
            if not line:  # EOF — process exited
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


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MIMO agent optimization harness")
    parser.add_argument("--reference-report", default="")
    parser.add_argument("--max-iterations", type=int, default=0)
    parser.add_argument("--output-dir", default="")
    parser.add_argument("--model", default="")
    parser.add_argument("--budget", type=float, default=0.0)
    args = parser.parse_args()

    opt = MimoOptConfig()
    if args.reference_report:
        opt.reference_report = args.reference_report
    if args.max_iterations:
        opt.max_iterations = args.max_iterations
    if args.output_dir:
        opt.output_dir = args.output_dir
    if args.model:
        opt.model = args.model
    if args.budget:
        opt.budget_usd = args.budget

    # ── Load reference ────────────────────────────────────────────────────────
    if not os.path.exists(opt.reference_report):
        print(f"[ERROR] Reference report not found: {opt.reference_report}")
        print("  Run examples/mimolib/scripts/run_mimo.sh first to generate it.")
        sys.exit(1)

    reference_report = load_report(opt.reference_report)
    ref_latency = reference_report["summary"]["avg_latency_us"]
    print(f"[INFO] Reference baseline avg_latency_us = {ref_latency:.1f} µs")

    # ── Output directory ──────────────────────────────────────────────────────
    run_label = datetime.now().strftime("run_%Y%m%d_%H%M%S")
    output_dir = Path(opt.output_dir) / run_label
    output_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(opt.reference_report, output_dir / "reference_report.json")
    print(f"[INFO] Results → {output_dir}")

    static_prompt = STATIC_PROMPT_PATH.read_text()

    # ── Tracking state ────────────────────────────────────────────────────────
    current_config = {
        "batching_size":         opt.batching_size,
        "batching_limit":        opt.batching_limit,
        "sched_flush_threshold": opt.sched_flush_threshold,
        "spin_iterations":       opt.spin_iterations,
        "spin_wait_spin_iters":  opt.spin_wait_spin_iters,
        "spin_wait_yield_iters": opt.spin_wait_yield_iters,
        "spin_wait_park_ns":     opt.spin_wait_park_ns,
    }
    current_report: dict | None = None
    history: list[dict] = []
    best_config = dict(current_config)
    best_latency = ref_latency

    # ── Optimization loop ─────────────────────────────────────────────────────
    for iteration in range(opt.max_iterations):
        print(f"\n{'='*60}")
        print(f"[INFO] Iteration {iteration + 1} / {opt.max_iterations}")
        print(f"[INFO] Current config: {current_config}")

        iter_dir = output_dir / f"iter_{iteration}"
        iter_dir.mkdir(exist_ok=True)

        # Ask Claude for next config
        prompt = build_prompt(
            static_prompt, reference_report, current_report, history, current_config
        )
        (iter_dir / "prompt.md").write_text(prompt)

        print("[INFO] Invoking Claude...")
        raw_response = invoke_claude(prompt, opt.model, opt.budget_usd)
        (iter_dir / "response.txt").write_text(raw_response)

        next_cfg = extract_json_config(raw_response)
        if next_cfg is None:
            print(f"[WARN] Could not parse JSON config from response:\n{raw_response[:300]}")
            print("[WARN] Skipping iteration")
            continue

        next_cfg = validate_config(next_cfg, opt)
        print(f"[INFO] Claude suggests: {next_cfg}")
        (iter_dir / "config.json").write_text(json.dumps(next_cfg, indent=2))

        # Run benchmark
        report_path = str(iter_dir / "report.json")
        print("[INFO] Running benchmark (MIMO_CLEANUP=0, MIMO_SKIP_VIZ=1)...")
        success = run_benchmark(next_cfg, opt.run_script, report_path, opt.run_timeout_s)

        if not success:
            print("[WARN] Benchmark did not complete or report.json not found")
            history.append({"config": next_cfg, "avg_latency_us": float("inf"), "error": True})
            continue

        iter_report = load_report(report_path)
        iter_latency = iter_report["summary"]["avg_latency_us"]
        iter_p99 = iter_report["summary"].get("p99_latency_us", 0)
        overhead = iter_report["summary"].get("scheduling_overhead_diagnostic", {}).get("overhead_pct", 0)

        print(f"[INFO] avg_latency_us = {iter_latency:.1f} µs  "
              f"p99 = {iter_p99:.1f} µs  overhead = {overhead:.1f}%")
        print(f"[INFO] vs baseline: {ref_latency:.1f} µs  "
              f"({'↓' if iter_latency < ref_latency else '↑'} "
              f"{abs(iter_latency - ref_latency) / ref_latency * 100:.1f}%)")

        history.append({
            "config": next_cfg,
            "avg_latency_us": iter_latency,
            "p99_latency_us": iter_p99,
            "overhead_pct": overhead,
        })

        if iter_latency < best_latency:
            best_latency = iter_latency
            best_config = dict(next_cfg)
            print(f"[INFO] *** New best: {iter_latency:.1f} µs ***")

        current_config = next_cfg
        current_report = iter_report

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"[DONE] Optimization complete after {len(history)} iterations")
    print(f"  Baseline:     {ref_latency:.1f} µs")
    print(f"  Best:         {best_latency:.1f} µs")
    if best_latency < ref_latency:
        print(f"  Improvement:  {ref_latency / best_latency:.2f}×")
    print(f"  Best config:  {best_config}")

    summary = {
        "reference_latency_us": ref_latency,
        "best_latency_us": best_latency,
        "improvement_ratio": ref_latency / best_latency if best_latency > 0 else None,
        "best_config": best_config,
        "iterations": history,
        "model": opt.model,
        "timestamp": run_label,
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"  Summary →     {summary_path}")


if __name__ == "__main__":
    main()
