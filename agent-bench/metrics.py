"""Metric collection and aggregation for the agent benchmark."""
from __future__ import annotations
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional


def parse_claude_response(response_json: str) -> Dict[str, Any]:
    """Parse `claude -p --output-format json` output."""
    try:
        data = json.loads(response_json)
    except json.JSONDecodeError:
        return {"error": "invalid_json", "raw": response_json[:200]}

    # Extract token usage — field names may vary between claude versions
    usage = data.get("usage", data.get("token_usage", {}))
    input_tokens  = usage.get("input_tokens",  usage.get("prompt_tokens", 0))
    output_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))

    # Count internal build attempts (Bash tool calls with "cargo build" or "make")
    content = json.dumps(data)
    internal_iters = len(re.findall(r'"cargo build|make\b', content))

    return {
        "input_tokens":      input_tokens,
        "output_tokens":     output_tokens,
        "total_tokens":      input_tokens + output_tokens,
        "internal_iters":    internal_iters,
        "result_type":       data.get("result", data.get("type", "unknown")),
    }


def parse_synstream_report(report_path: Path) -> Dict[str, Any]:
    """Parse a SynStream JSON report file."""
    if not report_path.exists():
        return {"error": "report_missing"}
    try:
        data = json.loads(report_path.read_text())
    except json.JSONDecodeError:
        return {"error": "report_invalid_json"}

    summary = data.get("summary", {})
    return {
        "avg_latency_us":   summary.get("avg_latency_us"),
        "p99_latency_us":   summary.get("p99_latency_us"),
        "min_latency_us":   summary.get("min_latency_us"),
        "total_streams":    summary.get("total_streams"),
        "worker_busy_pct":  summary.get("worker_busy_pct"),
        "bottleneck_hints": data.get("bottleneck_hints", []),
        "critical_path":    data.get("critical_path", []),
    }


def parse_taskflow_csv(csv_path: Path) -> Dict[str, Any]:
    """Parse a Taskflow output CSV."""
    if not csv_path.exists():
        return {"error": "csv_missing"}
    try:
        lines = csv_path.read_text().strip().splitlines()
        if len(lines) < 2:
            return {"error": "csv_empty"}
        header = lines[0].split(",")
        vals   = lines[-1].split(",")
        row    = dict(zip(header, vals))
        s_per_iter = float(row.get("s_per_iter", 0))
        return {
            "avg_latency_us": s_per_iter * 1e6,
            "total_s":        float(row.get("total_s", 0)),
            "iterations":     int(row.get("iterations", 0)),
        }
    except Exception as e:
        return {"error": str(e)}


def verify_synstream_correctness(iter_dir: Path) -> bool:
    """Check if the SynStream run printed PASS."""
    run_log = iter_dir / "run.log"
    if run_log.exists():
        content = run_log.read_text()
        if "PASS" in content:
            return True
        if "FAIL" in content:
            return False
    # Fallback: report.json exists and is non-empty
    report = iter_dir / "report.json"
    return report.exists() and report.stat().st_size > 10


def verify_taskflow_correctness(iter_dir: Path) -> bool:
    """Check if the Taskflow run printed PASS."""
    run_log = iter_dir / "run.log"
    if run_log.exists():
        content = run_log.read_text()
        if "PASS" in content:
            return True
        if "FAIL" in content:
            return False
    # Fallback: report.json exists and is non-empty
    report = iter_dir / "report.json"
    return report.exists() and report.stat().st_size > 10


def collect_iteration_metrics(
    iter_dir: Path,
    framework: str,
    build_exit: int,
    run_exit: int,
    wall_time_s: float,
    correct: bool,
) -> Dict[str, Any]:
    """Collect all metrics for one harness iteration."""
    m: Dict[str, Any] = {
        "build_success":  build_exit == 0,
        "run_success":    run_exit == 0,
        "correct":        correct,
        "wall_time_s":    round(wall_time_s, 3),
    }

    # Parse Claude response
    resp_file = iter_dir / "response.json"
    if resp_file.exists():
        m.update(parse_claude_response(resp_file.read_text()))

    # Parse performance — both frameworks now write report.json
    perf = parse_synstream_report(iter_dir / "report.json")
    m.update(perf)

    return m


def aggregate_trial_metrics(iter_metrics: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate per-iteration metrics into per-trial summary."""
    total_tokens      = sum(m.get("total_tokens", 0)  for m in iter_metrics)
    total_wall_time   = sum(m.get("wall_time_s", 0)   for m in iter_metrics)
    compile_errors    = sum(0 if m.get("build_success") else 1 for m in iter_metrics)
    correct_iters     = [i for i, m in enumerate(iter_metrics) if m.get("correct")]
    first_correct     = correct_iters[0] if correct_iters else None

    latencies = [
        m["avg_latency_us"]
        for m in iter_metrics
        if m.get("correct") and m.get("avg_latency_us") is not None
    ]
    best_latency = min(latencies) if latencies else None
    naive_latency = latencies[0] if latencies else None
    improvement = (naive_latency / best_latency) if (naive_latency and best_latency) else None

    return {
        "iterations_to_correct": first_correct,
        "compile_errors_total":  compile_errors,
        "best_latency_us":       best_latency,
        "improvement_ratio":     improvement,
        "total_tokens":          total_tokens,
        "total_wall_time_s":     round(total_wall_time, 1),
    }
