#!/usr/bin/env python3
"""Verifier for task_1: sensor pipeline.

Usage (from workspace root):
    python verifier.py [--streams N] [--exclude N]

Prints exactly one of:
    PASS
    FAIL: <reason>

And optionally (when PASS):
    latency_us=NNN.N
"""

from __future__ import annotations
import argparse
import json
import os
import re
import sys

GOLDEN_PER_STREAM = (
    "Sensor-0: [41.50, 42.00, 42.50]\n"
    "Sensor-1: [41.50, 42.00, 42.50]\n"
    "Sensor-2: [41.50, 42.00, 42.50]\n"
    "Sensor-3: [41.50, 42.00, 42.50]\n"
)

GOLDEN_LINES = [l for l in GOLDEN_PER_STREAM.strip().splitlines() if l]
LATENCY_FLOOR_US = 200.0  # physical floor: even 1 calibration call ≈ 223µs
MIN_TASKS_PER_STREAM = 15  # collapsed graph (9 tasks) vs any real graph (≥25)
MIN_GENERATE_PER_STREAM = (
    256  # must equal total_readings; any tile coarsening reduces this below 256
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--streams", type=int, default=5, help="Total streams run (including warm-up)"
    )
    p.add_argument(
        "--exclude",
        type=int,
        default=2,
        help="Warm-up streams excluded from output file",
    )
    p.add_argument("--result-file", default="result.txt")
    return p.parse_args()


def _fail(reason: str) -> None:
    print(f"FAIL: {reason}")
    sys.exit(0)  # exit 0 so harness parses output, not exit code


def _extract_latency() -> float | None:
    for path in ("report.json", "report.txt"):
        if os.path.exists(path):
            try:
                data = json.loads(open(path).read())
                lat = data.get("summary", {}).get("avg_latency_us")
                if lat is not None:
                    return float(lat)
            except Exception:
                pass
    for path in ("timing.txt",):
        if os.path.exists(path):
            for line in open(path).read().splitlines():
                m = re.search(r"avg_latency_us\s*=\s*([\d.]+)", line)
                if m:
                    return float(m.group(1))
    return None


def _extract_tasks() -> int | None:
    for path in ("report.json",):
        if os.path.exists(path):
            try:
                data = json.loads(open(path).read())
                v = data.get("summary", {}).get("total_tasks_per_stream")
                if v is not None:
                    return int(v)
            except Exception:
                pass
    return None


def _extract_generate_per_stream() -> int | None:
    timing_path = "timing.txt"
    if not os.path.exists(timing_path):
        return None
    text = open(timing_path).read()
    m_ss = re.search(r"Steady-state:\s*(\d+)\s*streams?", text)
    if not m_ss:
        return None
    steady_state = int(m_ss.group(1))
    if steady_state == 0:
        return None
    m_gen = re.search(r"Task 'generate'[^\n]*Total Executions:\s*(\d+)", text)
    if not m_gen:
        return None
    return int(m_gen.group(1)) // steady_state


def main() -> None:
    args = _parse_args()
    # Both frameworks write ALL streams (including warm-up) to result.txt;
    # exclude_streams only affects which streams contribute to latency timing.
    expected_lines = args.streams * len(GOLDEN_LINES)

    if not os.path.exists(args.result_file):
        _fail("result.txt not found — did the implementation write output?")

    with open(args.result_file) as f:
        content = f.read()

    lines = [l for l in content.splitlines() if l.strip()]

    if len(lines) == 0:
        _fail("result.txt is empty")

    if len(lines) != expected_lines:
        _fail(
            f"expected {expected_lines} lines "
            f"({args.streams} streams × {len(GOLDEN_LINES)} lines/stream), "
            f"got {len(lines)}"
        )

    for i, line in enumerate(lines):
        expected = GOLDEN_LINES[i % len(GOLDEN_LINES)]
        if line.strip() != expected:
            _fail(f"line {i}: expected '{expected}', got '{line.strip()}'")

    # Require timing evidence — block hardcoded-output degenerate trials
    timing_files = ("report.json", "report.txt", "timing.txt")
    if not any(os.path.exists(p) for p in timing_files):
        _fail(
            "no timing file found (report.json / timing.txt) "
            "— did the pipeline actually run?"
        )

    # Physical floor check — detect degenerate fast solutions
    latency_us = _extract_latency()
    if latency_us is not None and latency_us < LATENCY_FLOOR_US:
        _fail(
            f"latency_us={latency_us:.2f} is below physical floor {LATENCY_FLOOR_US}µs "
            f"— likely removed calibration load (each generate/amplify call ~223µs)"
        )

    # Minimum task count — detect collapsed graphs (removed compute nodes)
    tasks = _extract_tasks()
    if tasks is not None and tasks < MIN_TASKS_PER_STREAM:
        _fail(
            f"total_tasks_per_stream={tasks} < {MIN_TASKS_PER_STREAM} "
            f"— graph was collapsed; must keep generate/classify nodes"
        )

    # Minimum generate executions per stream — detect reduced problem size
    gen_per = _extract_generate_per_stream()
    if gen_per is not None and gen_per < MIN_GENERATE_PER_STREAM:
        _fail(
            f"generate executions/stream={gen_per} < {MIN_GENERATE_PER_STREAM} "
            f"— total_readings reduced below minimum; keep factor≥8 for generate node"
        )

    print("PASS")
    if latency_us is not None:
        print(f"latency_us={latency_us:.1f}")


if __name__ == "__main__":
    main()
