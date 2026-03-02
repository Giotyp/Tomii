"""Convert SynStream STREAM timing .txt files to comparison CSV format.

Reads timing files produced by run_bench.py (synstream_stream_<kernel>_w<N>.txt)
and writes one CSV row per file to benchmarks/results/ with columns:
  system, kernel, array_size, workers, elapsed_s, gb_s

The elapsed_s is taken from the kernel task's "Avg/Stream" timing (not including
array generation), matching what Timely's stream_bench times.

Usage:
    # Convert all existing results
    python benchmarks/parse_synstream_stream.py

    # Custom input/output dirs
    python benchmarks/parse_synstream_stream.py \\
        --input-dir examples/stream-bench/results \\
        --output-dir benchmarks/results \\
        --array-size 268435456
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path

# Kernel → task node name in the graph
_TASK_NAME = {
    "copy":  "copy_op",
    "scale": "scale_op",
    "add":   "add_op",
    "triad": "triad_op",
}

# Number of arrays accessed per element (reads + writes)
_ARRAYS = {
    "copy":  2,  # read B, write A
    "scale": 2,  # read B, write A
    "add":   3,  # read B+C, write A
    "triad": 3,  # read B+C, write A
}

_TIME_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)\s*(ns|µs|us|ms|s)\b")


def _parse_seconds(token: str) -> float:
    """Parse a duration token like '3.9328s', '983.2ms', '636ns' to seconds."""
    m = _TIME_RE.fullmatch(token.strip().rstrip(",;"))
    if not m:
        raise ValueError(f"Unrecognised time token: {token!r}")
    value, unit = float(m.group(1)), m.group(2)
    return value * {"ns": 1e-9, "µs": 1e-6, "us": 1e-6, "ms": 1e-3, "s": 1.0}[unit]


_STEADY_STATE_RE = re.compile(r"Steady-state:\s*(\d+)\s*streams?")
_MIN_STEADY_STATE = 5


def check_sufficient_streams(txt: str, path: Path) -> None:
    """Raise ValueError if fewer than _MIN_STEADY_STATE steady-state streams were recorded."""
    m = _STEADY_STATE_RE.search(txt)
    if m and int(m.group(1)) < _MIN_STEADY_STATE:
        raise ValueError(
            f"only {m.group(1)} steady-state streams (need {_MIN_STEADY_STATE}); "
            "re-run with --max-streams >= 8"
        )


def extract_kernel_avg_task(txt: str, task_name: str) -> float:
    """Return Avg/Task (seconds) for the named task from a timing .txt file.

    Avg/Task is the mean wall-clock time per individual task instance.  With
    factor=N parallel instances running on N workers, Avg/Task ≈ the parallel
    wall-clock duration of one batch, so total GB/s = N*bytes / Avg/Task.
    (Avg/Stream is the SUM of all N instance times — a CPU-time metric, not
    wall-clock — and must not be used for bandwidth calculation.)
    """
    # Timing line format:
    #   Timing - Avg/Stream: 3.9328s, Avg/Task: 983.2026ms, Min: ...
    header_pat = re.compile(
        rf"Task\s+'{re.escape(task_name)}'[^\n]*\n"
        rf"\s+Timing\s+-\s+Avg/Stream:\s*\S+\s*,\s*Avg/Task:\s*(\S+)",
        re.MULTILINE,
    )
    m = header_pat.search(txt)
    if not m:
        raise ValueError(f"Task '{task_name}' not found in timing output")
    return _parse_seconds(m.group(1))


def convert_file(
    txt_path: Path,
    output_dir: Path,
    array_size: int,
    system: str = "synstream",
) -> None:
    """Parse one SynStream timing .txt and append a row to the output CSV."""
    # Expected filename: synstream_stream_<kernel>_w<workers>.txt
    stem = txt_path.stem  # e.g. synstream_stream_triad_w4
    parts = stem.split("_")
    # parts = ["synstream", "stream", <kernel>, "w<N>"]
    if len(parts) != 4 or parts[0] != "synstream" or parts[1] != "stream":
        print(f"[skip] Unexpected filename: {txt_path.name}", file=sys.stderr)
        return
    kernel = parts[2]
    if kernel not in _TASK_NAME:
        print(f"[skip] Unknown kernel '{kernel}': {txt_path.name}", file=sys.stderr)
        return
    try:
        workers = int(parts[3].lstrip("w"))
    except ValueError:
        print(f"[skip] Cannot parse workers from '{parts[3]}': {txt_path.name}", file=sys.stderr)
        return

    txt = txt_path.read_text(encoding="utf-8")
    task_name = _TASK_NAME[kernel]
    try:
        check_sufficient_streams(txt, txt_path)
        elapsed_s = extract_kernel_avg_task(txt, task_name)
    except ValueError as e:
        print(f"[skip] {txt_path.name}: {e}", file=sys.stderr)
        return

    bytes_total = workers * _ARRAYS[kernel] * array_size * 8
    gb_s = bytes_total / elapsed_s / 1e9

    out_csv = output_dir / f"{system}_stream_{kernel}_w{workers}.csv"
    needs_header = not out_csv.exists()
    with out_csv.open("a", newline="") as f:
        w = csv.writer(f)
        if needs_header:
            w.writerow(["system", "kernel", "array_size", "workers", "elapsed_s", "gb_s"])
        w.writerow([system, kernel, array_size, workers, f"{elapsed_s:.6f}", f"{gb_s:.3f}"])

    print(
        f"  {txt_path.name} → {out_csv.name}  "
        f"({elapsed_s:.4f}s  {gb_s:.2f} GB/s)"
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Convert SynStream timing .txt to comparison CSV")
    p.add_argument(
        "--input-dir",
        type=Path,
        default=Path("examples/stream-bench/results"),
        help="directory containing synstream_stream_*.txt files",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("benchmarks/results"),
        help="directory to write per-run CSV files",
    )
    p.add_argument(
        "--array-size",
        type=int,
        default=268_435_456,
        help="f64 elements per worker array (default: 256M)",
    )
    p.add_argument(
        "--system",
        default="synstream",
        help="system label for output CSV rows (default: synstream)",
    )
    args = p.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    txt_files = sorted(args.input_dir.glob("synstream_stream_*.txt"))
    if not txt_files:
        print(f"No synstream_stream_*.txt files found in {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Converting {len(txt_files)} file(s) from {args.input_dir} → {args.output_dir}")
    for f in txt_files:
        convert_file(f, args.output_dir, args.array_size, args.system)


if __name__ == "__main__":
    main()
