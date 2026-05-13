"""Agent-tuning harness for stream-analytics.

Shared evaluation infrastructure used by all four search arms.

Usage (standalone):
    python harness.py --help
    python harness.py --results-dir results/baseline_run
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
STREAM_ANALYTICS = REPO_ROOT / "examples" / "stream-analytics"
VERIFY_PY = STREAM_ANALYTICS / "verify.py"
_HERE = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class KnobConfig:
    workers: int = 4
    slots: int = 4
    inline_continuation: bool = True
    coalesce_barriers: bool = True
    fifo: bool = False
    custom: bool = True
    no_fanout_bulk: bool = False
    batching_size: int = 1


@dataclass
class EvalResult:
    verifier_ok: bool
    ms_per_stream: float | None  # None if verifier failed or timing unavailable
    rejection_reason: str | None
    wall_seconds: float


@dataclass
class TrialRecord:
    iteration: int
    knobs: KnobConfig
    result: EvalResult
    arm: str
    notes: str = ""


# ---------------------------------------------------------------------------
# Build helpers
# ---------------------------------------------------------------------------


def _dylib_path() -> Path:
    """Return the expected release dylib path for stream-analytics."""
    return REPO_ROOT / "target" / "release" / "libstream_analytics.so"


def _ensure_dylib() -> str:
    """Build the stream-analytics dylib if it does not yet exist. Returns path."""
    dylib = _dylib_path()
    if dylib.exists():
        return str(dylib)

    print("[harness] libstream_analytics.so not found — building ...", flush=True)
    func_path = STREAM_ANALYTICS / "src" / "lib.rs"
    manifest = STREAM_ANALYTICS / "Cargo.toml"
    env = {
        "FUNC_PATH": str(func_path.resolve()),
    }
    build_env = {**os.environ, **env}
    result = subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--manifest-path",
            str(manifest.resolve()),
        ],
        env=build_env,
        cwd=str(REPO_ROOT),
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"cargo build failed (exit {result.returncode}) for stream-analytics"
        )
    if not dylib.exists():
        raise RuntimeError(f"dylib not found at {dylib} after build")
    return str(dylib)


def _find_binary() -> str:
    """Locate the tomii-core main binary (release preferred)."""
    for profile in ("release", "debug"):
        candidate = REPO_ROOT / "target" / profile / "main"
        if candidate.exists():
            return str(candidate)
    raise RuntimeError(
        "tomii binary not found. Build with: cargo build --release -p tomii-core --bin main"
    )


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------


def evaluate(
    knobs: KnobConfig,
    streams: int = 500,
    warmup: int = 50,
) -> EvalResult:
    """Run stream-analytics with the given knobs and return an EvalResult.

    Uses a temporary result.txt so concurrent calls don't interfere.
    """
    t0 = time.monotonic()

    try:
        dylib = _ensure_dylib()
    except RuntimeError as exc:
        return EvalResult(
            verifier_ok=False,
            ms_per_stream=None,
            rejection_reason=f"dylib build failed: {exc}",
            wall_seconds=time.monotonic() - t0,
        )

    try:
        binary = _find_binary()
    except RuntimeError as exc:
        return EvalResult(
            verifier_ok=False,
            ms_per_stream=None,
            rejection_reason=f"binary not found: {exc}",
            wall_seconds=time.monotonic() - t0,
        )

    graph_json = STREAM_ANALYTICS / "graph.json"

    with tempfile.TemporaryDirectory(prefix="agent_tuning_") as tmp_str:
        tmp_dir = Path(tmp_str)
        result_file = tmp_dir / "result.txt"
        report_file = tmp_dir / "report.json"
        result_file.touch()

        out_file = tmp_dir / "out.txt"
        timing_file = tmp_dir / "timing.txt"
        cmd = [
            binary,
            "--json", str(graph_json),
            "--dylib", dylib,
            "--max-streams", str(streams),
            "--exclude-streams", str(warmup),
            "--workers", str(knobs.workers),
            "--slots", str(knobs.slots),
            "--batching-size", str(knobs.batching_size),
            "--output", str(out_file),
            "--report", str(report_file),
            "--timing", str(timing_file),
        ]
        if knobs.inline_continuation:
            cmd.append("--inline-continuation")
        if knobs.coalesce_barriers:
            cmd.append("--coalesce-barriers")
        if knobs.fifo:
            cmd.append("--fifo")
        if knobs.custom:
            cmd.append("--custom")
        if knobs.no_fanout_bulk:
            cmd.append("--no-fanout-bulk")

        run_env = {**os.environ, "SCRIPT_DIR": str(tmp_dir)}

        try:
            proc = subprocess.run(
                cmd,
                env=run_env,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except subprocess.TimeoutExpired:
            return EvalResult(
                verifier_ok=False,
                ms_per_stream=None,
                rejection_reason="timeout after 120s",
                wall_seconds=time.monotonic() - t0,
            )

        if proc.returncode != 0:
            stderr_tail = (proc.stderr or "")[-200:].strip()
            return EvalResult(
                verifier_ok=False,
                ms_per_stream=None,
                rejection_reason=f"tomii exit {proc.returncode}: {stderr_tail}",
                wall_seconds=time.monotonic() - t0,
            )

        # Run verifier
        golden = STREAM_ANALYTICS / "result.golden.txt"
        verify_proc = subprocess.run(
            [
                sys.executable,
                str(VERIFY_PY),
                "--result", str(result_file),
                "--golden", str(golden),
                "--streams", str(streams),
            ],
            capture_output=True,
            text=True,
        )

        if verify_proc.returncode != 0:
            msg = (verify_proc.stdout + verify_proc.stderr).strip()
            return EvalResult(
                verifier_ok=False,
                ms_per_stream=None,
                rejection_reason=f"verifier: {msg}",
                wall_seconds=time.monotonic() - t0,
            )

        # Parse latency from report.json
        ms: float | None = None
        if report_file.exists():
            try:
                data = json.loads(report_file.read_text())
                avg_us = data.get("summary", {}).get("avg_latency_us")
                if avg_us is not None:
                    ms = float(avg_us) / 1000.0
            except Exception:
                pass

        wall = time.monotonic() - t0
        return EvalResult(
            verifier_ok=True,
            ms_per_stream=ms,
            rejection_reason=None,
            wall_seconds=wall,
        )


# ---------------------------------------------------------------------------
# Baseline
# ---------------------------------------------------------------------------


def establish_baseline(
    streams: int = 500,
    warmup: int = 50,
    results_dir: Path | None = None,
) -> float:
    """Run with default KnobConfig and return ms_per_stream.

    Writes results_dir/baseline.json. Falls back to 0.0 on failure.
    """
    knobs = KnobConfig()
    print("[harness] establishing baseline with default knobs ...", flush=True)
    result = evaluate(knobs, streams=streams, warmup=warmup)

    if not result.verifier_ok or result.ms_per_stream is None:
        reason = result.rejection_reason or "unknown"
        print(f"[harness] WARNING: baseline run failed: {reason}", flush=True)
        baseline_ms = 0.0
    else:
        baseline_ms = result.ms_per_stream
        print(
            f"[harness] baseline = {baseline_ms:.4f} ms/stream  "
            f"(wall {result.wall_seconds:.1f}s)",
            flush=True,
        )

    if results_dir is not None:
        results_dir.mkdir(parents=True, exist_ok=True)
        data: dict[str, Any] = {
            "baseline_ms_per_stream": baseline_ms,
            "verifier_ok": result.verifier_ok,
            "rejection_reason": result.rejection_reason,
            "wall_seconds": result.wall_seconds,
            "knobs": asdict(knobs),
        }
        (results_dir / "baseline.json").write_text(json.dumps(data, indent=2))

    return baseline_ms


# ---------------------------------------------------------------------------
# Trial logging
# ---------------------------------------------------------------------------


def log_trial(record: TrialRecord, log_file: Path) -> None:
    """Append a JSON line to log_file with all trial fields."""
    entry: dict[str, Any] = {
        "iteration": record.iteration,
        "arm": record.arm,
        "notes": record.notes,
        "knobs": asdict(record.knobs),
        "verifier_ok": record.result.verifier_ok,
        "ms_per_stream": record.result.ms_per_stream,
        "rejection_reason": record.result.rejection_reason,
        "wall_seconds": record.result.wall_seconds,
    }
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# Knob space
# ---------------------------------------------------------------------------


def load_knob_space() -> dict[str, Any]:
    """Read knob_space.json and return it as a dict."""
    ks_path = _HERE / "knob_space.json"
    result: dict[str, Any] = json.loads(ks_path.read_text())
    return result


# ---------------------------------------------------------------------------
# CLI (standalone use)
# ---------------------------------------------------------------------------


def _main() -> None:
    p = argparse.ArgumentParser(description="agent-tuning harness — establish baseline")
    p.add_argument("--streams", type=int, default=500, help="total streams to run")
    p.add_argument("--warmup", type=int, default=50, help="warm-up streams to exclude")
    p.add_argument(
        "--results-dir",
        type=Path,
        default=Path("results"),
        help="directory to write baseline.json",
    )
    args = p.parse_args()

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    print(f"baseline ms/stream: {baseline:.4f}")


if __name__ == "__main__":
    _main()
