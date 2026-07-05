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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
STREAM_ANALYTICS = REPO_ROOT / "examples" / "stream-analytics"
VERIFY_PY = STREAM_ANALYTICS / "verify.py"
_HERE = Path(__file__).resolve().parent

sys.path.insert(0, str(REPO_ROOT))

from tomii import knobs as tomii_knobs  # noqa: E402
from tomii._runner import build_command  # noqa: E402

GRAPH_JSON = STREAM_ANALYTICS / "graph.json"

# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

# Knob configurations are plain dicts keyed by knob name, drawn from the
# generated knob space (see load_knob_space) — no per-workload dataclass.

# The workload's documented starting point (matches the pre-M3 defaults).
BASELINE_KNOBS: dict[str, Any] = {
    "workers": 4,
    "slots": 4,
    "inline_continuation": True,
    "coalesce_barriers": True,
    "fifo": False,
    "custom": True,
    "no_fanout_bulk": False,
    "batching_size": 1,
}


@dataclass
class EvalResult:
    verifier_ok: bool
    ms_per_stream: float | None  # None if verifier failed or timing unavailable
    rejection_reason: str | None
    wall_seconds: float


@dataclass
class TrialRecord:
    iteration: int
    knobs: dict[str, Any]
    result: EvalResult
    arm: str
    notes: str = ""


# ---------------------------------------------------------------------------
# Build helpers
# ---------------------------------------------------------------------------


def _dylib_path() -> Path:
    """Return the expected release dylib path for stream-analytics."""
    return REPO_ROOT / "target" / "release" / "libstream_analytics.so"


def _ensure_build() -> tuple[str, str]:
    """Build the stream-analytics dylib and tomii-core main binary if needed.

    Both artifacts must be compiled with the same FUNC_PATH so the function
    registry embedded in the main binary matches the dylib's exported symbols.
    Returns (dylib_path, binary_path).
    """
    func_path = STREAM_ANALYTICS / "src" / "lib.rs"
    manifest = STREAM_ANALYTICS / "Cargo.toml"
    build_env = {**os.environ, "FUNC_PATH": str(func_path.resolve())}

    dylib = _dylib_path()
    binary = REPO_ROOT / "target" / "release" / "main"

    if not dylib.exists():
        print("[harness] libstream_analytics.so not found — building ...", flush=True)
        result = subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", str(manifest.resolve())],
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

    # Always build the main binary with the stream-analytics FUNC_PATH so its
    # embedded function registry matches the dylib. cargo is a no-op if inputs
    # are unchanged, so this is cheap when everything is already up-to-date.
    result = subprocess.run(
        ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
        env=build_env,
        cwd=str(REPO_ROOT),
        capture_output=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"cargo build failed (exit {result.returncode}) for tomii-core"
        )

    return str(dylib), str(binary)


def _find_binary() -> str:
    """Locate the tomii-core main binary (release preferred)."""
    for profile in ("release", "debug"):
        candidate = REPO_ROOT / "target" / profile / "main"
        if candidate.exists():
            return str(candidate)
    raise RuntimeError(
        "tomii binary not found. Build with:\n"
        "  FUNC_PATH=$(pwd)/examples/stream-analytics/src/lib.rs "
        "cargo build --release -p tomii-core --bin main"
    )


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------


def evaluate(
    knobs: dict[str, Any],
    streams: int = 500,
    warmup: int = 50,
    space: dict[str, Any] | None = None,
) -> EvalResult:
    """Run stream-analytics with the given knob values and return an EvalResult.

    `knobs` is a dict keyed by knob name from the generated knob space: CLI
    knobs become command-line flags, `graph:*` knobs are applied as edits to a
    per-trial copy of graph.json.  Uses a temporary result.txt so concurrent
    calls don't interfere.
    """
    t0 = time.monotonic()

    if space is None:
        space = load_knob_space()

    try:
        cli_kwargs, graph_edits = tomii_knobs.split(space, knobs)
    except KeyError as exc:
        return EvalResult(
            verifier_ok=False,
            ms_per_stream=None,
            rejection_reason=f"unknown knob: {exc}",
            wall_seconds=time.monotonic() - t0,
        )

    try:
        dylib, binary = _ensure_build()
    except RuntimeError as exc:
        return EvalResult(
            verifier_ok=False,
            ms_per_stream=None,
            rejection_reason=f"build failed: {exc}",
            wall_seconds=time.monotonic() - t0,
        )

    with tempfile.TemporaryDirectory(prefix="agent_tuning_") as tmp_str:
        tmp_dir = Path(tmp_str)
        result_file = tmp_dir / "result.txt"
        report_file = tmp_dir / "report.json"
        result_file.touch()

        out_file = tmp_dir / "out.txt"
        timing_file = tmp_dir / "timing.txt"

        graph_path = GRAPH_JSON
        if graph_edits:
            patched = tomii_knobs.apply_graph_edits(GRAPH_JSON, graph_edits)
            graph_path = tmp_dir / "graph.json"
            graph_path.write_text(json.dumps(patched, indent=1), encoding="utf-8")

        cmd = build_command(
            binary,
            str(graph_path),
            dylib,
            max_streams=streams,
            exclude_streams=warmup,
            output=str(out_file),
            report=str(report_file),
            timing=str(timing_file),
            **cli_kwargs,
        )

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
    """Run with the documented BASELINE_KNOBS and return ms_per_stream.

    Writes results_dir/baseline.json. Falls back to 0.0 on failure.
    """
    knobs = dict(BASELINE_KNOBS)
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
            "knobs": knobs,
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
        "knobs": record.knobs,
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


_SPACE_CACHE: dict[str, Any] | None = None


def load_knob_space() -> dict[str, Any]:
    """Generate the knob search space for stream-analytics.

    Generated live from the runtime knob catalog + graph.json — the single
    source of truth (M3).  No hand-written per-workload spec exists; regenerate
    a snapshot any time with:

        python -m tomii --knob-space examples/stream-analytics/graph.json \\
            --workload stream-analytics
    """
    global _SPACE_CACHE
    if _SPACE_CACHE is None:
        _SPACE_CACHE = tomii_knobs.knob_space(
            GRAPH_JSON, workload="stream-analytics"
        )
    return _SPACE_CACHE


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
