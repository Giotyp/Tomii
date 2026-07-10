"""Workload definitions for the agent-tuning harness.

A Workload bundles everything an arm script needs to tune one benchmark:

- the generated knob space (`tomii.knob_space` over the workload's graph — M3,
  no hand-written per-workload spec),
- artifact builds (dylib + main binary with matching FUNC_PATH),
- one-trial evaluation, gated by the workload's own verifier (a trial only
  counts toward perf when the verifier passes — rejected trials are logged
  with the reason),
- the documented baseline knob configuration.

Arms select a workload with --workload; the search loop is workload-agnostic.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from tomii import knobs as tomii_knobs  # noqa: E402
from tomii._runner import build_command  # noqa: E402

import importlib.util
import math
import re


def _import_module(name: str, path: Path) -> Any:
    """Import a bench script as an isolated module (avoids run_bench name clashes)."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _parse_avg_ms(timing_file: Path) -> float:
    """Parse `Avg Time Per Frame` from a Tomii timing file, normalized to ms."""
    if not timing_file.exists():
        return float("nan")
    m = re.search(
        r"Avg Time Per Frame:\s+([\d.]+)(ms|µs|us|s)", timing_file.read_text()
    )
    if not m:
        return float("nan")
    val, unit = float(m.group(1)), m.group(2)
    if unit in ("µs", "us"):
        return val / 1e3
    if unit == "s":
        return val * 1e3
    return val


def _parse_frames_processed(timing_file: Path) -> int:
    if not timing_file.exists():
        return -1
    m = re.search(r"Total Frames Processed:\s+(\d+)", timing_file.read_text())
    return int(m.group(1)) if m else -1


@dataclass
class EvalResult:
    verifier_ok: bool
    ms_per_frame: float | None  # None if verifier failed or timing unavailable
    rejection_reason: str | None
    wall_seconds: float


class Workload:
    """Base class: shared knob-space plumbing; subclasses implement evaluate."""

    name: str = ""
    #: Documented starting configuration (also used for the baseline run).
    baseline_knobs: dict[str, Any] = {}
    #: Path to the graph JSON the knob space is generated from (may be
    #: produced lazily by ensure_built for generated-graph workloads).
    graph_json: Path

    def __init__(self) -> None:
        self._space: dict[str, Any] | None = None

    def knob_space(self) -> dict[str, Any]:
        """Generate (and cache) the knob search space for this workload."""
        if self._space is None:
            self.ensure_built()
            self._space = tomii_knobs.knob_space(self.graph_json, workload=self.name)
        return self._space

    def ensure_built(self) -> None:
        """Build all artifacts needed to evaluate (idempotent, cheap when fresh)."""
        raise NotImplementedError

    def evaluate(
        self,
        knobs: dict[str, Any],
        frames: int,
        warmup: int,
        space: dict[str, Any] | None = None,
    ) -> EvalResult:
        """Run one verifier-gated trial with the given knob values."""
        raise NotImplementedError

    # -- shared helpers ----------------------------------------------------

    def _split_or_reject(
        self, knobs: dict[str, Any], space: dict[str, Any], t0: float
    ) -> tuple[dict[str, Any], list[dict[str, Any]]] | EvalResult:
        try:
            return tomii_knobs.split(space, knobs)
        except KeyError as exc:
            return EvalResult(
                verifier_ok=False,
                ms_per_frame=None,
                rejection_reason=f"unknown knob: {exc}",
                wall_seconds=time.monotonic() - t0,
            )

    def _patched_graph_path(
        self, graph_edits: list[dict[str, Any]], tmp_dir: Path
    ) -> Path:
        """Write a per-trial patched copy of the graph (or return the original)."""
        if not graph_edits:
            return self.graph_json
        patched = tomii_knobs.apply_graph_edits(self.graph_json, graph_edits)
        path = tmp_dir / "graph.json"
        path.write_text(json.dumps(patched, indent=1), encoding="utf-8")
        return path


def _cargo(args: list[str], env: dict[str, str], what: str) -> None:
    result = subprocess.run(args, env=env, cwd=str(REPO_ROOT), capture_output=False)
    if result.returncode != 0:
        raise RuntimeError(f"cargo build failed (exit {result.returncode}) for {what}")


# ---------------------------------------------------------------------------
# stream-analytics
# ---------------------------------------------------------------------------


class StreamAnalyticsWorkload(Workload):
    """Self-contained streaming example; golden-file verifier, avg-latency metric."""

    name = "stream-analytics"
    baseline_knobs = {
        "workers": 4,
        "slots": 4,
        "inline_continuation": True,
        "coalesce_barriers": True,
        "fifo": False,
        "custom": True,
        "no_fanout_bulk": False,
        "batching_size": 1,
    }

    def __init__(self) -> None:
        super().__init__()
        self.root = REPO_ROOT / "examples" / "stream-analytics"
        self.graph_json = self.root / "graph.json"
        self.verify_py = self.root / "verify.py"
        self.dylib = REPO_ROOT / "target" / "release" / "libstream_analytics.so"
        self.binary = REPO_ROOT / "target" / "release" / "main"

    def ensure_built(self) -> None:
        func_path = self.root / "src" / "lib.rs"
        build_env = {**os.environ, "FUNC_PATH": str(func_path.resolve())}

        if not self.dylib.exists():
            print(
                "[workload] libstream_analytics.so not found — building ...",
                flush=True,
            )
            _cargo(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--manifest-path",
                    str((self.root / "Cargo.toml").resolve()),
                ],
                build_env,
                "stream-analytics",
            )
            if not self.dylib.exists():
                raise RuntimeError(f"dylib not found at {self.dylib} after build")

        # Always (re)build the main binary with this workload's FUNC_PATH so
        # the embedded function registry matches the dylib.  cargo is a no-op
        # when inputs are unchanged.
        _cargo(
            ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
            build_env,
            "tomii-core",
        )

    def evaluate(
        self,
        knobs: dict[str, Any],
        frames: int = 500,
        warmup: int = 50,
        space: dict[str, Any] | None = None,
    ) -> EvalResult:
        t0 = time.monotonic()
        if space is None:
            space = self.knob_space()

        split = self._split_or_reject(knobs, space, t0)
        if isinstance(split, EvalResult):
            return split
        cli_kwargs, graph_edits = split

        try:
            self.ensure_built()
        except RuntimeError as exc:
            return EvalResult(
                verifier_ok=False,
                ms_per_frame=None,
                rejection_reason=f"build failed: {exc}",
                wall_seconds=time.monotonic() - t0,
            )

        with tempfile.TemporaryDirectory(prefix="agent_tuning_") as tmp_str:
            tmp_dir = Path(tmp_str)
            result_file = tmp_dir / "result.txt"
            report_file = tmp_dir / "report.json"
            result_file.touch()

            graph_path = self._patched_graph_path(graph_edits, tmp_dir)

            cmd = build_command(
                str(self.binary),
                str(graph_path),
                str(self.dylib),
                max_frames=frames,
                exclude_frames=warmup,
                output=str(tmp_dir / "out.txt"),
                report=str(report_file),
                timing=str(tmp_dir / "timing.txt"),
                **cli_kwargs,
            )
            run_env = {**os.environ, "SCRIPT_DIR": str(tmp_dir)}

            try:
                proc = subprocess.run(
                    cmd, env=run_env, capture_output=True, text=True, timeout=120
                )
            except subprocess.TimeoutExpired:
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason="timeout after 120s",
                    wall_seconds=time.monotonic() - t0,
                )

            if proc.returncode != 0:
                stderr_tail = (proc.stderr or "")[-200:].strip()
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason=f"tomii exit {proc.returncode}: {stderr_tail}",
                    wall_seconds=time.monotonic() - t0,
                )

            verify_proc = subprocess.run(
                [
                    sys.executable,
                    str(self.verify_py),
                    "--result",
                    str(result_file),
                    "--golden",
                    str(self.root / "result.golden.txt"),
                    "--frames",
                    str(frames),
                ],
                capture_output=True,
                text=True,
            )
            if verify_proc.returncode != 0:
                msg = (verify_proc.stdout + verify_proc.stderr).strip()
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason=f"verifier: {msg}",
                    wall_seconds=time.monotonic() - t0,
                )

            ms: float | None = None
            if report_file.exists():
                try:
                    data = json.loads(report_file.read_text())
                    avg_us = data.get("summary", {}).get("avg_latency_us")
                    if avg_us is not None:
                        ms = float(avg_us) / 1000.0
                except Exception:
                    pass

            return EvalResult(
                verifier_ok=True,
                ms_per_frame=ms,
                rejection_reason=None,
                wall_seconds=time.monotonic() - t0,
            )


# ---------------------------------------------------------------------------
# pipeline (bench/pipeline-bench)
# ---------------------------------------------------------------------------


class PipelineWorkload(Workload):
    """4-stage fan-out/fan-in pipeline (bench/pipeline-bench), self-contained.

    Per-trial verification is knob-aware: before the perf run, the verify
    graph (pl_emit_to_file) runs with the SAME CLI knobs and graph edits and
    its numeric output is checked against the Python reference — replicating
    bench/pipeline-bench/tomii/verify.py's checks (line count, 30% envelope
    for SIMD divergence, cross-frame consistency).
    """

    name = "pipeline"
    N = 256  # items per frame — matches the bench default
    baseline_knobs = {
        "workers": 4,
        "slots": 4,
        "system_threads": 1,
        "inline_continuation": True,
        "coalesce_barriers": True,
        "fifo": False,
        "custom": True,
        "no_fanout_bulk": False,
        "slot_priority": False,
        "batching_size": 1,
    }

    def __init__(self) -> None:
        super().__init__()
        self.root = REPO_ROOT / "bench" / "pipeline-bench" / "tomii"
        self.dylib = self.root / "target" / "release" / "libpl_bench.so"
        self.binary = REPO_ROOT / "target" / "release" / "main"
        self._run_bench = _import_module("plbench_run", self.root / "run_bench.py")
        self._verify = _import_module("plbench_verify", self.root / "verify.py")
        self._built = False

        # Serialize the bench's in-process graph once; the knob space and all
        # per-trial patched copies derive from this file.
        graph = self._run_bench.build_pipeline(self.N)
        fh = tempfile.NamedTemporaryFile(
            prefix="agent_tuning_pipeline_", suffix=".json", delete=False, mode="w"
        )
        fh.write(graph.to_json())
        fh.close()
        self.graph_json = Path(fh.name)

        src = (self.root / "src" / "lib.rs").read_text()
        m = re.search(r"const TRANSFORM_ITERS\s*:\s*usize\s*=\s*(\d+)", src)
        self.transform_iters = int(m.group(1)) if m else 2048

    def ensure_built(self) -> None:
        if self._built:
            return
        build_env = {**os.environ, "FUNC_PATH": str(self.root / "src" / "lib.rs")}
        if not self.dylib.exists():
            print("[workload] libpl_bench.so not found — building ...", flush=True)
            _cargo(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--manifest-path",
                    str(self.root / "Cargo.toml"),
                ],
                build_env,
                "pipeline-bench plugin",
            )
        _cargo(
            ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
            build_env,
            "tomii-core",
        )
        self._built = True

    def _run_verify_pass(
        self,
        cli_kwargs: dict[str, Any],
        graph_edits: list[dict[str, Any]],
        tmp_dir: Path,
        t0: float,
    ) -> EvalResult | None:
        """Run the emit-to-file graph with the trial's knobs; None on success."""
        verify_graph = json.loads(self._verify.build_verify_graph(self.N).to_json())
        if graph_edits:
            verify_graph = tomii_knobs.apply_graph_edits(verify_graph, graph_edits)
        graph_path = tmp_dir / "verify_graph.json"
        graph_path.write_text(json.dumps(verify_graph, indent=1), encoding="utf-8")

        result_file = tmp_dir / "verify_result.txt"
        frames = 5
        # max_runtime bounds structurally-broken graph edits (unresolvable
        # dependencies hang forever otherwise); rejection then comes from the
        # output checks below.  The subprocess timeout stays as backstop.
        cmd = build_command(
            str(self.binary),
            str(graph_path),
            str(self.dylib),
            max_frames=frames,
            exclude_frames=0,
            max_runtime=30,
            **cli_kwargs,
        )
        env = {**os.environ, "PIPELINE_BENCH_RESULT": str(result_file)}
        try:
            proc = subprocess.run(
                cmd, env=env, capture_output=True, text=True, timeout=60
            )
        except subprocess.TimeoutExpired:
            return self._reject("verify run timeout after 60s", t0)
        if proc.returncode != 0:
            tail = (proc.stderr or "")[-200:].strip()
            return self._reject(f"verify run exit {proc.returncode}: {tail}", t0)
        if not result_file.exists():
            return self._reject("verifier: result file was not written", t0)

        lines = [
            ln.strip() for ln in result_file.read_text().splitlines() if ln.strip()
        ]
        if len(lines) != frames:
            return self._reject(
                f"verifier: expected {frames} lines, got {len(lines)}", t0
            )
        expected = self._verify.expected_mean(self.N, self.transform_iters)
        tolerance = self._verify.RELATIVE_TOLERANCE
        vals = []
        for i, line in enumerate(lines):
            try:
                v = float(line)
            except ValueError:
                return self._reject(f"verifier: frame {i} not a float: {line!r}", t0)
            if not math.isfinite(v):
                return self._reject(f"verifier: frame {i} not finite", t0)
            if expected != 0.0 and abs(v - expected) / abs(expected) > tolerance:
                return self._reject(
                    f"verifier: frame {i} rel_delta "
                    f"{abs(v - expected) / abs(expected):.1%} > {tolerance:.0%}",
                    t0,
                )
            vals.append(v)
        if len(set(f"{v:.8f}" for v in vals)) > 1:
            return self._reject("verifier: frames non-deterministic", t0)
        return None

    def _reject(self, reason: str, t0: float) -> EvalResult:
        return EvalResult(
            verifier_ok=False,
            ms_per_frame=None,
            rejection_reason=reason,
            wall_seconds=time.monotonic() - t0,
        )

    def evaluate(
        self,
        knobs: dict[str, Any],
        frames: int = 500,
        warmup: int = 50,
        space: dict[str, Any] | None = None,
    ) -> EvalResult:
        t0 = time.monotonic()
        if space is None:
            space = self.knob_space()

        split = self._split_or_reject(knobs, space, t0)
        if isinstance(split, EvalResult):
            return split
        cli_kwargs, graph_edits = split

        try:
            self.ensure_built()
        except RuntimeError as exc:
            return self._reject(f"build failed: {exc}", t0)

        with tempfile.TemporaryDirectory(prefix="agent_tuning_") as tmp_str:
            tmp_dir = Path(tmp_str)

            failure = self._run_verify_pass(cli_kwargs, graph_edits, tmp_dir, t0)
            if failure is not None:
                return failure

            graph_path = self._patched_graph_path(graph_edits, tmp_dir)
            timing_file = tmp_dir / "timing.txt"
            cmd = build_command(
                str(self.binary),
                str(graph_path),
                str(self.dylib),
                max_frames=frames + warmup,
                exclude_frames=warmup,
                max_runtime=240,
                timing=str(timing_file),
                use_rdtsc=True,
                core_offset=1,
                **cli_kwargs,
            )
            try:
                proc = subprocess.run(
                    cmd, env=os.environ.copy(), capture_output=True, text=True,
                    timeout=300,
                )
            except subprocess.TimeoutExpired:
                return self._reject("timeout after 300s", t0)
            if proc.returncode != 0:
                tail = (proc.stderr or "")[-200:].strip()
                return self._reject(f"tomii exit {proc.returncode}: {tail}", t0)

            ms = _parse_avg_ms(timing_file)
            if math.isnan(ms):
                return self._reject("no Avg Time Per Frame in timing output", t0)
            return EvalResult(
                verifier_ok=True,
                ms_per_frame=ms,
                rejection_reason=None,
                wall_seconds=time.monotonic() - t0,
            )


# ---------------------------------------------------------------------------
# mimo (bench/mimo-bench) — network workload with external Agora sender
# ---------------------------------------------------------------------------


class MimoWorkload(Workload):
    """16x16 MIMO uplink pipeline fed by the Agora sender over UDP.

    Runtime (CLI) knobs only: graph knobs are DISABLED because there is no
    per-trial output verification for edited MIMO graphs yet (the hash
    verifier runs a fixed dump-node graph).  The per-trial gate is keep-up
    soundness — `Total Frames Processed` must equal the frames sent, so a
    config cannot look fast by dropping frames.  `frames` maps to the sender
    frame budget (default 500 is ~35s/trial; 200 is a reasonable tuning
    budget).  Sender lifecycle per the MIMO runbook: receiver first, 10s
    delay, hard kill after; MKL/OMP pinned to 1 thread.
    """

    name = "mimo"
    baseline_knobs = {
        "workers": 8,
        "slots": 4,
        "system_threads": 2,
        "receiver_threads": 4,
        "slot_priority": True,
        "inline_continuation": True,
        "coalesce_barriers": True,
        "fifo": False,
        "custom": True,
        "no_fanout_bulk": False,
        "batching_size": 1,
    }

    SENDER_DELAY_S = 10
    # Per-slot processing floor at 16x16 is ~48 ms; the sender pacing floor
    # for S slots is ceil(48000/S) µs per frame.
    SLOT_FLOOR_US = 48_000

    def __init__(self) -> None:
        super().__init__()
        self.root = REPO_ROOT / "bench" / "mimo-bench" / "tomii"
        self.agora_dir = Path("~/Agora").expanduser().resolve()
        self.dylib = self.root / "target" / "release" / "libmimo_bench_tomii.so"
        self.binary = REPO_ROOT / "target" / "release" / "main"
        self.sender_config = self.root / "graphs" / "tddconfig-16x16.json"
        self._build_graph = _import_module(
            "mimo_build_graph", self.root / "build_graph.py"
        )
        self._built = False

        graph = self._build_graph.build_mimo_graph(
            config_path=str(self.sender_config)
        )
        fh = tempfile.NamedTemporaryFile(
            prefix="agent_tuning_mimo_", suffix=".json", delete=False, mode="w"
        )
        fh.write(graph.to_json())
        fh.close()
        self.graph_json = Path(fh.name)

    def knob_space(self) -> dict[str, Any]:
        if self._space is None:
            self.ensure_built()
            self._space = tomii_knobs.knob_space(
                self.graph_json,
                workload=self.name,
                include_graph_knobs=False,  # see class docstring
            )
        return self._space

    def ensure_built(self) -> None:
        if self._built:
            return
        sender_bin = self.agora_dir / "build" / "sender"
        if not sender_bin.exists():
            raise RuntimeError(f"Agora sender not found at {sender_bin}")
        build_env = {**os.environ, "FUNC_PATH": str(self.root / "src" / "lib.rs")}
        if not self.dylib.exists():
            print(
                "[workload] libmimo_bench_tomii.so not found — building ...",
                flush=True,
            )
            _cargo(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--manifest-path",
                    str(self.root / "Cargo.toml"),
                ],
                build_env,
                "mimo-bench plugin",
            )
        _cargo(
            ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
            build_env,
            "tomii-core",
        )
        self._built = True

    def _sender_config_for(self, num_frames: int, tmp_dir: Path) -> Path:
        """Temp tddconfig with max_frame pinned so the sender stops after
        exactly num_frames (same mechanism as verify.py)."""
        cfg = json.loads(self.sender_config.read_text())
        cfg["max_frame"] = num_frames
        path = tmp_dir / "sender_config.json"
        path.write_text(json.dumps(cfg), encoding="utf-8")
        return path

    def evaluate(
        self,
        knobs: dict[str, Any],
        frames: int = 500,
        warmup: int = 50,
        space: dict[str, Any] | None = None,
    ) -> EvalResult:
        t0 = time.monotonic()
        if space is None:
            space = self.knob_space()

        split = self._split_or_reject(knobs, space, t0)
        if isinstance(split, EvalResult):
            return split
        cli_kwargs, graph_edits = split
        if graph_edits:
            return EvalResult(
                verifier_ok=False,
                ms_per_frame=None,
                rejection_reason="graph knobs are disabled for mimo",
                wall_seconds=time.monotonic() - t0,
            )

        try:
            self.ensure_built()
        except RuntimeError as exc:
            return EvalResult(
                verifier_ok=False,
                ms_per_frame=None,
                rejection_reason=f"build failed: {exc}",
                wall_seconds=time.monotonic() - t0,
            )

        num_frames = frames
        warmup = min(warmup, num_frames // 5)
        slots = int(cli_kwargs.get("slots", 1))
        # Pace at 2x the per-slot throughput floor: fast enough that knob
        # effects show in per-slot latency (at slack pacing every config sits
        # at the ~48 ms compute floor and tuning is insensitive), slow enough
        # to stay off the keep-up cliff where measurements are unstable.
        frame_duration = max(2 * (-(-self.SLOT_FLOOR_US // slots)), 2_000)
        send_window_s = (num_frames * frame_duration) / 1_000_000
        max_runtime = int(self.SENDER_DELAY_S + send_window_s + 20)

        with tempfile.TemporaryDirectory(prefix="agent_tuning_mimo_") as tmp_str:
            tmp_dir = Path(tmp_str)
            timing_file = tmp_dir / "timing.txt"
            sender_cfg = self._sender_config_for(num_frames, tmp_dir)

            cmd = build_command(
                str(self.binary),
                str(self.graph_json),
                str(self.dylib),
                core_offset=1,
                max_frames=num_frames,
                exclude_frames=warmup,
                max_runtime=max_runtime,
                timing=str(timing_file),
                use_rdtsc=True,
                **cli_kwargs,
            )
            env = {
                **os.environ,
                "MKL_NUM_THREADS": "1",
                "OMP_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "GOTO_NUM_THREADS": "1",
            }

            tomii_proc = subprocess.Popen(
                cmd, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            time.sleep(self.SENDER_DELAY_S)
            sender_cmd = [
                str(self.agora_dir / "build" / "sender"),
                "--num_threads=2",
                "--core_offset=55",
                f"--frame_duration={frame_duration}",
                "--enable_slow_start=0",
                "--inter_frame_delay=0",
                f"--conf_file={sender_cfg}",
            ]
            sender_proc = subprocess.Popen(
                sender_cmd,
                cwd=str(self.agora_dir),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=os.environ.copy(),
            )

            try:
                ret = tomii_proc.wait(timeout=max_runtime + 15)
            except subprocess.TimeoutExpired:
                tomii_proc.kill()
                tomii_proc.wait()
                ret = -1
            finally:
                # The Agora sender ignores SIGTERM — kill hard, always.
                sender_proc.kill()
                sender_proc.wait()

            if ret != 0:
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason=(
                        "tomii hung past watchdog"
                        if ret == -1
                        else f"tomii exit {ret}"
                    ),
                    wall_seconds=time.monotonic() - t0,
                )

            processed = _parse_frames_processed(timing_file)
            if processed != num_frames:
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason=(
                        f"keep-up gate: processed {processed} of {num_frames} "
                        "frames (config fell behind or dropped frames)"
                    ),
                    wall_seconds=time.monotonic() - t0,
                )

            ms = _parse_avg_ms(timing_file)
            if math.isnan(ms):
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason="no Avg Time Per Frame in timing output",
                    wall_seconds=time.monotonic() - t0,
                )
            return EvalResult(
                verifier_ok=True,
                ms_per_frame=ms,
                rejection_reason=None,
                wall_seconds=time.monotonic() - t0,
            )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_WORKLOAD_TYPES: dict[str, type[Workload]] = {
    StreamAnalyticsWorkload.name: StreamAnalyticsWorkload,
    PipelineWorkload.name: PipelineWorkload,
    MimoWorkload.name: MimoWorkload,
}

_INSTANCES: dict[str, Workload] = {}


def workload_names() -> list[str]:
    return sorted(_WORKLOAD_TYPES)


def get_workload(name: str) -> Workload:
    """Return the (cached) workload instance for `name`."""
    if name not in _WORKLOAD_TYPES:
        raise KeyError(f"unknown workload {name!r}; available: {workload_names()}")
    if name not in _INSTANCES:
        _INSTANCES[name] = _WORKLOAD_TYPES[name]()
    return _INSTANCES[name]
