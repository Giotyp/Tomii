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

_HERE = Path(__file__).resolve().parent

# --- Eval-protocol machine discipline (paper/experiments/EVAL_PROTOCOL.md) ---
# Measured processes run on NUMA0; builds (and the harness/LLM processes,
# via run_e8.sh) on NUMA1 at low priority.  Each workload builds into its own
# CARGO_TARGET_DIR so the FUNC_PATH-specific `main` binary never clobbers (or
# is clobbered by) other experiments sharing the worktree's target/.
MEASURE_CPUS = os.environ.get("E8_MEASURE_CPUS", "0-31")
BUILD_PREFIX = ["taskset", "-c", "32-63", "nice", "-n", "10"]
TARGET_ROOT = Path(os.environ.get("E8_TARGET_ROOT", str(_HERE / ".target")))

# Per-run report fields surfaced to optimizers (the `summary` section of
# `--report` JSON plus its bottleneck hints; per-node arrays are dropped to
# keep the prompt bounded).
_REPORT_DROP_KEYS = (
    "nodes",
    "per_node",
    "tasks",
    "per_task",
    "workers_detail",
    "frame_latencies_us",
)


def _die_with_parent() -> None:  # pragma: no cover - runs in the child
    """preexec_fn: SIGKILL measured children if the harness dies, so an
    interrupted arm never leaves an unlocked process on the measurement cores."""
    import ctypes
    import signal

    ctypes.CDLL("libc.so.6", use_errno=True).prctl(1, signal.SIGKILL)  # PR_SET_PDEATHSIG


def measured(cmd: list[str]) -> list[str]:
    """Pin a measured Tomii command to the measurement cores."""
    return ["taskset", "-c", MEASURE_CPUS, *cmd]


def load_report(path: Path) -> dict[str, Any] | None:
    """Load a `--report` JSON, trimmed to scalar/summary content."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None

    def trim(obj: Any, depth: int = 0) -> Any:
        if isinstance(obj, dict):
            return {
                k: trim(v, depth + 1)
                for k, v in obj.items()
                if k not in _REPORT_DROP_KEYS
            }
        if isinstance(obj, list):
            if len(obj) > 8:
                return [trim(v, depth + 1) for v in obj[:8]] + ["..."]
            return [trim(v, depth + 1) for v in obj]
        if isinstance(obj, float):
            return round(obj, 3)
        return obj

    return trim(data)


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
    report: dict[str, Any] | None = None  # trimmed --report JSON (perf run)


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


def _cargo(
    args: list[str], env: dict[str, str], what: str, target_dir: Path
) -> None:
    env = {**env, "CARGO_TARGET_DIR": str(target_dir)}
    result = subprocess.run(
        BUILD_PREFIX + args, env=env, cwd=str(REPO_ROOT), capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(result.stderr[-4000:], file=sys.stderr)
        raise RuntimeError(f"cargo build failed (exit {result.returncode}) for {what}")


# ---------------------------------------------------------------------------
# stream-analytics
# ---------------------------------------------------------------------------


class StreamAnalyticsWorkload(Workload):
    """Self-contained streaming example; golden-file verifier, avg-latency metric."""

    name = "stream-analytics"
    #: Per-trial watchdog (5 s = ~50x the ~0.1 s wall of any passing run);
    #: graph edits that break the derived-size invariant (total_readings !=
    #: num_sensors * readings_per_sensor) run away in memory/time instead of
    #: failing fast.  Anything slower than TIMEOUT_S is infeasible, uniformly
    #: for every arm (pre-E8: 120 s, >80% of the classic arms' time).
    TIMEOUT_S = int(os.environ.get("E8_SA_TIMEOUT_S", "5"))
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
        self.target = TARGET_ROOT / self.name
        self.dylib = self.target / "release" / "libstream_analytics.so"
        self.binary = self.target / "release" / "main"
        self._built = False

    #: Fixed by the graph (not a knob); golden output assumes 4 x 8 readings.
    READINGS_PER_SENSOR = 8

    def _invariant_violation(self, knobs: dict[str, Any]) -> str | None:
        """Derived-size invariant the golden file cannot see.

        The golden output is a constant per sensor, so it does NOT depend on
        how many readings are generated/classified: with total_readings=8
        (< num_sensors x readings_per_sensor = 32) the frame silently skips
        3/4 of its per-reading work and still matches the golden file.  E8
        found the agent exploiting this; the gate now enforces the invariant.
        """
        sensors = int(knobs.get("graph:init.num_sensors", 4))
        total = int(knobs.get("graph:init.total_readings", 32))
        if total != sensors * self.READINGS_PER_SENSOR:
            return (
                f"derived-size invariant violated: total_readings ({total}) != "
                f"num_sensors ({sensors}) x readings_per_sensor "
                f"({self.READINGS_PER_SENSOR}); the frame would skip or invent "
                "readings"
            )
        return None

    def ensure_built(self) -> None:
        if self._built:
            return
        func_path = self.root / "src" / "lib.rs"
        build_env = {**os.environ, "FUNC_PATH": str(func_path.resolve())}

        if True:  # always: cargo is a no-op when fresh
            print("[workload] building libstream_analytics.so ...", flush=True)
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
                self.target,
            )
            if not self.dylib.exists():
                raise RuntimeError(f"dylib not found at {self.dylib} after build")

        # Build the main binary with this workload's FUNC_PATH so the embedded
        # function registry matches the dylib (own target dir: see TARGET_ROOT).
        _cargo(
            ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
            build_env,
            "tomii-core",
            self.target,
        )
        self._built = True

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

        bad = self._invariant_violation(knobs)
        if bad is not None:
            return EvalResult(
                verifier_ok=False,
                ms_per_frame=None,
                rejection_reason=f"verifier: {bad}",
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
                    measured(cmd),
                    preexec_fn=_die_with_parent,
                    env=run_env,
                    capture_output=True,
                    text=True,
                    timeout=self.TIMEOUT_S,
                )
            except subprocess.TimeoutExpired:
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason=f"timeout after {self.TIMEOUT_S}s",
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
            report = load_report(report_file)
            if report is not None:
                avg_us = report.get("summary", {}).get("avg_latency_us")
                if avg_us is not None:
                    ms = float(avg_us) / 1000.0
            if ms is None:
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason="no avg_latency_us in --report output",
                    wall_seconds=time.monotonic() - t0,
                )

            return EvalResult(
                verifier_ok=True,
                ms_per_frame=ms,
                rejection_reason=None,
                wall_seconds=time.monotonic() - t0,
                report=report,
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
    #: Verify pass = 5 frames (< 1 s for any sane config).
    VERIFY_TIMEOUT_S = 20
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
        self.target = TARGET_ROOT / self.name
        self.dylib = self.target / "release" / "libpl_bench.so"
        self.binary = self.target / "release" / "main"
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

    # The bench's verification emitter formats the float straight into an
    # unbuffered File (several write(2) calls per line), so when two frames'
    # emits overlap (slots > 1) their fragments interleave: lines merge
    # ("0.0.00000069352700") or vanish, and the verify pass FALSELY rejects
    # the config — reproducibly for inline_continuation=false, slots=4 (the
    # pre-E8 study had this too).  E8 builds a harness-local copy of the
    # plugin whose ONLY change is emitting each line with one write(2)
    # (atomic under O_APPEND).  The perf-graph functions are byte-identical.
    _EMIT_OLD = 'let _ = writeln!(f, "{:.10}", mean);'
    _EMIT_NEW = 'let _ = f.write_all(format!("{:.10}\\n", mean).as_bytes());'

    def _patched_plugin_root(self) -> Path:
        dst = _HERE / ".pl-plugin"
        (dst / "src").mkdir(parents=True, exist_ok=True)
        for rel in ("Cargo.toml", "Cargo.lock"):
            # Copied once; cargo may refresh the copy's (stale) lockfile.
            if not (dst / rel).exists():
                (dst / rel).write_text((self.root / rel).read_text())
        lib = (self.root / "src" / "lib.rs").read_text()
        if lib.count(self._EMIT_OLD) != 1:
            raise RuntimeError("pipeline plugin emitter changed; revisit E8 patch")
        patched = lib.replace(self._EMIT_OLD, self._EMIT_NEW)
        if (
            not (dst / "src" / "lib.rs").exists()
            or (dst / "src" / "lib.rs").read_text() != patched
        ):
            (dst / "src" / "lib.rs").write_text(patched)
        return dst

    def ensure_built(self) -> None:
        if self._built:
            return
        plugin_root = self._patched_plugin_root()
        build_env = {**os.environ, "FUNC_PATH": str(plugin_root / "src" / "lib.rs")}
        if True:  # always: cargo is a no-op when fresh
            print("[workload] building libpl_bench.so ...", flush=True)
            _cargo(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--manifest-path",
                    str(plugin_root / "Cargo.toml"),
                ],
                build_env,
                "pipeline-bench plugin",
                self.target,
            )
        _cargo(
            ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
            build_env,
            "tomii-core",
            self.target,
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
        # Structurally-broken graph edits (unresolvable dependencies) hang;
        # the subprocess watchdog rejects them.  No --max-runtime: the runtime
        # polls max_runtime at 10 s granularity, so it added ~9 s of idle
        # shutdown to EVERY trial (pre-E8) without changing any measurement.
        cmd = build_command(
            str(self.binary),
            str(graph_path),
            str(self.dylib),
            max_frames=frames,
            exclude_frames=0,
            **cli_kwargs,
        )
        env = {**os.environ, "PIPELINE_BENCH_RESULT": str(result_file)}
        try:
            proc = subprocess.run(
                measured(cmd), env=env, capture_output=True, text=True,
                timeout=self.VERIFY_TIMEOUT_S,
                preexec_fn=_die_with_parent,
            )
        except subprocess.TimeoutExpired:
            return self._reject(
                f"verify run timeout after {self.VERIFY_TIMEOUT_S}s "
                "(5 frames; hung or broken graph edit)",
                t0,
            )
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
                timing=str(timing_file),
                report=str(tmp_dir / "report.json"),
                use_rdtsc=True,
                core_offset=1,
                **cli_kwargs,
            )
            try:
                proc = subprocess.run(
                    measured(cmd),
                    preexec_fn=_die_with_parent,
                    env=os.environ.copy(),
                    capture_output=True,
                    text=True,
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
                report=load_report(tmp_dir / "report.json"),
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
    KERNEL_LIB_DIR = _HERE / ".mimo-lib"
    #: Fixed NUMA1 sender core set (sender threads pin to 55,56 themselves);
    #: harness/LLM processes run on 32-52 so they never share these cores.
    SENDER_CPUS = "53-63"
    # Per-slot processing floor at 16x16 is ~48 ms; the sender pacing floor
    # for S slots is ceil(48000/S) µs per frame.
    SLOT_FLOOR_US = 48_000

    def __init__(self) -> None:
        super().__init__()
        self.root = REPO_ROOT / "bench" / "mimo-bench" / "tomii"
        self.agora_dir = Path("~/Agora").expanduser().resolve()
        self.target = TARGET_ROOT / self.name
        self.dylib = self.target / "release" / "libmimo_bench_tomii.so"
        self.binary = self.target / "release" / "main"
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
        # The C++ kernel libs (libdemod/libbeamfuncs/libfftfuncs) are built
        # out of tree and gitignored; build.rs looks in <crate>/lib.  Point the
        # linker and rpath at the harness-local copy instead of adding files
        # to bench/ (identical md5 to the E1/P0 worktrees' copies).
        if self.KERNEL_LIB_DIR.is_dir():
            build_env["RUSTFLAGS"] = (
                f"-L native={self.KERNEL_LIB_DIR} "
                f"-C link-arg=-Wl,-rpath,{self.KERNEL_LIB_DIR}"
            )
        if True:  # always: cargo is a no-op when fresh
            print("[workload] building libmimo_bench_tomii.so ...", flush=True)
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
                self.target,
            )
        _cargo(
            ["cargo", "build", "--release", "-p", "tomii-core", "--bin", "main"],
            build_env,
            "tomii-core",
            self.target,
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
                report=str(tmp_dir / "report.json"),
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

            stderr_path = tmp_dir / "tomii.stderr"
            stderr_fh = stderr_path.open("wb")
            tomii_proc = subprocess.Popen(
                measured(cmd), env=env, stdout=subprocess.DEVNULL, stderr=stderr_fh,
                preexec_fn=_die_with_parent,
            )
            # Receiver first; the sender starts SENDER_DELAY_S later.  A config
            # that crashes at startup (e.g. a panic) is detected during the
            # delay and never gets a sender.
            deadline = time.monotonic() + self.SENDER_DELAY_S
            while time.monotonic() < deadline and tomii_proc.poll() is None:
                time.sleep(0.2)
            sender_cmd = [
                "taskset",
                "-c",
                self.SENDER_CPUS,
                str(self.agora_dir / "build" / "sender"),
                "--num_threads=2",
                "--core_offset=55",
                f"--frame_duration={frame_duration}",
                "--enable_slow_start=0",
                "--inter_frame_delay=0",
                f"--conf_file={sender_cfg}",
            ]
            sender_proc = None
            if tomii_proc.poll() is None:
                sender_proc = subprocess.Popen(
                    sender_cmd,
                    preexec_fn=_die_with_parent,
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
                if sender_proc is not None:
                    sender_proc.kill()
                    sender_proc.wait()
                stderr_fh.close()

            if ret != 0:
                tail = stderr_path.read_text(errors="replace")[-200:].strip()
                return EvalResult(
                    verifier_ok=False,
                    ms_per_frame=None,
                    rejection_reason=(
                        "tomii hung past watchdog"
                        if ret == -1
                        else f"tomii exit {ret}: {tail}"
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
                report=load_report(tmp_dir / "report.json"),
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
