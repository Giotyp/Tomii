"""FrameworkAdapter: per-framework build/run/report commands."""
from __future__ import annotations
import json
import subprocess
import os
import re
from dataclasses import dataclass
from pathlib import Path

from config import TASKFLOW_INCLUDE, EVAL_STREAMS, EVAL_EXCLUDE_STREAMS


@dataclass
class RunResult:
    stdout: str
    stderr: str
    returncode: int
    latency_us: float | None  # None if not parseable


class FrameworkAdapter:
    name: str

    def scaffold_dir(self, task_id: str, tier: int) -> Path:
        raise NotImplementedError

    def build(self, workspace: Path) -> RunResult:
        raise NotImplementedError

    def run(self, workspace: Path, max_streams: int, exclude_streams: int, workers: int) -> RunResult:
        raise NotImplementedError

    def parse_latency(self, workspace: Path, run_result: RunResult) -> float | None:
        raise NotImplementedError

    def result_file(self, workspace: Path) -> Path:
        return workspace / "result.txt"


class TomiiAdapter(FrameworkAdapter):
    name = "tomii"

    def scaffold_dir(self, task_id: str, tier: int) -> Path:
        from config import AGENT_EVAL
        subdir = "tier_2" if tier == 2 else ""
        if subdir:
            return AGENT_EVAL / "scaffolds" / "tomii" / task_id / subdir
        return AGENT_EVAL / "scaffolds" / "tomii" / task_id

    def build(self, workspace: Path) -> RunResult:
        env = os.environ.copy()
        env["SCRIPT_DIR"] = str(workspace)
        result = subprocess.run(
            ["python", "run_bench.py", "--workers", "1",
             "--max-streams", "1", "--build-only"],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=300,
            env=env,
        )
        return RunResult(result.stdout, result.stderr, result.returncode, None)

    def run(self, workspace: Path, max_streams: int = EVAL_STREAMS,
            exclude_streams: int = EVAL_EXCLUDE_STREAMS, workers: int = 4) -> RunResult:
        # result.txt: plugin writes here (append mode) — must exist before run
        result_file = workspace / "result.txt"
        result_file.unlink(missing_ok=True)
        result_file.touch()
        (workspace / "out.txt").unlink(missing_ok=True)
        (workspace / "report.json").unlink(missing_ok=True)
        (workspace / "timing.txt").unlink(missing_ok=True)

        env = os.environ.copy()
        env["SCRIPT_DIR"] = str(workspace)
        result = subprocess.run(
            ["python", "run_bench.py",
             "--max-streams", str(max_streams),
             "--exclude-streams", str(exclude_streams),
             "--report", "report.json",
             "--timing", "timing.txt"],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
        )
        latency = self.parse_latency(workspace, result)
        return RunResult(result.stdout, result.stderr, result.returncode, latency)

    def parse_latency(self, workspace: Path, run_result: RunResult) -> float | None:
        report = workspace / "report.json"
        if report.exists():
            try:
                data = json.loads(report.read_text())
                return data.get("summary", {}).get("avg_latency_us")
            except Exception:
                pass
        timing = workspace / "timing.txt"
        if timing.exists():
            for line in timing.read_text().splitlines():
                m = re.search(r"avg_latency_us\s*=\s*([\d.]+)", line)
                if m:
                    return float(m.group(1))
        return None


class TaskflowAdapter(FrameworkAdapter):
    name = "taskflow"

    def scaffold_dir(self, task_id: str, tier: int) -> Path:
        from config import AGENT_EVAL
        subdir = "tier_2" if tier == 2 else ""
        if subdir:
            return AGENT_EVAL / "scaffolds" / "taskflow" / task_id / subdir
        return AGENT_EVAL / "scaffolds" / "taskflow" / task_id

    def build(self, workspace: Path) -> RunResult:
        build_dir = workspace / "build"
        build_dir.mkdir(exist_ok=True)
        cmake_result = subprocess.run(
            ["cmake", "..",
             f"-DTASKFLOW_DIR={TASKFLOW_INCLUDE}",
             "-DCMAKE_BUILD_TYPE=Release"],
            cwd=build_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if cmake_result.returncode != 0:
            return RunResult(cmake_result.stdout, cmake_result.stderr,
                             cmake_result.returncode, None)
        make_result = subprocess.run(
            ["make", "-j4"],
            cwd=build_dir,
            capture_output=True,
            text=True,
            timeout=120,
        )
        return RunResult(make_result.stdout, make_result.stderr,
                         make_result.returncode, None)

    def run(self, workspace: Path, max_streams: int = EVAL_STREAMS,
            exclude_streams: int = EVAL_EXCLUDE_STREAMS, workers: int = 4) -> RunResult:
        result_file = workspace / "result.txt"
        result_file.unlink(missing_ok=True)

        binary = workspace / "build" / "sensor_pipeline"
        if not binary.exists():
            return RunResult("", f"Binary not found: {binary}", 1, None)

        result = subprocess.run(
            [str(binary), str(workers), str(max_streams), str(exclude_streams)],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=120,
        )
        latency = self.parse_latency(workspace, result)
        if latency is not None:
            (workspace / "timing.txt").write_text(f"avg_latency_us = {latency:.3f}\n")
        return RunResult(result.stdout, result.stderr, result.returncode, latency)

    def parse_latency(self, workspace: Path, run_result: RunResult) -> float | None:
        for line in (run_result.stdout + run_result.stderr).splitlines():
            m = re.search(r"avg_latency_us\s*=\s*([\d.]+)", line)
            if m:
                return float(m.group(1))
        return None


ADAPTERS: dict[str, FrameworkAdapter] = {
    "tomii": TomiiAdapter(),
    "taskflow": TaskflowAdapter(),
}
