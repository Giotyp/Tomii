"""agent-eval configuration."""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
AGENT_EVAL = Path(__file__).resolve().parent

TASKFLOW_INCLUDE = Path(
    os.environ.get("TASKFLOW_ROOT", "")
)  # header-only install; set TASKFLOW_ROOT

# Golden output per task — one stream's worth (harness checks N×golden for N streams)
GOLDEN: dict[str, str] = {
    "task_1": (
        "Sensor-0: [41.50, 42.00, 42.50]\n"
        "Sensor-1: [41.50, 42.00, 42.50]\n"
        "Sensor-2: [41.50, 42.00, 42.50]\n"
        "Sensor-3: [41.50, 42.00, 42.50]\n"
    ),
}

# Minimum physically-possible latency (µs). Results below this are degenerate.
LATENCY_FLOOR_US: dict[str, float] = {
    "task_1": 5.0,
}

# Number of streams for the harness's independent validation run.
EVAL_STREAMS = 5
EVAL_EXCLUDE_STREAMS = 2  # warm-up streams excluded from latency measurement

# Held-out verification sizes (harness varies these to detect hardcoding).
# For task_1 these are max_streams values; output lines = 4 × (max_streams - exclude).
HELD_OUT_STREAM_COUNTS = [3, 5, 7]


@dataclass
class EvalConfig:
    model: str = "claude-sonnet-4-6"
    budget_usd: float = 1.00
    wall_timeout_s: int = 600
    results_dir: str = str(AGENT_EVAL / "results")

    # Q threshold: fraction of oracle_best_latency the agent must reach for Tier 2
    q_threshold: float = 0.80
