"""Experiment configurations for the agent benchmark."""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
AGENT_BENCH = Path(__file__).resolve().parent

@dataclass
class ExperimentConfig:
    name: str                          # e.g. "implement_synstream"
    framework: str                     # "synstream" or "taskflow"
    task: str                          # "implement" or "optimize"
    prompt_file: str                   # relative to prompts/
    reference_dir: str                 # relative to references/
    max_iterations: int = 8
    max_budget_usd: float = 5.00
    timeout_s: int = 400
    n: int = 256                       # grid size
    workers: int = 4
    iterations: int = 5              # benchmark iterations


EXPERIMENTS = [
    ExperimentConfig(
        name="implement_synstream",
        framework="synstream",
        task="implement",
        prompt_file="implement_synstream.md",
        reference_dir="synstream",
        timeout_s=600,
    ),
    ExperimentConfig(
        name="implement_taskflow",
        framework="taskflow",
        task="implement",
        prompt_file="implement_taskflow.md",
        reference_dir="taskflow",
        timeout_s=400,
    ),
    ExperimentConfig(
        name="optimize_synstream",
        framework="synstream",
        task="optimize",
        prompt_file="optimize_synstream.md",
        reference_dir="synstream",
        max_iterations=5,
        timeout_s=300,   # read + explore + edit only; harness does build+run
    ),
    ExperimentConfig(
        name="optimize_taskflow",
        framework="taskflow",
        task="optimize",
        prompt_file="optimize_taskflow.md",
        reference_dir="taskflow",
        max_iterations=5,
        timeout_s=300,   # read + explore + edit only; harness does build+run
    ),
]
