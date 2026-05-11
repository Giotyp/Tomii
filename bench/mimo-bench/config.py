from dataclasses import dataclass, field
import os

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@dataclass
class MimoOptConfig:
    # ── Fixed (never tuned) ──────────────────────────────────────────────────
    workers: int = 26
    system_threads: int = 8
    receiver_threads: int = 4

    # ── Fixed knobs ──────────────────────────────────────────────────────────
    slots: int = 10             # fixed
    slot_priority: bool = True  # fixed

    # ── Tunable knobs (defaults match current run_mimo.sh) ───────────────────
    batching_size: int = 32         # range 1–256
    batching_limit: int = 10        # range 0–100 (µs)
    sched_flush_threshold: int = 32 # range 1–256
    spin_iterations: int = 32       # range 0–1024
    spin_wait_spin_iters: int = 64  # range 0–512
    spin_wait_yield_iters: int = 256 # range 0–1024
    spin_wait_park_ns: int = 100    # range 0–10000 (ns)

    # ── Knob metadata (for prompt generation) ────────────────────────────────
    KNOB_RANGES: dict = field(default_factory=lambda: {
        "batching_size":        {"min": 1,  "max": 256,   "type": "int"},
        "batching_limit":       {"min": 0,  "max": 100,   "type": "int"},
        "sched_flush_threshold":{"min": 16, "max": 256,   "type": "int"},
        "spin_iterations":      {"min": 0,  "max": 1024,  "type": "int"},
        "spin_wait_spin_iters": {"min": 0,  "max": 512,   "type": "int"},
        "spin_wait_yield_iters":{"min": 0,  "max": 1024,  "type": "int"},
        "spin_wait_park_ns":    {"min": 0,  "max": 10000, "type": "int"},
    })

    # ── Harness settings ─────────────────────────────────────────────────────
    max_streams: int = 500          # streams per benchmark run (skill_runner uses this)
    run_script: str = os.path.join(
        REPO_ROOT, "examples/mimolib/scripts/run_mimo.sh"
    )
    reference_report: str = os.path.join(
        REPO_ROOT, "examples/mimolib/report.json"
    )
    output_dir: str = os.path.join(REPO_ROOT, "mimo-bench/results")
    model: str = "claude-opus-4-6"
    budget_usd: float = 1.00
    run_timeout_s: int = 720
