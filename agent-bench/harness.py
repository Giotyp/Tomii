#!/usr/bin/env python3
"""Agent benchmark harness.

Runs `claude -p` non-interactively to measure AI-driven implementation and
optimization difficulty across SynStream (Python + Rust) vs Taskflow (C++).

Usage:
    python agent-bench/harness.py --experiment implement_synstream --trials 5
    python agent-bench/harness.py --experiment implement_taskflow  --trials 5
    python agent-bench/harness.py --dry-run --experiment implement_synstream
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

HERE      = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
sys.path.insert(0, str(REPO_ROOT))

from config   import EXPERIMENTS, ExperimentConfig  # noqa: E402
from metrics  import (                               # noqa: E402
    collect_iteration_metrics,
    aggregate_trial_metrics,
    verify_synstream_correctness,
    verify_taskflow_correctness,
)


# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------

def _load_prompt(cfg: ExperimentConfig, workspace: Path) -> str:
    text = (HERE / "prompts" / cfg.prompt_file).read_text()
    return (
        text
        .replace("<REPO_ROOT>", str(REPO_ROOT))
        .replace("<WORKSPACE>", str(workspace))
    )


def _build_prompt(base_prompt: str, iteration: int, prev: Optional[Dict[str, Any]]) -> str:
    if iteration == 0 or prev is None:
        return base_prompt
    feedback_parts = [base_prompt, "\n\n---\n\n## Feedback from previous attempt\n"]
    if not prev.get("build_success"):
        feedback_parts.append("**Build failed.** Check the build log and fix compilation errors.\n")
    elif not prev.get("run_success"):
        feedback_parts.append("**Runtime failed.** The binary crashed or timed out.\n")
    elif not prev.get("correct"):
        feedback_parts.append("**Correctness check failed.** The output does not match expected.\n")
    else:
        feedback_parts.append("**Build and run succeeded!** Now optimize for lower latency.\n")
        if prev.get("avg_latency_us"):
            feedback_parts.append(f"Current avg latency: {prev['avg_latency_us']:.1f} µs\n")
        if prev.get("bottleneck_hints"):
            feedback_parts.append(f"Bottleneck hints: {json.dumps(prev['bottleneck_hints'], indent=2)}\n")
        if prev.get("critical_path"):
            cp = prev["critical_path"]
            # critical_path may be a list (legacy) or a dict (native synstream report)
            if isinstance(cp, list):
                cp = cp[:5]
            feedback_parts.append(f"Critical path: {json.dumps(cp, indent=2)}\n")
    return "".join(feedback_parts)


# ---------------------------------------------------------------------------
# Workspace management
# ---------------------------------------------------------------------------

def _setup_workspace(cfg: ExperimentConfig, trial_dir: Path) -> Path:
    workspace = trial_dir / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    if cfg.task == "optimize":
        # Seed workspace from seeds/ (no optimization hints) when available,
        # otherwise fall back to references/ for backward compatibility.
        if cfg.seed_dir:
            seed = HERE / "seeds" / cfg.seed_dir
        else:
            seed = HERE / "references" / cfg.reference_dir
        if seed.exists():
            shutil.copytree(seed, workspace, dirs_exist_ok=True)

    # For taskflow experiments, symlink the taskflow-lib headers into the workspace
    # so the Makefile's -Itaskflow-lib resolves correctly from any working directory.
    if cfg.framework == "taskflow":
        taskflow_lib_src = REPO_ROOT / "taskflow-bench" / "taskflow-lib"
        taskflow_lib_dst = workspace / "taskflow-lib"
        if taskflow_lib_src.exists() and not taskflow_lib_dst.exists():
            taskflow_lib_dst.symlink_to(taskflow_lib_src)

    # Fix any SynStream Cargo.toml copied into the workspace:
    #   1. Add [workspace] so cargo doesn't try to pull it into the root repo
    #      workspace (git worktrees share git root with the main repo, so cargo
    #      traversal reaches /home/.../SynStream/Cargo.toml which rejects it).
    #   2. Replace relative synstream-types path with absolute path so it
    #      resolves correctly from any workspace location.
    cargo_toml = workspace / "Cargo.toml"
    if cargo_toml.exists():
        text = cargo_toml.read_text()
        synstream_types_abs = REPO_ROOT / "synstream-types"
        for pat in ('"../../synstream-types"', '"../../../synstream-types"'):
            if pat in text:
                text = text.replace(pat, f'"{synstream_types_abs}"')
        synstream_macro_abs = REPO_ROOT / "synstream-macro"
        for pat in ('"../../synstream-macro"', '"../../../synstream-macro"'):
            if pat in text:
                text = text.replace(pat, f'"{synstream_macro_abs}"')
        if "[workspace]" not in text:
            text = "[workspace]\n\n" + text
        cargo_toml.write_text(text)

    # Init git so we can capture diffs
    subprocess.run(["git", "init"], cwd=workspace, capture_output=True)
    subprocess.run(["git", "add", "-A"], cwd=workspace, capture_output=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "initial"],
        cwd=workspace,
        capture_output=True,
        env={**os.environ, "GIT_AUTHOR_NAME": "harness", "GIT_AUTHOR_EMAIL": "h@h",
             "GIT_COMMITTER_NAME": "harness", "GIT_COMMITTER_EMAIL": "h@h"},
    )
    return workspace


def _save_diff(workspace: Path, iter_dir: Path) -> None:
    result = subprocess.run(
        ["git", "diff", "HEAD"],
        cwd=workspace,
        capture_output=True,
        text=True,
    )
    (iter_dir / "workspace.patch").write_text(result.stdout)


# ---------------------------------------------------------------------------
# Build and run
# ---------------------------------------------------------------------------

def _get_env_with_exports() -> Dict[str, str]:
    """Return a clean environment for benchmark subprocesses.

    The Python synstream API handles WRAP_PATH / REG_PATH / FUNC_PATH
    internally via app.build(wrap_path=..., reg_path=...) — we must NOT
    pre-populate those from mimolib's export.sh because that would override
    the per-experiment wrappers the agent (or reference) specifies.
    """
    env = {**os.environ}
    # Strip any stale wrapper/func env vars so the Python API can set them cleanly.
    for var in ("WRAP_PATH", "REG_PATH", "FUNC_PATH"):
        env.pop(var, None)
    return env


def _build_synstream(workspace: Path, _cfg: ExperimentConfig, dylib_cache: Dict) -> Tuple[int, str]:
    """Build the SynStream plugin. Returns (exit_code, log)."""
    cargo_toml = workspace / "Cargo.toml"
    if not cargo_toml.exists():
        return 1, "No Cargo.toml found in workspace — agent has not created the plugin manifest yet."

    # If run_wavefront.py already exists, delegate build+run to it.
    # A dummy-graph build would overwrite wrappers.rs/reg.rs with incorrect
    # function registrations, causing "Function X not found" panics at runtime.
    runner = workspace / "run_wavefront.py"
    if runner.exists():
        return 0, "run_wavefront.py present; build delegated to run step."

    env = _get_env_with_exports()

    # Fallback for scaffolds that have Cargo.toml but no run_wavefront.py yet:
    # attempt a bare cargo build to surface compile errors early.
    result = subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(cargo_toml)],
        cwd=workspace, capture_output=True, text=True, env=env, timeout=180,
    )
    log = result.stdout + result.stderr
    return result.returncode, log


def _run_synstream(workspace: Path, cfg: ExperimentConfig, iter_dir: Path, dylib_cache: Dict) -> Tuple[int, str]:
    """Run the SynStream benchmark. Returns (exit_code, log)."""
    runner = workspace / "run_wavefront.py"
    if not runner.exists():
        return 1, "No run_wavefront.py found in workspace."

    report_path = iter_dir / "report.json"
    timing_path = iter_dir / "timing.csv"
    dylib = dylib_cache.get("dylib", "")

    env = _get_env_with_exports()

    # Build base command
    base_cmd = [
        sys.executable, str(runner),
        "--n", str(cfg.n),
        "--workers", str(cfg.workers),
        "--iterations", str(cfg.iterations),
        "--report", str(report_path),
        "--timing", str(timing_path),
    ]

    # Try with --dylib flag first; agent's script may handle build internally
    if dylib:
        cmd = base_cmd + ["--dylib", dylib]
    else:
        cmd = base_cmd

    try:
        result = subprocess.run(cmd, cwd=workspace, capture_output=True, text=True, env=env, timeout=900)
        log = result.stdout + result.stderr
        # Retry without --dylib if that flag caused an error
        if result.returncode != 0 and dylib and "unrecognized" in log.lower():
            result = subprocess.run(base_cmd, cwd=workspace, capture_output=True, text=True, env=env, timeout=900)
            log = result.stdout + result.stderr
    except subprocess.TimeoutExpired as e:
        log = (e.stdout or b"").decode(errors="replace") + (e.stderr or b"").decode(errors="replace")
        log += "\n[harness] run_wavefront.py timed out after 900s"
        (iter_dir / "run.log").write_text(log)
        return 1, log

    (iter_dir / "run.log").write_text(log)

    # Copy report if agent wrote it to workspace root
    for candidate in [workspace / "report.json", workspace / "results" / "report.json"]:
        if candidate.exists() and not report_path.exists():
            shutil.copy(candidate, report_path)

    return result.returncode, log


def _build_taskflow(workspace: Path, _cfg: ExperimentConfig) -> Tuple[int, str]:
    """Build Taskflow binary. Try make, fall back to g++ on any .cpp files."""
    # If run_wavefront.py already exists, delegate build+run to it.
    runner = workspace / "run_wavefront.py"
    if runner.exists():
        return 0, "run_wavefront.py present; build delegated to run step."

    taskflow_include = REPO_ROOT / "taskflow-bench" / "taskflow-lib"

    if (workspace / "Makefile").exists():
        result = subprocess.run(
            ["make"],
            cwd=workspace,
            capture_output=True,
            text=True,
            timeout=120,
        )
    else:
        cpp_files = list(workspace.glob("*.cpp"))
        if not cpp_files:
            return 1, "No .cpp files found in workspace — agent has not created source files yet."
        cmd = [
            "g++", "-O3", "-std=c++17", f"-I{taskflow_include}",
            "-lpthread", "-o", "wavefront",
        ] + [str(f) for f in cpp_files]
        result = subprocess.run(cmd, cwd=workspace, capture_output=True, text=True, timeout=120)

    log = result.stdout + result.stderr
    (workspace / "build.log").write_text(log)
    return result.returncode, log


def _run_taskflow(workspace: Path, cfg: ExperimentConfig, iter_dir: Path) -> Tuple[int, str]:
    """Run Taskflow benchmark. Delegates to run_wavefront.py if present, else binary."""
    report_path = iter_dir / "report.json"

    # Delegate to Python wrapper if the agent created one
    runner = workspace / "run_wavefront.py"
    if runner.exists():
        env = _get_env_with_exports()
        base_cmd = [
            sys.executable, str(runner),
            "--n",          str(cfg.n),
            "--workers",    str(cfg.workers),
            "--iterations", str(cfg.iterations),
            "--report",     str(report_path),
        ]
        try:
            result = subprocess.run(base_cmd, cwd=workspace, capture_output=True, text=True,
                                    env=env, timeout=900)
            log = result.stdout + result.stderr
            if result.returncode != 0 and "unrecognized" in log.lower():
                result = subprocess.run(
                    base_cmd[:-2],  # drop --report <path>
                    cwd=workspace, capture_output=True, text=True, env=env, timeout=900,
                )
                log = result.stdout + result.stderr
        except subprocess.TimeoutExpired as e:
            log = (e.stdout or b"").decode(errors="replace") + (e.stderr or b"").decode(errors="replace")
            log += "\n[harness] run_wavefront.py timed out after 900s"
            (iter_dir / "run.log").write_text(log)
            return 1, log
        (iter_dir / "run.log").write_text(log)
        for candidate in [workspace / "report.json"]:
            if candidate.exists() and not report_path.exists():
                shutil.copy(candidate, report_path)
        return result.returncode, log

    # Binary path — find executable (agent may have named it differently)
    candidates = ["wavefront", "wavefront_bench", "main"]
    exe = None
    for name in candidates:
        p = workspace / name
        if p.exists() and os.access(p, os.X_OK):
            exe = p
            break
    if exe is None:
        for p in workspace.iterdir():
            if p.is_file() and os.access(p, os.X_OK) and p.suffix == "":
                exe = p
                break
    if exe is None:
        return 1, "No executable found in workspace."

    base_cmd = [
        str(exe),
        "--n",          str(cfg.n),
        "--workers",    str(cfg.workers),
        "--iterations", str(cfg.iterations),
        "--warmup",     "2",
        "--report",     str(report_path),
    ]

    # Try with --pin first
    result = subprocess.run(
        base_cmd + ["--pin"],
        cwd=workspace, capture_output=True, text=True, timeout=120,
    )
    log = result.stdout + result.stderr

    # Retry without --pin if agent didn't implement that flag
    if result.returncode != 0 and ("unrecognized" in log.lower() or "unknown" in log.lower()):
        result = subprocess.run(base_cmd, cwd=workspace, capture_output=True, text=True, timeout=120)
        log = result.stdout + result.stderr

    (iter_dir / "run.log").write_text(log)
    for candidate in [workspace / "report.json"]:
        if candidate.exists() and not report_path.exists():
            shutil.copy(candidate, report_path)
    return result.returncode, log


# ---------------------------------------------------------------------------
# Post-run harness validation
# ---------------------------------------------------------------------------

def _harness_checks(
    workspace: Path,
    cfg: ExperimentConfig,
    iter_dir: Path,
    run_exit: int,
    correct: bool,
) -> Tuple[bool, Dict[str, Any]]:
    """Apply sanity checks after a run. Returns (correct, extra_metrics)."""
    extra: Dict[str, Any] = {}
    report_path = iter_dir / "report.json"

    if run_exit == 0 and report_path.exists():
        try:
            report = json.loads(report_path.read_text())
        except Exception:
            report = {}

        # Check 1: N mismatch — agent used a different problem size
        reported_n = report.get("config", {}).get("n")
        if reported_n is not None and reported_n != cfg.n:
            correct = False
            with open(iter_dir / "run.log", "a") as f:
                f.write(f"\nHARNESS_FAIL: n_mismatch (expected {cfg.n}, got {reported_n})\n")

        # Check 2: Suspiciously low latency (computation hoisted out of hot path)
        avg_us = report.get("summary", {}).get("avg_latency_us", 0)
        serial_floor_us = cfg.n * cfg.n * 0.5e-3  # 0.5 ns/cell serial lower bound
        if avg_us > 0 and avg_us < serial_floor_us / 10:
            correct = False
            extra["suspicious_latency"] = True
            with open(iter_dir / "run.log", "a") as f:
                f.write(
                    f"\nHARNESS_FAIL: suspicious_latency "
                    f"({avg_us:.1f} µs < threshold {serial_floor_us / 10:.1f} µs)\n"
                )

    # Check 3: Single-task collapse — all work in one emplace(), no dependencies
    if cfg.framework == "taskflow":
        for cpp_file in workspace.glob("*.cpp"):
            try:
                content = cpp_file.read_text()
                if (
                    "taskflow.emplace" in content
                    and "for_each_index" not in content
                    and "precede" not in content
                ):
                    extra["single_task_collapse"] = True
                    break
            except Exception:
                pass

    return correct, extra


# ---------------------------------------------------------------------------
# Baseline helpers
# ---------------------------------------------------------------------------

def _read_baseline_latency(trial_dir: Path) -> Optional[float]:
    """Return avg_latency_us from the pre-run baseline, or None if unavailable."""
    baseline_report = trial_dir / "baseline" / "report.json"
    if not baseline_report.exists():
        return None
    try:
        data = json.loads(baseline_report.read_text())
        return data.get("summary", {}).get("avg_latency_us")
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Per-iteration execution helper
# ---------------------------------------------------------------------------

def _execute_iteration(
    cfg: ExperimentConfig,
    workspace: Path,
    trial_dir: Path,
    iter_label: str,
    prompt: str,
    dry_run: bool,
    dylib_cache: Dict,
) -> Tuple[Dict[str, Any], Path]:
    """Invoke Claude, build, run, collect metrics for one iteration.

    Returns (metrics_dict, iter_dir).
    """
    iter_dir = trial_dir / iter_label
    iter_dir.mkdir(exist_ok=True)

    t0 = time.monotonic()
    _invoke_claude(prompt, workspace, cfg, iter_dir, dry_run=dry_run)
    wall_time = time.monotonic() - t0

    _save_diff(workspace, iter_dir)
    subprocess.run(["git", "add", "-A"], cwd=workspace, capture_output=True)
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", iter_label],
        cwd=workspace, capture_output=True,
        env={**os.environ, "GIT_AUTHOR_NAME": "agent", "GIT_AUTHOR_EMAIL": "a@a",
             "GIT_COMMITTER_NAME": "agent", "GIT_COMMITTER_EMAIL": "a@a"},
    )

    print("  Building...")
    if cfg.framework == "synstream":
        build_exit, build_log = _build_synstream(workspace, cfg, dylib_cache)
    else:
        build_exit, build_log = _build_taskflow(workspace, cfg)
    (iter_dir / "build.log").write_text(build_log)
    print(f"  Build: {'OK' if build_exit == 0 else 'FAIL'}")

    run_exit = 1
    correct = False
    if build_exit == 0:
        print("  Running...")
        if cfg.framework == "synstream":
            run_exit, _ = _run_synstream(workspace, cfg, iter_dir, dylib_cache)
            correct = verify_synstream_correctness(iter_dir)
        else:
            run_exit, _ = _run_taskflow(workspace, cfg, iter_dir)
            correct = verify_taskflow_correctness(iter_dir)
        correct, extra_flags = _harness_checks(workspace, cfg, iter_dir, run_exit, correct)
        print(f"  Run: {'OK' if run_exit == 0 else 'FAIL'} | correct={correct}")
    else:
        extra_flags = {}

    m = collect_iteration_metrics(
        iter_dir, cfg.framework, build_exit, run_exit, wall_time, correct
    )
    m.update(extra_flags)
    (iter_dir / "metrics.json").write_text(json.dumps(m, indent=2))
    return m, iter_dir


# ---------------------------------------------------------------------------
# Claude invocation
# ---------------------------------------------------------------------------

def _invoke_claude(
    prompt: str,
    workspace: Path,
    cfg: ExperimentConfig,
    iter_dir: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:
    (iter_dir / "prompt.md").write_text(prompt)

    if dry_run:
        print("  [dry-run] skipping claude invocation")
        (iter_dir / "response.json").write_text(json.dumps({
            "result": "dry_run",
            "usage": {"input_tokens": 0, "output_tokens": 0},
        }))
        return {}

    cmd = [
        "claude", "-p", prompt,
        "--dangerously-skip-permissions",
        "--output-format",    "json",
        "--max-budget-usd",   str(cfg.max_budget_usd),
        "--add-dir",          str(workspace),
        "--add-dir",          str(REPO_ROOT),
    ]

    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(workspace),
            capture_output=True,
            stdin=subprocess.DEVNULL,
            text=True,
            timeout=cfg.timeout_s,
        )
        response_text = result.stdout or result.stderr
        ret = {"wall_time_s": time.monotonic() - t0}
    except subprocess.TimeoutExpired as e:
        print(f"  [timeout] Claude exceeded {cfg.timeout_s}s — recording partial iteration.")
        response_text = (e.stdout or b"").decode(errors="replace")
        (iter_dir / "response.json").write_text(json.dumps({
            "result": "timeout",
            "usage": {"input_tokens": 0, "output_tokens": 0},
            "partial_output": response_text[:500],
        }))
        ret = {"wall_time_s": time.monotonic() - t0, "timed_out": True}
    finally:
        # claude -p modifies terminal settings (disables echo); restore them
        # so the calling shell remains usable after each invocation.
        subprocess.run(["stty", "sane"], stderr=subprocess.DEVNULL)

    if "timed_out" not in ret:
        (iter_dir / "response.json").write_text(response_text)
    return ret


# ---------------------------------------------------------------------------
# Main trial loop
# ---------------------------------------------------------------------------

def run_trial(
    cfg: ExperimentConfig,
    trial_idx: int,
    results_root: Path,
    dry_run: bool = False,
) -> Dict[str, Any]:
    trial_dir = results_root / f"{cfg.name}_trial{trial_idx}"
    trial_dir.mkdir(parents=True, exist_ok=True)

    # Save experiment config
    (trial_dir / "metadata.json").write_text(json.dumps({
        "experiment": cfg.name,
        "framework":  cfg.framework,
        "task":       cfg.task,
        "trial":      trial_idx,
        "n":          cfg.n,
        "workers":    cfg.workers,
    }, indent=2))

    workspace = _setup_workspace(cfg, trial_dir)
    base_prompt = _load_prompt(cfg, workspace)

    dylib_cache: Dict = {}
    iter_metrics: List[Dict[str, Any]] = []
    prev_metrics: Optional[Dict] = None

    # For optimize experiments, pre-run the baseline so report.json exists in
    # the workspace when Claude is invoked on iteration 0.
    if cfg.task == "optimize" and not dry_run:
        print("  Pre-running baseline to generate report.json...")
        _build_synstream(workspace, cfg, dylib_cache) if cfg.framework == "synstream" else None
        baseline_dir = trial_dir / "baseline"
        baseline_dir.mkdir(exist_ok=True)
        if cfg.framework == "synstream":
            _run_synstream(workspace, cfg, baseline_dir, dylib_cache)
        elif cfg.framework == "taskflow":
            _build_taskflow(workspace, cfg)
            _run_taskflow(workspace, cfg, baseline_dir)
        baseline_report = baseline_dir / "report.json"
        if baseline_report.exists():
            shutil.copy(baseline_report, workspace / "report.json")

    if cfg.task in ("implement", "optimize"):
        # ----------------------------------------------------------------
        # Standard single-phase loop
        # ----------------------------------------------------------------
        for iteration in range(cfg.max_iterations):
            print(f"\n--- Trial {trial_idx} | Iteration {iteration} ---")
            prompt = _build_prompt(base_prompt, iteration, prev_metrics)
            m, _ = _execute_iteration(
                cfg, workspace, trial_dir, f"iter_{iteration}",
                prompt, dry_run, dylib_cache,
            )
            iter_metrics.append(m)
            prev_metrics = m
            if m.get("correct") and cfg.task == "implement":
                print("  Implementation correct — stopping early.")
                break

        baseline_latency = _read_baseline_latency(trial_dir)
        summary = aggregate_trial_metrics(iter_metrics, baseline_latency_us=baseline_latency)

    else:
        # ----------------------------------------------------------------
        # Pipeline: implement until correct, then optimize same workspace
        # ----------------------------------------------------------------
        implement_metrics: List[Dict[str, Any]] = []
        optimize_metrics:  List[Dict[str, Any]] = []
        implement_succeeded = False

        # Phase 1 — implement
        for iteration in range(cfg.max_iterations):
            print(f"\n--- Trial {trial_idx} | Implement {iteration} ---")
            prompt = _build_prompt(base_prompt, iteration, prev_metrics)
            m, iter_dir = _execute_iteration(
                cfg, workspace, trial_dir, f"impl_{iteration}",
                prompt, dry_run, dylib_cache,
            )
            implement_metrics.append(m)
            prev_metrics = m
            if m.get("correct"):
                implement_succeeded = True
                # Ensure report.json sits in workspace root for optimize prompt
                report_src = iter_dir / "report.json"
                if report_src.exists():
                    shutil.copy(report_src, workspace / "report.json")
                print("  Implementation correct — starting optimize phase.")
                break

        # Phase 2 — optimize (only when implement produced a correct solution)
        if implement_succeeded and cfg.optimize_prompt_file:
            opt_base = (
                (HERE / "prompts" / cfg.optimize_prompt_file).read_text()
                .replace("<REPO_ROOT>", str(REPO_ROOT))
                .replace("<WORKSPACE>", str(workspace))
            )
            prev_metrics = None  # fresh feedback context
            for opt_iter in range(cfg.max_optimize_iters):
                print(f"\n--- Trial {trial_idx} | Optimize {opt_iter} ---")
                prompt = _build_prompt(opt_base, opt_iter, prev_metrics)
                m, _ = _execute_iteration(
                    cfg, workspace, trial_dir, f"opt_{opt_iter}",
                    prompt, dry_run, dylib_cache,
                )
                optimize_metrics.append(m)
                prev_metrics = m

        impl_summary = aggregate_trial_metrics(implement_metrics)
        impl_baseline = impl_summary.get("best_latency_us")
        summary = {
            "implement": impl_summary,
            "optimize":  aggregate_trial_metrics(optimize_metrics, baseline_latency_us=impl_baseline) if optimize_metrics else None,
        }

    (trial_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"\nTrial {trial_idx} summary: {json.dumps(summary, indent=2)}")
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Agent benchmark harness")
    p.add_argument("--experiment", required=True,
                   choices=[e.name for e in EXPERIMENTS],
                   help="Which experiment to run")
    p.add_argument("--trials",   type=int, default=1, help="Number of independent trials")
    p.add_argument("--results",  type=Path, default=HERE / "results",
                   help="Root directory for results")
    p.add_argument("--dry-run",  action="store_true",
                   help="Skip claude invocation; useful for testing the harness")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    args.results = args.results.resolve()  # ensure absolute so workspace paths don't double
    cfg  = next(e for e in EXPERIMENTS if e.name == args.experiment)
    args.results.mkdir(parents=True, exist_ok=True)

    all_summaries = []
    for trial in range(args.trials):
        summary = run_trial(cfg, trial, args.results, dry_run=args.dry_run)
        all_summaries.append(summary)

    # Cross-trial aggregate
    agg_path = args.results / f"{cfg.name}_aggregate.json"
    agg_path.write_text(json.dumps({
        "experiment":  cfg.name,
        "trials":      args.trials,
        "per_trial":   all_summaries,
    }, indent=2))
    print(f"\nResults written to {args.results}")


if __name__ == "__main__":
    main()
