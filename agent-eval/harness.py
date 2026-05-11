#!/usr/bin/env python3
"""agent-eval harness — runs one trial and records metrics.

Usage:
    python harness.py --framework tomii --skills full --tier 1 --task task_1
    python harness.py --framework taskflow --skills bare --tier 2 --task task_1
    python harness.py --n-trials 20 --framework tomii --skills full --tier 2 --task task_1

Each trial:
  1. Copies scaffold into a fresh temp workspace.
  2. Installs SKILLS (if skills=full and framework=tomii).
  3. Copies verifier into workspace.
  4. Invokes `claude -p --dangerously-skip-permissions` with the task prompt.
  5. After claude exits, runs the harness-controlled verifier with held-out stream counts.
  6. Records: tokens_used, wall_seconds, verify_pass, latency_us, signal_usage, censored.
"""
from __future__ import annotations

import argparse
import json
import os
import re  # noqa: F401 — used in measure_oracle and extract_signal_usage
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SKILLS_DIR = REPO_ROOT / "SKILLS"
VERIFIERS_DIR = SCRIPT_DIR / "verifiers"
PROMPTS_DIR = SCRIPT_DIR / "prompts"

# Dedicated oracle implementations (outside the bench workspace to avoid dylib conflicts).
# Set TOMII_ORACLE_DIR to the sensor-pipeline oracle directory, or leave unset to fall
# back to the scaffold baseline.
TOMII_ORACLE_DIR = Path(os.environ.get("TOMII_ORACLE_DIR", ""))

# SKILL names that count as "used a SKILL" in signal extraction.
# Derived from SKILLS/*.md (excluding README) so it stays in sync automatically.
KNOWN_SKILLS: frozenset[str] = frozenset(
    p.stem for p in SKILLS_DIR.glob("*.md") if p.stem.lower() != "readme"
) if SKILLS_DIR.exists() else frozenset()

# Framework resource blurbs injected into prompts
FRAMEWORK_RESOURCES: dict[str, dict[str, str]] = {
    "tomii": {
        "full": (
            "Optimization loop:\n"
            "  1. python run_bench.py          # run benchmark → writes report.json\n"
            "  2. python -m tomii --explain    # read report.json → prints bottleneck + command\n"
            "  3. python -m tomii tune k=v ... # apply knob changes + re-run (no file edits needed)\n"
            "  4. Repeat until verifier passes\n"
            "\n"
            "Knob reference: python -m tomii tune --help\n"
            "Graph API: python -m tomii --schema\n"
            "All knobs: python -m tomii --list-knobs-json\n"
            "See TASK.md for constraints and the expected output.\n"
        ),
        "bare": (
            "Tomii Python API: `import tomii as tm`. Use `tm.Graph()`, `app.var()`, `app.node()`, "
            "`app.build()`, `app.run()`.\n"
            "Plugin functions are Rust with `#[tomii_export]`.\n"
            "See `TASK.md` for the full API contract."
        ),
    },
    "taskflow": {
        "bare": (
            "Taskflow C++ library (header-only). "
            "Set TASKFLOW_ROOT to your Taskflow install directory, "
            "or pass -DTASKFLOW_DIR=... to cmake.\n"
            "Key APIs: `tf::Executor(N)`, `tf::Taskflow`, `taskflow.for_each_index(...)`, "
            "`taskflow.emplace(...)`, task dependencies via `.succeed()` / `.precede()`.\n"
            "Build: `cmake .. -DCMAKE_BUILD_TYPE=Release && make -j$(nproc)`\n"
            "Docs: https://taskflow.github.io/taskflow/"
        ),
    },
}

# Held-out stream counts — harness varies these to detect hardcoded output
HELD_OUT_COUNTS = [(3, 1), (5, 2), (7, 3)]  # (total_streams, exclude_streams)


# ── Data types ────────────────────────────────────────────────────────────────

@dataclass
class TrialResult:
    framework: str
    skills: str
    tier: int
    task_id: str
    trial_idx: int
    # Core metrics
    tokens_input: int = 0
    tokens_output: int = 0
    wall_seconds: float = 0.0
    # Quality
    verify_pass: bool = False
    latency_us: float | None = None
    reached_q: bool = False          # only meaningful for tier 2
    # Signal usage (Tomii-only)
    signal_report_reads: int = 0     # times agent read report.json
    signal_knobs_calls: int = 0      # times agent called --list-knobs
    signal_skill_reads: int = 0      # times agent invoked a SKILL (Read or Skill tool)
    # Status
    censored: bool = False           # True if agent timed out / hit budget
    error: str = ""

    @property
    def tokens_total(self) -> int:
        return self.tokens_input + self.tokens_output


# ── Signal extraction ─────────────────────────────────────────────────────────

def _parse_events(response_json: str) -> list[dict]:
    """Parse claude --output-format json --verbose output into a list of events.

    Handles both a JSON array and newline-delimited JSON.
    """
    text = response_json.strip()
    if not text:
        return []
    # Try as a single JSON array first
    if text.startswith("["):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
    # Try newline-delimited JSON
    events = []
    for line in text.splitlines():
        line = line.strip().rstrip(",")
        if not line or line in ("[", "]"):
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return events


def extract_signal_usage(response_json: str) -> dict:
    """Count how many times the agent used introspection affordances."""
    counts = {"report_reads": 0, "knobs_calls": 0, "skill_reads": 0}
    try:
        events = _parse_events(response_json)
        for ev in events:
            # Look inside assistant messages for tool_use content
            msg = ev.get("message", {})
            content_blocks = msg.get("content", [])
            if isinstance(content_blocks, list):
                for block in content_blocks:
                    if block.get("type") != "tool_use":
                        continue
                    tool = block.get("name", "")
                    inp = json.dumps(block.get("input", {}))
                    if tool in ("Read", "ReadFile"):
                        if "report.json" in inp or "report.txt" in inp:
                            counts["report_reads"] += 1
                        if ".claude/skills" in inp:
                            counts["skill_reads"] += 1
                    if tool == "Skill":
                        skill_name = block.get("input", {}).get("skill", "")
                        if skill_name in KNOWN_SKILLS:
                            counts["skill_reads"] += 1
                    if tool in ("Bash",):
                        if "--list-knobs" in inp or "--schema" in inp:
                            counts["knobs_calls"] += 1
                        if "report.json" in inp:
                            counts["report_reads"] += 1
                        # v2 agent-native interface signals
                        if "tomii tune" in inp or "tomii --explain" in inp:
                            counts["knobs_calls"] += 1  # reuse counter; captures tune/explain usage
    except Exception:
        pass
    return counts


def extract_token_usage(response_json: str) -> tuple[int, int]:
    """Return (input_tokens, output_tokens) from claude --output-format json --verbose.

    Falls back to summing per-assistant-turn usage when the final 'result' event is
    missing (e.g. process killed at wall-timeout before emitting the result event).
    """
    events = _parse_events(response_json)
    # Primary: look for a 'result' event with aggregate usage
    for ev in events:
        if ev.get("type") != "result":
            continue
        usage = ev.get("usage", {})
        input_tok = (
            usage.get("input_tokens", 0)
            + usage.get("cache_creation_input_tokens", 0)
            + usage.get("cache_read_input_tokens", 0)
        )
        output_tok = usage.get("output_tokens", 0)
        if input_tok or output_tok:
            return input_tok, output_tok
    # Fallback: sum per-assistant-turn usage (present in stream-json verbose mode)
    max_cache = 0
    cum_output = 0
    for ev in events:
        if ev.get("type") != "assistant":
            continue
        usage = ev.get("message", {}).get("usage", {}) or ev.get("usage", {})
        c_read   = usage.get("cache_read_input_tokens", 0) or 0
        c_create = usage.get("cache_creation_input_tokens", 0) or 0
        out      = usage.get("output_tokens", 0) or 0
        max_cache = max(max_cache, c_read + c_create)
        cum_output += out
    return max_cache, cum_output


# ── Workspace setup ───────────────────────────────────────────────────────────

def setup_workspace(
    framework: str,
    skills: str,
    tier: int,
    task_id: str,
    tmp_dir: Path,
) -> Path:
    from adapter import ADAPTERS
    adapter = ADAPTERS[framework]

    scaffold = adapter.scaffold_dir(task_id, tier)
    workspace = tmp_dir / "workspace"
    shutil.copytree(scaffold, workspace,
                    ignore=shutil.ignore_patterns(
                        "build", "target", ".git", "__pycache__",
                        "report*.json", "report.json", "_oracle_report.json",
                        "result.txt", "timing.txt", "out.txt",
                    ))
    _absolutize_cargo_paths(workspace, scaffold)

    # Copy verifier into workspace
    verifier_src = VERIFIERS_DIR / f"{task_id}.py"
    shutil.copy(verifier_src, workspace / "verifier.py")

    # Copy reference golden (same dir as verifier for now)
    golden_src = SCRIPT_DIR / "references" / "tomii" / task_id / "result.golden.txt"
    if golden_src.exists():
        shutil.copy(golden_src, workspace / "result.golden.txt")

    # Install SKILLS for T-full condition
    if framework == "tomii" and skills == "full":
        install_skills(workspace)

    # Create result.txt (empty, so Tomii's append mode works)
    (workspace / "result.txt").touch()

    return workspace


def install_skills(workspace: Path) -> None:
    """Copy SKILLS/*.md into workspace/.claude/skills/<name>/SKILL.md."""
    if not SKILLS_DIR.exists():
        print(f"[WARN] SKILLS dir not found at {SKILLS_DIR}")
        return
    target = workspace / ".claude" / "skills"
    target.mkdir(parents=True, exist_ok=True)
    for skill_file in SKILLS_DIR.glob("*.md"):
        if skill_file.stem == "README":
            continue
        dest = target / skill_file.stem
        dest.mkdir(exist_ok=True)
        shutil.copy2(skill_file, dest / "SKILL.md")


def _absolutize_cargo_paths(workspace: Path, scaffold_src: Path) -> None:
    """Rewrite relative path deps in workspace Cargo.toml files to absolute paths.

    Relative paths are valid inside the repo but break when the scaffold is copied
    to a temp directory. Each dep path is resolved relative to its original location
    in scaffold_src and rewritten as an absolute path.
    """
    import re as _re
    for cargo_toml in workspace.rglob("Cargo.toml"):
        rel = cargo_toml.relative_to(workspace)
        src_cargo = scaffold_src / rel
        if not src_cargo.exists():
            continue
        text = cargo_toml.read_text()
        changed = False

        def _replace(m):
            nonlocal changed
            dep_path = m.group(1)
            if dep_path.startswith("."):
                abs_path = (src_cargo.parent / dep_path).resolve()
                changed = True
                return f'path = "{abs_path}"'
            return m.group(0)

        new_text = _re.sub(r'path\s*=\s*"([^"]+)"', _replace, text)
        if changed:
            cargo_toml.write_text(new_text)


# ── Prompt rendering ──────────────────────────────────────────────────────────

def render_prompt(tier: int, framework: str, skills: str, max_iters: int = 10) -> str:
    template = (PROMPTS_DIR / f"tier_{tier}.md").read_text()
    resources = FRAMEWORK_RESOURCES[framework].get(skills, FRAMEWORK_RESOURCES[framework]["bare"])
    return template.format(
        framework_name=framework.capitalize(),
        framework_resources=resources,
        max_iters=max_iters,
    )


# ── Claude invocation ─────────────────────────────────────────────────────────

def invoke_claude(
    prompt: str,
    workspace: Path,
    model: str,
    budget_usd: float,
    wall_timeout_s: int,
) -> tuple[str, float]:
    """Invoke claude agentic mode; return (raw_json_output, wall_seconds)."""
    cmd = [
        "claude", "-p",
        "--dangerously-skip-permissions",
        "--output-format", "stream-json",
        "--verbose",
        "--model", model,
        "--max-budget-usd", str(budget_usd),
        "--add-dir", str(workspace),
    ]
    t0 = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=wall_timeout_s,
            cwd=workspace,
        )
        wall = time.monotonic() - t0
        return result.stdout + result.stderr, wall
    except subprocess.TimeoutExpired as e:
        # Capture whatever partial output was written before the kill.
        # e.stdout/e.stderr may be bytes even when text=True was set.
        def _decode(v):
            if v is None:
                return ""
            return v if isinstance(v, str) else v.decode("utf-8", errors="replace")
        partial = _decode(e.stdout) + _decode(e.stderr)
        return partial, time.monotonic() - t0


# ── Harness validation run ────────────────────────────────────────────────────

def harness_verify(
    workspace: Path,
    adapter,
    oracle_best: float | None,
    q_threshold: float,
    tier: int,
) -> tuple[bool, float | None]:
    """Run the harness's independent verification (not the agent's own run).

    Returns (verify_pass, latency_us).
    Uses HELD_OUT_COUNTS to cycle through multiple stream counts — detects hardcoding.
    """
    import nproc_count
    workers = nproc_count.physical_cores()

    last_run_r = None
    for total_streams, exclude_streams in HELD_OUT_COUNTS:
        # adapter.run() also does unlink+touch — this is belt-and-suspenders
        result_file = workspace / "result.txt"
        result_file.unlink(missing_ok=True)
        result_file.touch()
        (workspace / "out.txt").unlink(missing_ok=True)

        try:
            run_r = adapter.run(
                workspace,
                max_streams=total_streams,
                exclude_streams=exclude_streams,
                workers=workers,
            )
        except subprocess.TimeoutExpired:
            return False, None
        last_run_r = run_r

        verify_r = subprocess.run(
            [sys.executable, "verifier.py",
             "--streams", str(total_streams),
             "--exclude", str(exclude_streams)],
            capture_output=True, text=True, cwd=workspace,
        )

        if "PASS" not in verify_r.stdout:
            return False, run_r.latency_us

    latency = last_run_r.latency_us if last_run_r else None
    return True, latency


# ── Orphan-process cleanup ────────────────────────────────────────────────────

def _kill_workspace_orphans(workspace: Path) -> None:
    """SIGTERM any processes that have files open inside the workspace directory.

    Tomii's main binary is spawned by agent-written run_bench.py and can outlive
    the Claude session. Without this, it writes stale data into result.txt while
    harness verification runs, and keeps files open during shutil.rmtree, causing
    an [Errno 39] crash that swallows the TrialResult.
    """
    import signal as _signal
    ws = str(workspace)
    try:
        lsof = subprocess.run(
            ["lsof", "-t", "+D", ws],
            capture_output=True, text=True, timeout=10,
        )
        our_pid = os.getpid()
        for pid_str in lsof.stdout.splitlines():
            try:
                pid = int(pid_str.strip())
                if pid == our_pid:
                    continue
                os.kill(pid, _signal.SIGTERM)
            except (ValueError, ProcessLookupError, PermissionError):
                pass
    except Exception:
        pass  # lsof unavailable or timed out — proceed without cleanup


# ── Trial runner ──────────────────────────────────────────────────────────────

def run_trial(
    framework: str,
    skills: str,
    tier: int,
    task_id: str,
    trial_idx: int,
    oracle_best: float | None,
    output_dir: Path,
    model: str = "claude-sonnet-4-6",
    budget_usd: float = 5.00,
    wall_timeout_s: int = 600,
    q_threshold: float = 0.80,
) -> TrialResult:
    from adapter import ADAPTERS
    adapter = ADAPTERS[framework]

    result = TrialResult(
        framework=framework,
        skills=skills,
        tier=tier,
        task_id=task_id,
        trial_idx=trial_idx,
    )

    trial_dir = output_dir / f"trial_{trial_idx:03d}"
    trial_dir.mkdir(parents=True, exist_ok=True)

    tmp = Path(tempfile.mkdtemp(prefix="agent_eval_"))
    try:
        try:
            workspace = setup_workspace(framework, skills, tier, task_id, tmp)
        except Exception as e:
            result.error = f"workspace setup failed: {e}"
            result.censored = True
            return result

        # Pre-build so the agent starts with a compiled dylib; only incremental rebuilds remain.
        print(f"  [trial {trial_idx}] pre-building...")
        prebuild_r = adapter.build(workspace)
        (trial_dir / "prebuild.log").write_text(prebuild_r.stdout + prebuild_r.stderr)
        if prebuild_r.returncode != 0:
            result.error = "pre-build failed"
            result.censored = True
            return result

        prompt = render_prompt(tier, framework, skills)
        (trial_dir / "prompt.md").write_text(prompt)

        print(f"  [trial {trial_idx}] invoking claude (budget=${budget_usd}, timeout={wall_timeout_s}s)")
        raw_out, wall = invoke_claude(prompt, workspace, model, budget_usd, wall_timeout_s)
        result.wall_seconds = wall

        (trial_dir / "claude_output.jsonl").write_text(raw_out)

        if not raw_out.strip():
            result.error = "claude timed out with no output; attempting workspace verify"
            result.censored = True
            # Fall through to build+verify — agent may have modified workspace
        else:
            # Extract token usage
            result.tokens_input, result.tokens_output = extract_token_usage(raw_out)

        # Extract signal usage (Tomii-only; others will have zeros)
        sig = extract_signal_usage(raw_out)
        result.signal_report_reads = sig["report_reads"]
        result.signal_knobs_calls = sig["knobs_calls"]
        result.signal_skill_reads = sig["skill_reads"]

        # Kill any orphaned processes still using the workspace (e.g. Tomii main binary
        # spawned by the agent and left running after Claude's budget was exhausted).
        _kill_workspace_orphans(workspace)

        try:
            # Build (harness-controlled, fresh build to check agent's code compiles)
            print(f"  [trial {trial_idx}] building...")
            build_r = adapter.build(workspace)
            (trial_dir / "build.log").write_text(build_r.stdout + build_r.stderr)
            if build_r.returncode != 0:
                result.error = "build failed"
                # Still record — agent failed to produce compilable code
                return result

            # Harness-controlled verification with held-out stream counts
            print(f"  [trial {trial_idx}] verifying...")
            pass_, latency = harness_verify(workspace, adapter, oracle_best, q_threshold, tier)
            result.verify_pass = pass_
            result.latency_us = latency

            if tier == 2 and oracle_best is not None and latency is not None:
                # Q bar = oracle_best / q_threshold (e.g. 0.80 → within 25% of oracle).
                # An agent matching the oracle passes; one up to 25% slower still passes.
                result.reached_q = latency <= oracle_best / q_threshold

            lat_str = f" latency={latency:.1f}µs" if latency is not None else ""
            print(f"  [trial {trial_idx}] {'PASS' if pass_ else 'FAIL'}{lat_str} tokens={result.tokens_total}")
        except Exception as exc:
            result.error = f"post-claude step failed: {exc}"
            result.censored = True
            print(f"  [trial {trial_idx}] error: {exc}")

        # Save workspace snapshot (code only, no binaries)
        snap = trial_dir / "workspace_snapshot"
        snap.mkdir(exist_ok=True)
        for f in workspace.rglob("*"):
            if f.is_file() and not any(
                p in str(f) for p in ["target/", "build/", ".git/", "__pycache__"]
            ):
                rel = f.relative_to(workspace)
                dst = snap / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                try:
                    shutil.copy2(f, dst)
                except Exception:
                    pass
    finally:
        # ignore_errors=True prevents orphaned processes from crashing the trial result
        shutil.rmtree(tmp, ignore_errors=True)

    return result


# ── Oracle measurement ────────────────────────────────────────────────────────

def measure_oracle(framework: str, task_id: str, n_runs: int = 5) -> float | None:
    """Run the reference implementation N times and return median latency."""
    import statistics
    import nproc_count

    workers = nproc_count.physical_cores()
    scaffold = SCRIPT_DIR / "scaffolds" / framework / task_id / "tier_2"
    lats = []

    for _ in range(n_runs):
        env = os.environ.copy()

        if framework == "tomii":
            # Prefer the dedicated tile-coarsened oracle (separate workspace,
            # avoids libsensor_pipeline_oracle.so vs libsensor_pipeline.so mtime
            # conflicts in the bench target dir).
            oracle_script = TOMII_ORACLE_DIR / "run_bench.py"
            if oracle_script.exists():
                report_path = TOMII_ORACLE_DIR / "_oracle_report.json"
                report_path.unlink(missing_ok=True)
                cmd = [
                    "python", str(oracle_script),
                    "--max-streams", "7",
                    "--exclude-streams", "3",
                    "--report", str(report_path),
                ]
                cwd = str(TOMII_ORACLE_DIR)
            else:
                # Fallback: scaffold baseline (slow — only if oracle is missing).
                script = scaffold / "run_bench.py"
                if not script.exists():
                    print(f"[WARN] No oracle for tomii/{task_id}")
                    return None
                (scaffold / "result.txt").unlink(missing_ok=True)
                (scaffold / "result.txt").touch()
                report_path = scaffold / "_oracle_report.json"
                report_path.unlink(missing_ok=True)
                env["SCRIPT_DIR"] = str(scaffold)
                cmd = [
                    "python", str(script),
                    "--workers", str(workers),
                    "--batching-size", "64",
                    "--coalesce-barriers",
                    "--max-streams", "7",
                    "--exclude-streams", "3",
                    "--report", str(report_path),
                ]
                cwd = str(scaffold)
        else:
            ref_dir = SCRIPT_DIR / "references" / framework / task_id
            ref_sh = ref_dir / "run.sh"
            if not ref_sh.exists():
                print(f"[WARN] No oracle run.sh for {framework}/{task_id}")
                return None
            report_path = ref_dir / "_oracle_timing.txt"
            report_path.unlink(missing_ok=True)
            cmd = ["bash", str(ref_sh), str(workers), "7", "3"]
            cwd = str(ref_dir)

        # Touch the oracle plugin src so cargo always rebuilds it, ensuring
        # _find_dylib picks oracle's .so over any agent-built .so in the same
        # target dir (mtime tie-break).
        if framework == "tomii" and (TOMII_ORACLE_DIR / "src" / "lib.rs").exists():
            (TOMII_ORACLE_DIR / "src" / "lib.rs").touch()

        try:
            result = subprocess.run(
                cmd,
                capture_output=True, text=True, timeout=300, env=env,
                cwd=cwd,
            )
        except subprocess.TimeoutExpired as e:
            print(f"[oracle WARN] run timed out after 300s")
            lats.append(float("nan"))  # counted as failed run; filtered below
            continue

        latency = None
        if report_path.exists():
            try:
                text = report_path.read_text()
                try:
                    data = json.loads(text)
                    latency = data.get("summary", {}).get("avg_latency_us")
                except json.JSONDecodeError:
                    # timing.txt format: avg_latency_us = X
                    m = re.search(r"avg_latency_us\s*=\s*([\d.]+)", text)
                    if m:
                        latency = float(m.group(1))
            except Exception:
                pass
        if latency is None:
            for line in (result.stdout + result.stderr).splitlines():
                m = re.search(r"avg_latency_us\s*=\s*([\d.]+)", line)
                if m:
                    latency = float(m.group(1))
                    break
        if latency is None:
            stderr_tail = (result.stderr or "")[-300:].strip()
            print(f"[oracle WARN] run returned no latency "
                  f"(rc={result.returncode}){': ' + stderr_tail if stderr_tail else ''}")
        else:
            lats.append(latency)

    valid = [x for x in lats if not (x != x)]  # drop NaN
    if not valid:
        return None
    return statistics.median(valid)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="agent-eval: single-condition trial runner")
    p.add_argument("--framework", choices=["tomii", "taskflow"], required=True)
    p.add_argument("--skills", choices=["full", "bare"], default="bare")
    p.add_argument("--tier", type=int, choices=[1, 2], required=True)
    p.add_argument("--task", default="task_1")
    p.add_argument("--n-trials", type=int, default=1)
    p.add_argument("--trial-start", type=int, default=0)
    p.add_argument("--model", default="claude-sonnet-4-6")
    p.add_argument("--budget", type=float, default=5.00)
    p.add_argument("--timeout", type=int, default=1800)
    p.add_argument("--output-dir", default="")
    p.add_argument("--q-threshold", type=float, default=0.80)
    p.add_argument("--skip-oracle", action="store_true",
                   help="Skip oracle measurement (use None for Q threshold)")
    p.add_argument("--oracle-latency", type=float, default=None,
                   help="Use pre-measured oracle latency (µs) instead of running the oracle.")
    args = p.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_label = f"{args.framework}_{args.skills}_tier{args.tier}_{args.task}_{timestamp}"
    out_dir = Path(args.output_dir or SCRIPT_DIR / "results" / run_label)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Measure oracle best (for Tier 2 Q threshold)
    oracle_best = None
    if args.tier == 2 and args.oracle_latency is not None:
        oracle_best = args.oracle_latency
        print(f"[oracle] using pre-measured latency = {oracle_best:.1f}µs")
        print(f"[oracle] Q bar (pass if ≤) = {oracle_best / args.q_threshold:.1f}µs")
    elif args.tier == 2 and not args.skip_oracle:
        print(f"[oracle] measuring reference latency for {args.framework}/{args.task}...")
        oracle_best = measure_oracle(args.framework, args.task)
        if oracle_best:
            print(f"[oracle] median oracle latency = {oracle_best:.1f}µs")
            print(f"[oracle] Q bar (pass if ≤) = {oracle_best / args.q_threshold:.1f}µs")

    (out_dir / "config.json").write_text(json.dumps({
        "framework": args.framework,
        "skills": args.skills,
        "tier": args.tier,
        "task": args.task,
        "model": args.model,
        "budget_usd": args.budget,
        "wall_timeout_s": args.timeout,
        "q_threshold": args.q_threshold,
        "oracle_best_us": oracle_best,
        "n_trials": args.n_trials,
    }, indent=2))

    all_results = []
    try:
        for i in range(args.trial_start, args.trial_start + args.n_trials):
            print(f"\n[trial {i}/{args.trial_start + args.n_trials - 1}] "
                  f"{args.framework}/{args.skills}/tier{args.tier}/{args.task}")
            r = run_trial(
                framework=args.framework,
                skills=args.skills,
                tier=args.tier,
                task_id=args.task,
                trial_idx=i,
                oracle_best=oracle_best,
                output_dir=out_dir,
                model=args.model,
                budget_usd=args.budget,
                wall_timeout_s=args.timeout,
                q_threshold=args.q_threshold,
            )
            all_results.append(asdict(r))
            trial_dir = out_dir / f"trial_{i:03d}"
            (trial_dir / "result.json").write_text(json.dumps(asdict(r), indent=2))
            try:
                from rescore import rescore_trial as _rescore
                _rescore(trial_dir, framework=args.framework,
                         oracle_best_us=oracle_best, q_threshold=args.q_threshold,
                         overwrite=True)
            except Exception as _re:
                print(f"  [rescore] warning: enrichment failed for trial_{i:03d}: {_re}")
    except Exception as exc:
        print(f"\n[ERROR] trial loop crashed: {exc}")
    finally:
        (out_dir / "all_results.json").write_text(json.dumps(all_results, indent=2))
        print(f"\n[done] {len(all_results)} trials → {out_dir}")


if __name__ == "__main__":
    main()
