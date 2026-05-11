#!/usr/bin/env python3
"""Post-hoc enrichment of agent-eval trials.

Reads existing trial_XXX/claude_output.jsonl and writes trial_XXX/result_enriched.json
without re-running anything.  All metrics are derived from the agent's stream-json.

Usage:
    python rescore.py results/smoke_v2_t1
    python rescore.py results/smoke_v2_t1 --glob trial_000 --overwrite
    python rescore.py results/ --glob 'smoke_*/trial_*' --overwrite
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Iterator

RESCORE_VERSION = "1.0"
LATENCY_FLOOR_US = 200.0   # below this = degenerate (mirrors verifier.py)


# ── Regex catalogue ───────────────────────────────────────────────────────────

# Latency extraction — ordered: most specific first
_LAT_RE = [
    re.compile(r"\bavg=(\d+(?:\.\d+)?)\s*(?:µs|us)\b"),           # Tomii explain/tune
    re.compile(r"avg_latency_us[\"'\s:=]+(\d+(?:\.\d+)?)"),        # report.json / TF stdout
]

# Build failures
_BUILD_ERR_TOMII = re.compile(
    r"error\[E\d+\]"
    r"|error: could not compile"
    r"|cargo.*(?:failed|error)"
    r"|run_bench\.py: error: unrecognized"
    r"|No such file or directory.*\.so"
    , re.I
)
_BUILD_ERR_TASKFLOW = re.compile(
    r"\berror:"
    r"|undefined reference to"
    r"|ld returned \d+ exit status"
    r"|CMake Error"
    r"|make.*\*\*\* \[.*\] Error"
    r"|ninja: error"
    , re.I
)

# Runtime failures (framework-neutral)
_RUNTIME_ERR = re.compile(
    r"thread [\"'].*[\"'] panicked"
    r"|Segmentation fault"
    r"|SIGSEGV"
    r"|\baborted\b.*core dumped"
    r"|double free or corruption"
    r"|AddressSanitizer"
    r"|Python.*Traceback \(most recent"
    , re.I
)

# tomii tune parse-time / validation rejections
_TUNE_REJECT = re.compile(
    r"\[reject\]"
    r"|\[error\] Knob [\"'].*[\"'] is not tunable"
    r"|\[error\] Could not find --.* in run_bench\.py"
    r"|\[validate\] FAIL"
    , re.M
)

# Optimization run commands (tool_use input.command)
_OPT_CMD_TOMII = re.compile(r"(?:python\s+run_bench\.py\b|python -m tomii\s+tune\b)")
_OPT_CMD_TASKFLOW = re.compile(r"\./(?:build/)?sensor_pipeline\b")

# Agent-originated build commands (for failure classification)
_BUILD_CMD = re.compile(
    r"\bcargo\s+build\b"
    r"|\bcmake\b"
    r"|\bmake\b"
    r"|\bg\+\+\b"
    r"|\bcc\b"
    r"|run_bench\.py\s+--build-only"
)


# ── Event helpers ─────────────────────────────────────────────────────────────

def _parse_events(path: Path) -> list[dict]:
    events: list[dict] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip().rstrip(",")
            if not line or line in ("[", "]"):
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return events


def _text_of(block: dict) -> str:
    """Extract plain text from a tool_result content block."""
    c = block.get("content", "")
    if isinstance(c, str):
        return c
    if isinstance(c, list):
        parts: list[str] = []
        for item in c:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("text", ""))
        return "\n".join(parts)
    return ""


def _build_tool_use_index(events: list[dict]) -> dict[str, dict]:
    """Map tool_use_id → tool_use block from all assistant events."""
    idx: dict[str, dict] = {}
    for ev in events:
        if ev.get("type") != "assistant":
            continue
        msg = ev.get("message", ev)
        for block in msg.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "tool_use":
                idx[block["id"]] = block
    return idx


# ── Token accounting ──────────────────────────────────────────────────────────

def _build_token_axis(events: list[dict]) -> tuple[list[int], bool]:
    """Return (tokens_at_turn_k, axis_is_tokens).

    tokens_at_turn_k[i] = cumulative "effort" after the i-th assistant turn.
    Uses max(cache_read + cache_creation) + cumulative(output) when per-message
    usage is available, otherwise falls back to turn index.
    """
    # Collect per-assistant-turn usage
    turns: list[dict] = []
    for ev in events:
        if ev.get("type") == "assistant":
            msg = ev.get("message", ev)
            usage = msg.get("usage", None)
            if usage:
                turns.append(usage)
            else:
                turns.append({})

    if not any(t.get("cache_read_input_tokens") or t.get("output_tokens") for t in turns):
        return list(range(len(turns))), False

    tokens: list[int] = []
    max_cache = 0
    cum_output = 0
    for t in turns:
        c_read = t.get("cache_read_input_tokens", 0) or 0
        c_create = t.get("cache_creation_input_tokens", 0) or 0
        output = t.get("output_tokens", 0) or 0
        max_cache = max(max_cache, c_read + c_create)
        cum_output += output
        tokens.append(max_cache + cum_output)

    return tokens, True


# ── Latency extraction ────────────────────────────────────────────────────────

@dataclass
class LatencySample:
    turn_idx: int
    tokens_so_far: int
    latency_us: float
    source: str


def _extract_latency_from_text(text: str) -> float | None:
    for pat in _LAT_RE:
        m = pat.search(text)
        if m:
            v = float(m.group(1))
            if v > 0:
                return v
    return None


def _tag_source(text: str, tool_name: str, cmd: str) -> str:
    if "tomii tune" in cmd or "python -m tomii tune" in cmd:
        return "tune_result"
    if "tomii --explain" in cmd or "--explain" in cmd:
        return "explain"
    if "run_bench.py" in cmd:
        return "run_bench"
    if "sensor_pipeline" in cmd:
        return "sensor_pipeline"
    if "report.json" in text[:50]:
        return "report_json"
    return "other"


def extract_latencies(
    events: list[dict],
    token_axis: list[int],
) -> list[LatencySample]:
    """Walk tool_use/tool_result pairs and extract every latency reading."""
    tool_uses = _build_tool_use_index(events)
    samples: list[LatencySample] = []

    # Build ordered list of (assistant_turn_idx, user_event) pairs
    asst_turn = -1
    for ev in events:
        if ev.get("type") == "assistant":
            asst_turn += 1
        elif ev.get("type") == "user":
            msg = ev.get("message", ev)
            for block in msg.get("content") or []:
                if not isinstance(block, dict):
                    continue
                if block.get("type") != "tool_result":
                    continue
                text = _text_of(block)
                lat = _extract_latency_from_text(text)
                if lat is None or lat <= 0:
                    continue
                # Map back to the originating tool_use
                tid = block.get("tool_use_id", "")
                tu = tool_uses.get(tid, {})
                cmd = tu.get("input", {}).get("command", "")
                source = _tag_source(text, tu.get("name", ""), cmd)
                tokens = token_axis[min(asst_turn, len(token_axis) - 1)] if token_axis else asst_turn
                samples.append(LatencySample(
                    turn_idx=asst_turn,
                    tokens_so_far=tokens,
                    latency_us=lat,
                    source=source,
                ))

    return samples


# ── Failure extraction ────────────────────────────────────────────────────────

@dataclass
class FailureCounts:
    build_failures_agent: int = 0
    build_failures_inspected: int = 0
    runtime_failures: int = 0
    invalid_tune_rejections: int = 0
    bash_nonzero_exits: int = 0


def extract_failures(events: list[dict], framework: str) -> FailureCounts:
    tool_uses = _build_tool_use_index(events)
    build_re = _BUILD_ERR_TOMII if framework == "tomii" else _BUILD_ERR_TASKFLOW
    fc = FailureCounts()

    for ev in events:
        if ev.get("type") != "user":
            continue
        msg = ev.get("message", ev)
        for block in msg.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "tool_result":
                continue

            is_error = bool(block.get("is_error", False))
            text = _text_of(block)

            if is_error:
                fc.bash_nonzero_exits += 1

            # Check if it was a tune rejection
            if _TUNE_REJECT.search(text):
                fc.invalid_tune_rejections += 1

            # Runtime failures (panics, segfaults)
            if _RUNTIME_ERR.search(text):
                fc.runtime_failures += 1

            # Build failures — classify as agent-initiated or inspected
            if build_re.search(text):
                tid = block.get("tool_use_id", "")
                tu = tool_uses.get(tid, {})
                name = tu.get("name", "")
                cmd = tu.get("input", {}).get("command", "")
                file_path = tu.get("input", {}).get("file_path", "")
                # Agent-initiated = Bash that ran a build command
                if name == "Bash" and _BUILD_CMD.search(cmd):
                    fc.build_failures_agent += 1
                # Inspected = Read/cat of an existing log
                elif name in ("Read", "ReadFile") or (name == "Bash" and "cat " in cmd and "build" in (cmd + file_path).lower()):
                    fc.build_failures_inspected += 1
                else:
                    # Unclassified but still a build error text
                    fc.build_failures_agent += 1

    return fc


# ── Optimization iteration count ──────────────────────────────────────────────

def extract_optimization_iterations(events: list[dict], framework: str) -> int:
    opt_re = _OPT_CMD_TOMII if framework == "tomii" else _OPT_CMD_TASKFLOW
    count = 0
    for ev in events:
        if ev.get("type") != "assistant":
            continue
        msg = ev.get("message", ev)
        for block in msg.get("content") or []:
            if not isinstance(block, dict):
                continue
            if block.get("type") != "tool_use" or block.get("name") != "Bash":
                continue
            cmd = block.get("input", {}).get("command", "")
            # Exclude introspection-only invocations
            if any(x in cmd for x in ("--build-only", "--help", "--list-knobs", "--schema",
                                       "--explain", "tune --help")):
                continue
            if opt_re.search(cmd):
                count += 1
    return count


# ── Misc counters ─────────────────────────────────────────────────────────────

def _count_tool_calls(events: list[dict]) -> int:
    n = 0
    for ev in events:
        if ev.get("type") == "assistant":
            msg = ev.get("message", ev)
            for b in msg.get("content") or []:
                if isinstance(b, dict) and b.get("type") == "tool_use":
                    n += 1
    return n


def _count_tool_results(events: list[dict]) -> int:
    n = 0
    for ev in events:
        if ev.get("type") == "user":
            msg = ev.get("message", ev)
            for b in msg.get("content") or []:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    n += 1
    return n


def _count_assistant_turns(events: list[dict]) -> int:
    return sum(1 for e in events if e.get("type") == "assistant")


def _num_turns_from_result(events: list[dict]) -> int | None:
    for ev in events:
        if ev.get("type") == "result":
            v = ev.get("num_turns")
            if v is not None:
                return int(v)
    return None


# ── Core rescorer ─────────────────────────────────────────────────────────────

def rescore_trial(
    trial_dir: Path,
    framework: str,
    oracle_best_us: float | None = None,
    q_threshold: float = 0.80,
    overwrite: bool = False,
    warnings: list[str] | None = None,
) -> dict:
    """Parse trial_dir/claude_output.jsonl and return an enriched metrics dict.

    Also writes trial_dir/result_enriched.json unless it already exists and
    overwrite=False.
    """
    if warnings is None:
        warnings = []

    jsonl = trial_dir / "claude_output.jsonl"
    if not jsonl.exists():
        raise FileNotFoundError(f"claude_output.jsonl not found in {trial_dir}")

    out_path = trial_dir / "result_enriched.json"
    if out_path.exists() and not overwrite:
        return json.loads(out_path.read_text())

    events = _parse_events(jsonl)

    # Read config from parent (may not exist for old trials)
    config: dict = {}
    config_path = trial_dir.parent / "config.json"
    if config_path.exists():
        try:
            config = json.loads(config_path.read_text())
        except Exception:
            pass

    # Read trial result.json for baseline identity fields
    result_json: dict = {}
    rj_path = trial_dir / "result.json"
    if rj_path.exists():
        try:
            result_json = json.loads(rj_path.read_text())
        except Exception:
            pass

    # Resolve framework from result.json > config > parameter
    fw = result_json.get("framework") or config.get("framework") or framework
    q_thr = float(config.get("q_threshold") or q_threshold)
    oracle = oracle_best_us or config.get("oracle_best_us")

    # ── Token axis ────────────────────────────────────────────────────────────
    token_axis, tokens_axis_valid = _build_token_axis(events)

    # ── Latency trajectory ────────────────────────────────────────────────────
    samples = extract_latencies(events, token_axis)

    baseline_latency: float | None = None
    final_latency: float | None = None
    tokens_to_first_working: int | None = None
    tokens_to_q: int | None = None
    first_action_improvement: float | None = None

    valid_samples = [s for s in samples if s.latency_us >= LATENCY_FLOOR_US]

    if valid_samples:
        baseline_latency = valid_samples[0].latency_us
        final_latency = valid_samples[-1].latency_us
        tokens_to_first_working = valid_samples[0].tokens_so_far

        if oracle is not None:
            q_bar = oracle / q_thr
            for s in valid_samples:
                if s.latency_us <= q_bar:
                    tokens_to_q = s.tokens_so_far
                    break

        if len(valid_samples) >= 2:
            l1 = valid_samples[1].latency_us
            if baseline_latency > 0:
                first_action_improvement = (baseline_latency - l1) / baseline_latency

    # ── Failure counts ────────────────────────────────────────────────────────
    fc = extract_failures(events, fw)

    # ── Optimization iterations ───────────────────────────────────────────────
    opt_iters = extract_optimization_iterations(events, fw)

    # ── Misc ──────────────────────────────────────────────────────────────────
    num_asst = _count_assistant_turns(events)
    num_tool_calls = _count_tool_calls(events)
    num_tool_results = _count_tool_results(events)
    num_turns_result = _num_turns_from_result(events)

    enriched: dict = {
        "trial_idx": result_json.get("trial_idx"),
        "framework": fw,
        "skills": result_json.get("skills") or config.get("skills"),
        "task_id": result_json.get("task_id") or config.get("task"),
        "tier": result_json.get("tier") or config.get("tier"),

        "trajectory_axis": "tokens" if tokens_axis_valid else "turn_idx",
        "latency_trajectory": [
            {
                "turn_idx": s.turn_idx,
                "tokens_so_far": s.tokens_so_far,
                "latency_us": s.latency_us,
                "source": s.source,
            }
            for s in samples
        ],

        "baseline_latency_us": baseline_latency,
        "final_latency_us": final_latency,
        "tokens_to_first_working_latency": tokens_to_first_working,
        "tokens_to_q": tokens_to_q,
        "first_action_improvement": first_action_improvement,

        "build_failures_agent": fc.build_failures_agent,
        "build_failures_inspected": fc.build_failures_inspected,
        "runtime_failures": fc.runtime_failures,
        "invalid_tune_rejections": fc.invalid_tune_rejections,
        "bash_nonzero_exits": fc.bash_nonzero_exits,

        "optimization_iterations": opt_iters,

        "num_assistant_turns": num_asst,
        "num_tool_calls": num_tool_calls,
        "tool_result_count": num_tool_results,
        "num_turns_reported_by_result": num_turns_result,

        "per_turn_usage_available": tokens_axis_valid,
        "rescore_version": RESCORE_VERSION,
        "rescore_schema_warnings": warnings,
    }

    out_path.write_text(json.dumps(enriched, indent=2))
    return enriched


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(description="Post-hoc metric enrichment for agent-eval trials")
    p.add_argument("results_dir", help="Directory containing trial_XXX subdirs (or a parent containing multiple run dirs)")
    p.add_argument("--glob", default="trial_*", help="Glob pattern for trial dirs relative to results_dir (default: trial_*)")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing result_enriched.json")
    p.add_argument("--framework", default="tomii", help="Framework fallback if config.json absent")
    p.add_argument("--oracle-latency", type=float, default=None)
    p.add_argument("--q-threshold", type=float, default=0.80)
    p.add_argument("--dry-run", action="store_true", help="Parse and print metrics; don't write files")
    p.add_argument("--summary", action="store_true", help="Print one-line summary per trial after rescoring")
    args = p.parse_args()

    root = Path(args.results_dir)
    trial_dirs = sorted(root.glob(args.glob))
    if not trial_dirs:
        # Try one level up: root/*/trial_*
        trial_dirs = sorted(root.glob(f"*/{args.glob}"))

    if not trial_dirs:
        print(f"No trial dirs found under {root} with pattern '{args.glob}'", file=sys.stderr)
        sys.exit(1)

    ok = failed = 0
    for td in trial_dirs:
        if not (td / "claude_output.jsonl").exists():
            continue
        try:
            if args.dry_run:
                enriched = rescore_trial(
                    td,
                    framework=args.framework,
                    oracle_best_us=args.oracle_latency,
                    q_threshold=args.q_threshold,
                    overwrite=True,
                )
                # Don't write
            else:
                enriched = rescore_trial(
                    td,
                    framework=args.framework,
                    oracle_best_us=args.oracle_latency,
                    q_threshold=args.q_threshold,
                    overwrite=args.overwrite,
                )
            ok += 1
            if args.summary or args.dry_run:
                fw = enriched.get("framework", "?")
                sk = enriched.get("skills", "?")
                traj_len = len(enriched.get("latency_trajectory", []))
                base = enriched.get("baseline_latency_us")
                final = enriched.get("final_latency_us")
                toq = enriched.get("tokens_to_q")
                bf = enriched.get("build_failures_agent", 0)
                rej = enriched.get("invalid_tune_rejections", 0)
                opt = enriched.get("optimization_iterations", 0)
                fai = enriched.get("first_action_improvement")
                base_s = f"{base:.0f}µs" if base else "?"
                final_s = f"{final:.0f}µs" if final else "?"
                toq_s = str(toq) if toq else "never"
                fai_s = f"{fai:.2%}" if fai is not None else "?"
                print(
                    f"{td.parent.name}/{td.name}  [{fw}/{sk}]  "
                    f"baseline={base_s}  final={final_s}  "
                    f"traj={traj_len}  opt_iters={opt}  "
                    f"tokens_to_q={toq_s}  first_improvement={fai_s}  "
                    f"build_failures={bf}  rejections={rej}"
                )
        except Exception as exc:
            print(f"ERROR {td}: {exc}", file=sys.stderr)
            failed += 1

    print(f"\n[rescore] {ok} trials processed, {failed} errors")


if __name__ == "__main__":
    main()
