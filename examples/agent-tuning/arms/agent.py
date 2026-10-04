"""Arm 4: Claude-driven optimisation over a workload's knob space.

Invokes the `claude` CLI (Claude Code subscription) as a subprocess, isolated
from this machine's settings, memory, CLAUDE.md files, MCP servers and tools
(`--tools "" --setting-sources "" --strict-mcp-config`, custom system prompt,
empty working directory), so the model sees only the prompt built here.

Structure ablation (E8) — `--variant` selects what the agent is given:

  full                  knob catalog (domains, descriptions, search hints,
                        risks, graph knobs, forbidden edits) + per-trial
                        `--report` JSON + verifier diagnostics on failures
  no-catalog            raw `main --help` text instead of the catalog (the
                        tunable flag names are listed; no domains, hints,
                        risks or graph knobs); values are not domain-checked
  no-report             scalar latency + pass/fail (with diagnostic) only;
                        no per-run report JSON (this is the pre-E8 agent's
                        feedback design)
  no-verifier-feedback  failed trials report only "FAILED", no diagnostic

Every variant sees the same task statement, budget, default configuration and
latency, and the full trial history (configs + latency / status).

Requires:
    claude CLI on PATH  (verify with: claude --version)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import (  # noqa: E402
    TrialRecord,
    add_common_args,
    establish_baseline,
    log_trial,
    run_trial,
    setup_arm,
    write_run_meta,
)

from tomii import knobs as tomii_knobs  # noqa: E402

#: E8 model (checked with `claude --model claude-sonnet-5 -p hi`, 2026-09-27).
#: The pre-E8 study used "claude-sonnet-4-6".
MODEL = "claude-sonnet-5"
_TIMEOUT_S = 240  # max wall time for one claude call

VARIANTS = ("full", "no-catalog", "no-report", "no-verifier-feedback")

_SYSTEM_PROMPT = (
    "You are an expert performance-tuning assistant for streaming task-graph "
    "runtimes. You answer with a single JSON object and nothing else."
)

_PROMPT_TEMPLATE = """\
Your task: choose the next configuration to try for the Tomii task-graph \
runtime running the {workload} workload. The objective is to MINIMISE mean \
per-frame latency (ms/frame). A trial only counts if the workload's \
verifier/soundness gate passes. You have a budget of {budget} trials; \
this is trial {iteration} ({remaining} remaining, including this one). \
The best verifier-passing trial at the end is what counts.

{knob_block}

## Default configuration (measured once, not part of your budget)

{baseline_block}

## Trial history (oldest first)

{history_block}
{report_block}
## Best so far

{best_block}

## Instructions

Reply with ONLY a JSON object — no prose, no markdown fences — mapping \
{key_kind} to values. Any option you omit keeps its runtime default (not the \
default configuration above). Example shape:
{example}
"""


# ---------------------------------------------------------------------------
# Prompt pieces
# ---------------------------------------------------------------------------


def _help_text(binary: Path) -> str:
    try:
        out = subprocess.run(
            [str(binary), "--help"], capture_output=True, text=True, timeout=30
        )
        return out.stdout.strip()
    except Exception as exc:  # pragma: no cover - diagnostic only
        return f"(could not run --help: {exc})"


def _knob_block(variant: str, space: dict, binary: Path) -> str:
    if variant != "no-catalog":
        return tomii_knobs.render_prompt(space)
    tunable = [k["name"] for k in space["knobs"] if k["kind"] == "cli"]
    flags = ", ".join("--" + n.replace("_", "-") for n in tunable)
    return (
        "## Runtime command-line interface (output of `main --help`)\n\n"
        + _help_text(binary)
        + "\n\nTunable options in this study: "
        + flags
        + ".\nAll other options (graph/plugin paths, frame counts, timing, "
        "report and output paths, core placement, rdtsc) are fixed by the "
        "harness. Use the option name without leading dashes, with "
        "underscores (e.g. \"batching_size\"); boolean flags take true/false."
    )


def _fmt_ms(ms: float | None) -> str:
    return f"{ms:.4f} ms" if ms is not None else "N/A"


def _status(entry: dict, variant: str) -> str:
    if entry.get("skipped"):
        return "NO TRIAL (reply could not be parsed)"
    if entry["verifier_ok"]:
        return f"OK {_fmt_ms(entry['ms_per_frame'])}"
    if variant == "no-verifier-feedback":
        return "FAILED"
    return f"FAILED ({entry.get('rejection_reason') or 'unknown'})"


def _history_block(trial_log: list[dict], variant: str) -> str:
    if not trial_log:
        return "(none yet)"
    lines = []
    for e in trial_log:
        note = ""
        if e.get("ignored"):
            note = f"  [ignored options: {', '.join(e['ignored'])}]"
        lines.append(
            f"trial {e['iteration']}: {_status(e, variant)} | "
            f"{json.dumps(e.get('knobs', {}))}{note}"
        )
    return "\n".join(lines)


def _report_block(
    trial_log: list[dict], baseline_report: dict | None, variant: str
) -> str:
    if variant == "no-report":
        return ""
    shown: list[tuple[str, dict]] = []
    ok = [e for e in trial_log if e["verifier_ok"] and e.get("report")]
    best = min(ok, key=lambda e: e["ms_per_frame"]) if ok else None
    recent = [e for e in trial_log if e.get("report")][-3:]
    picks = recent + ([best] if best is not None and best not in recent else [])
    for e in picks:
        shown.append((f"trial {e['iteration']}", e["report"]))
    if not shown and baseline_report is not None:
        shown.append(("default configuration", baseline_report))
    if not shown:
        return ""
    body = "\n".join(
        f"{label}: {json.dumps(rep, separators=(',', ':'))}" for label, rep in shown
    )
    return (
        "\n## Per-run performance reports (Tomii `--report` JSON; last 3 "
        "trials with a report, plus the best trial)\n\n" + body + "\n"
    )


def _best_block(trial_log: list[dict]) -> str:
    ok = [e for e in trial_log if e["verifier_ok"] and e["ms_per_frame"] is not None]
    if not ok:
        return "(no passing trial yet)"
    b = min(ok, key=lambda e: e["ms_per_frame"])
    return f"trial {b['iteration']}: {_fmt_ms(b['ms_per_frame'])} | {json.dumps(b['knobs'])}"


# ---------------------------------------------------------------------------
# Reply validation
# ---------------------------------------------------------------------------


def _validate_reply(
    space: dict, data: dict, variant: str
) -> tuple[dict | None, list[str]]:
    """Filter a reply to known knobs. Returns (knobs or None, ignored names).

    Catalog variants: out-of-domain values invalidate the reply (retry).
    no-catalog: names are normalised from flag spelling; values are only
    type-coerced (the runtime itself rejects nonsense values), graph knobs
    are not reachable.
    """
    ignored: list[str] = []
    if variant == "no-catalog":
        types = {k["name"]: k["domain"]["kind"] for k in space["knobs"] if k["kind"] == "cli"}
        knobs: dict = {}
        for raw_name, value in data.items():
            name = str(raw_name).lstrip("-").replace("-", "_")
            if name not in types:
                ignored.append(str(raw_name))
                continue
            try:
                if types[name] == "bool":
                    if isinstance(value, str):
                        value = value.strip().lower() in ("1", "true", "yes", "on")
                    knobs[name] = bool(value)
                else:
                    knobs[name] = int(value)
            except (TypeError, ValueError):
                return None, ignored
        return (knobs or None), ignored

    domains = {
        k["name"]: tomii_knobs.enumerate_domain(k["domain"]) for k in space["knobs"]
    }
    knobs = {}
    for name, value in data.items():
        if name not in domains:
            print(f"[agent] dropping unknown knob {name!r}", flush=True)
            ignored.append(str(name))
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            coerced = value
        else:
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                coerced = value
        if coerced not in domains[name]:
            print(f"[agent] value {value!r} out of domain for {name!r}", flush=True)
            return None, ignored
        knobs[name] = coerced
    return (knobs or None), ignored


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------


class LlmStats:
    def __init__(self) -> None:
        self.calls = 0
        self.failed_calls = 0
        self.input_tokens = 0
        self.cache_creation_tokens = 0
        self.cache_read_tokens = 0
        self.output_tokens = 0
        self.cost_usd = 0.0
        self.seconds = 0.0
        self.models: set[str] = set()
        self.limit_waits = 0

    def as_dict(self) -> dict:
        return {
            "llm_calls": self.calls,
            "llm_failed_calls": self.failed_calls,
            "llm_input_tokens": self.input_tokens,
            "llm_cache_creation_tokens": self.cache_creation_tokens,
            "llm_cache_read_tokens": self.cache_read_tokens,
            "llm_output_tokens": self.output_tokens,
            "llm_cost_usd": round(self.cost_usd, 4),
            "llm_seconds": round(self.seconds, 1),
            "llm_models": sorted(self.models),
            "llm_limit_waits": self.limit_waits,
        }


_LIMIT_WAIT_S = 300
_LIMIT_MAX_WAITS = 48  # up to 4 h


def _call_claude(prompt: str, stats: LlmStats, cwd: str) -> str | None:
    """One LLM call.  A subscription usage-limit error is NOT a failed
    proposal: wait for the limit to reset and retry (so the arm never burns
    its trial budget on an outage), up to _LIMIT_MAX_WAITS x _LIMIT_WAIT_S."""
    for _ in range(_LIMIT_MAX_WAITS):
        out = _call_claude_once(prompt, stats, cwd)
        if out is _LIMITED:
            print(f"[agent] usage limit hit — waiting {_LIMIT_WAIT_S}s", flush=True)
            stats.limit_waits += 1
            time.sleep(_LIMIT_WAIT_S)
            continue
        return out
    raise SystemExit("[agent] usage limit did not reset; aborting run")


_LIMITED = object()


def _call_claude_once(prompt: str, stats: LlmStats, cwd: str):
    cmd = [
        "claude",
        "-p",
        "--output-format",
        "json",
        "--model",
        MODEL,
        "--tools",
        "",
        "--setting-sources",
        "",
        "--strict-mcp-config",
        "--disable-slash-commands",
        "--no-session-persistence",
        "--system-prompt",
        _SYSTEM_PROMPT,
    ]
    t0 = time.monotonic()
    stats.calls += 1
    try:
        proc = subprocess.run(
            cmd,
            input=prompt,
            capture_output=True,
            text=True,
            timeout=_TIMEOUT_S,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired:
        stats.seconds += time.monotonic() - t0
        stats.failed_calls += 1
        print("[agent] claude CLI timed out", flush=True)
        return None
    stats.seconds += time.monotonic() - t0
    try:
        out = json.loads(proc.stdout)
    except json.JSONDecodeError:
        stats.failed_calls += 1
        print(f"[agent] non-JSON CLI output: {proc.stdout[:200]!r} {proc.stderr[:200]!r}", flush=True)
        return None
    usage = out.get("usage") or {}
    stats.input_tokens += int(usage.get("input_tokens") or 0)
    stats.cache_creation_tokens += int(usage.get("cache_creation_input_tokens") or 0)
    stats.cache_read_tokens += int(usage.get("cache_read_input_tokens") or 0)
    stats.output_tokens += int(usage.get("output_tokens") or 0)
    stats.cost_usd += float(out.get("total_cost_usd") or 0.0)
    stats.models.update((out.get("modelUsage") or {}).keys())
    if out.get("is_error"):
        msg = str(out.get("result") or "").lower()
        if "limit" in msg and ("session" in msg or "usage" in msg or "rate" in msg):
            stats.calls -= 1  # not a real proposal call
            return _LIMITED
        stats.failed_calls += 1
        print(f"[agent] CLI error: {str(out.get('result'))[:200]}", flush=True)
        return None
    return str(out.get("result") or "")


def _ask_claude(
    prompt: str, space: dict, variant: str, stats: LlmStats, cwd: str
) -> tuple[dict | None, list[str]]:
    for attempt in range(2):
        raw = _call_claude(prompt, stats, cwd)
        if raw is None:
            continue
        raw = raw.strip()
        if raw.startswith("```"):
            raw = "\n".join(
                ln for ln in raw.splitlines() if not ln.startswith("```")
            ).strip()
        # Tolerate prose around a single JSON object.
        if not raw.startswith("{") and "{" in raw and "}" in raw:
            raw = raw[raw.index("{") : raw.rindex("}") + 1]
        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError(f"expected a JSON object, got {type(data).__name__}")
            knobs, ignored = _validate_reply(space, data, variant)
            if knobs is None:
                raise ValueError("no valid knob values in reply")
            return knobs, ignored
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            print(
                f"[agent] parse failed (attempt {attempt + 1}: {exc}) raw={raw[:160]!r}",
                flush=True,
            )
    return None, []


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser(description="Claude-agent search over a knob space")
    add_common_args(p)
    p.add_argument("--variant", choices=VARIANTS, default="full")
    args = p.parse_args()

    arm = "agent" if args.variant == "full" else f"agent-{args.variant}"
    t_start = time.monotonic()
    workload, space, results_dir = setup_arm(args)
    log_file = results_dir / f"{arm}_trials.jsonl"
    print(
        f"[{arm}] workload={workload.name} knob space v{space['version']}: "
        f"{len(space['knobs'])} knobs, model={MODEL}, seed/run={args.seed}",
        flush=True,
    )

    baseline = establish_baseline(
        frames=args.frames,
        warmup=args.warmup,
        results_dir=results_dir,
        workload=workload,
    )
    baseline_info = json.loads((results_dir / "baseline.json").read_text())
    baseline_report = baseline_info.get("report")
    baseline_block = (
        f"{json.dumps(baseline_info['knobs'])} -> "
        + (f"{baseline:.4f} ms/frame" if baseline > 0 else "FAILED")
    )

    knob_block = _knob_block(args.variant, space, workload.binary)
    example = (
        '{"workers": 4, "slots": 1, "inline_continuation": true, "batching_size": 4}'
    )
    key_kind = (
        "option names" if args.variant == "no-catalog" else "the exact knob names listed above"
    )

    stats = LlmStats()
    trial_log: list[dict] = []
    best_ms = float("inf")
    llm_cwd = tempfile.mkdtemp(prefix="e8_llm_cwd_")

    for i in range(args.iterations):
        t_iter = time.monotonic()
        prompt = _PROMPT_TEMPLATE.format(
            workload=workload.name,
            budget=args.iterations,
            iteration=i,
            remaining=args.iterations - i,
            knob_block=knob_block,
            baseline_block=baseline_block,
            history_block=_history_block(trial_log, args.variant),
            report_block=_report_block(trial_log, baseline_report, args.variant),
            best_block=_best_block(trial_log),
            key_kind=key_kind,
            example=example,
        )
        prompt_dir = results_dir / f"{arm}_prompts"
        prompt_dir.mkdir(exist_ok=True)
        (prompt_dir / f"iter_{i:03d}.txt").write_text(prompt)
        calls_before = stats.calls
        llm_s_before = stats.seconds
        knobs, ignored = _ask_claude(prompt, space, args.variant, stats, llm_cwd)
        llm_s = stats.seconds - llm_s_before

        if knobs is None:
            print(f"[{arm} {i}] skipped — reply could not be parsed", flush=True)
            entry = {
                "iteration": i,
                "skipped": True,
                "verifier_ok": False,
                "ms_per_frame": None,
                "knobs": {},
            }
            trial_log.append(entry)
            with log_file.open("a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(
                        {
                            "iteration": i,
                            "arm": arm,
                            "skipped": True,
                            "verifier_ok": False,
                            "ms_per_frame": None,
                            "rejection_reason": "llm reply unparseable",
                            "wall_seconds": 0.0,
                            "llm_calls": stats.calls - calls_before,
                            "llm_seconds": llm_s,
                            "seed": args.seed,
                        }
                    )
                    + "\n"
                )
            continue

        result = run_trial(workload, knobs, args, space)
        record = TrialRecord(
            iteration=i,
            knobs=knobs,
            result=result,
            arm=arm,
            notes="claude-suggested",
            extra={
                "seed": args.seed,
                "variant": args.variant,
                "llm_calls": stats.calls - calls_before,
                "llm_seconds": llm_s,
                "prompt_chars": len(prompt),
                "ignored_options": ignored,
            },
        )
        log_trial(record, log_file)

        trial_log.append(
            {
                "iteration": i,
                "verifier_ok": result.verifier_ok and result.ms_per_frame is not None,
                "ms_per_frame": result.ms_per_frame,
                "rejection_reason": result.rejection_reason,
                "knobs": knobs,
                "report": result.report,
                "ignored": ignored,
            }
        )

        elapsed = time.monotonic() - t_iter
        if result.verifier_ok and result.ms_per_frame is not None:
            tag = "new best" if result.ms_per_frame < best_ms else "ok"
            best_ms = min(best_ms, result.ms_per_frame)
            print(
                f"[{arm} {i}] {tag}: {result.ms_per_frame:.4f} ms  best={best_ms:.4f} "
                f"llm={llm_s:.1f}s wall={elapsed:.1f}s",
                flush=True,
            )
        else:
            print(
                f"[{arm} {i}] rejected — {result.rejection_reason}  wall={elapsed:.1f}s",
                flush=True,
            )

    write_run_meta(
        results_dir,
        arm,
        args,
        space,
        t_start,
        model=MODEL,
        variant=args.variant,
        baseline_ms=baseline,
        best_ms=best_ms if best_ms < float("inf") else None,
        **stats.as_dict(),
    )
    print(f"\n[{arm}] done: best={best_ms:.4f} ms (baseline {baseline:.4f}) {stats.as_dict()}")


if __name__ == "__main__":
    main()
