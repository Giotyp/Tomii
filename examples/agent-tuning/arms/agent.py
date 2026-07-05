"""Arm 4: Claude-driven optimisation over the stream-analytics knob space.

Invokes the `claude` CLI (Claude Code subscription) as a subprocess.
No API key required — uses the active subscription on this machine.

Requires:
    claude CLI on PATH  (verify with: claude --version)
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import (  # noqa: E402
    TrialRecord,
    establish_baseline,
    evaluate,
    load_knob_space,
    log_trial,
)

from tomii import knobs as tomii_knobs  # noqa: E402

MODEL = "claude-sonnet-4-6"
_TIMEOUT_S = 60  # max wall time for one claude call

_PROMPT_TEMPLATE = """\
You are an expert performance-tuning assistant for the Tomii task-graph framework.

Your task: suggest runtime knob configurations for the stream-analytics workload \
to minimise ms_per_stream while keeping the verifier passing.

{knob_space_block}

## Current state

Iteration: {iteration}
Baseline: {baseline_ms:.4f} ms/stream
Current best: {best_ms}

Last {n_shown} trials:
{trial_summary}

## Instructions

Reply with ONLY a JSON object — no prose, no markdown fences — keyed by the
exact knob names listed above (any knob you omit keeps its default), like:
{{"workers": 4, "slots": 4, "inline_continuation": true, "batching_size": 4}}
"""


def _format_trial_summary(records: list[dict], n: int = 5) -> str:
    recent = records[-n:] if len(records) > n else records
    if not recent:
        return "  (none yet)"
    lines = []
    for r in recent:
        ok = r.get("verifier_ok", False)
        ms = r.get("ms_per_stream")
        ms_str = f"{ms:.4f} ms" if ms is not None else "N/A"
        k = r.get("knobs", {})
        reason = r.get("rejection_reason") or ""
        status = "OK" if ok else f"REJECTED ({reason})"
        lines.append(f"  iter={r['iteration']} {status} ms={ms_str} | {json.dumps(k)}")
    return "\n".join(lines)


def _validate_reply(space: dict, data: dict) -> dict | None:
    """Filter a raw reply dict down to known knobs with in-domain values.

    Unknown keys are dropped with a warning; out-of-domain values invalidate
    the reply (return None) so the retry prompt is exercised.
    """
    domains = {
        k["name"]: tomii_knobs.enumerate_domain(k["domain"]) for k in space["knobs"]
    }
    knobs: dict = {}
    for name, value in data.items():
        if name not in domains:
            print(f"[agent] dropping unknown knob {name!r}", flush=True)
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            coerced = value
        else:
            try:
                coerced = int(value)
            except (TypeError, ValueError):
                coerced = value
        if coerced not in domains[name]:
            print(
                f"[agent] value {value!r} out of domain for {name!r}",
                flush=True,
            )
            return None
        knobs[name] = coerced
    return knobs or None


def _ask_claude(
    space: dict,
    baseline_ms: float,
    best_ms: float,
    iteration: int,
    trial_log: list[dict],
) -> dict | None:
    """Ask Claude for the next knob config via `claude -p`. Returns None if parsing fails."""
    n_shown = min(5, len(trial_log))
    best_str = f"{best_ms:.4f}" if best_ms < float("inf") else "none yet"
    prompt = _PROMPT_TEMPLATE.format(
        knob_space_block=tomii_knobs.render_prompt(space),
        iteration=iteration,
        baseline_ms=baseline_ms,
        best_ms=best_str,
        n_shown=n_shown,
        trial_summary=_format_trial_summary(trial_log, n=5),
    )

    cmd = [
        "claude",
        "-p",
        "--output-format",
        "text",
        "--model",
        MODEL,
    ]

    for attempt in range(2):
        try:
            result = subprocess.run(
                cmd,
                input=prompt,
                capture_output=True,
                text=True,
                timeout=_TIMEOUT_S,
            )
        except subprocess.TimeoutExpired:
            print(f"[agent] claude CLI timed out (attempt {attempt + 1})", flush=True)
            continue
        except FileNotFoundError:
            print(
                "[agent] ERROR: `claude` CLI not found on PATH. "
                "Install Claude Code and ensure `claude` is on PATH.",
                file=sys.stderr,
            )
            return None

        raw = (result.stdout or "").strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            lines = raw.splitlines()
            raw = "\n".join(ln for ln in lines if not ln.startswith("```")).strip()

        try:
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError(f"expected a JSON object, got {type(data).__name__}")
            knobs = _validate_reply(space, data)
            if knobs is None:
                raise ValueError("no valid knob values in reply")
            return knobs
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            if attempt == 0:
                print(
                    f"[agent] parse failed ({exc}), retrying ... raw={raw[:120]!r}",
                    flush=True,
                )
            else:
                print(
                    f"[agent] parse failed after retry — skipping iteration: {raw[:120]!r}",
                    flush=True,
                )

    return None


def main() -> None:
    p = argparse.ArgumentParser(
        description="Claude-agent search over stream-analytics knobs"
    )
    p.add_argument("--iterations", type=int, default=50)
    p.add_argument("--streams", type=int, default=500)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--results-dir", type=Path, default=Path("results"))
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.results_dir / "agent_trials.jsonl"

    space = load_knob_space()
    print(f"[agent] knob space: {len(space['knobs'])} knobs", flush=True)

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")

    trial_log: list[dict] = []
    rejected_count = 0

    for i in range(args.iterations):
        t_iter = time.monotonic()
        knobs = _ask_claude(
            space,
            baseline_ms=baseline,
            best_ms=best_ms,
            iteration=i,
            trial_log=trial_log,
        )

        if knobs is None:
            rejected_count += 1
            print(f"[agent {i}] skipped — Claude response could not be parsed", flush=True)
            continue

        result = evaluate(knobs, streams=args.streams, warmup=args.warmup, space=space)
        record = TrialRecord(
            iteration=i,
            knobs=knobs,
            result=result,
            arm="agent",
            notes="claude-suggested",
        )
        log_trial(record, log_file)

        entry: dict = {
            "iteration": i,
            "verifier_ok": result.verifier_ok,
            "ms_per_stream": result.ms_per_stream,
            "rejection_reason": result.rejection_reason,
            "knobs": knobs,
        }
        trial_log.append(entry)

        elapsed = time.monotonic() - t_iter
        if result.verifier_ok and result.ms_per_stream is not None:
            if result.ms_per_stream < best_ms:
                best_ms = result.ms_per_stream
                delta_pct = (
                    (baseline - best_ms) / baseline * 100.0 if baseline > 0.0 else 0.0
                )
                print(
                    f"[agent {i}] new best: {best_ms:.4f} ms/stream "
                    f"(delta: {delta_pct:.1f}%) wall={elapsed:.1f}s",
                    flush=True,
                )
            else:
                print(
                    f"[agent {i}] ok: {result.ms_per_stream:.4f} ms  "
                    f"best={best_ms:.4f}  wall={elapsed:.1f}s",
                    flush=True,
                )
        else:
            reason = result.rejection_reason or "verifier failed"
            rejected_count += 1
            print(f"[agent {i}] rejected — {reason}  wall={elapsed:.1f}s", flush=True)

    if baseline > 0.0 and best_ms < float("inf"):
        improvement = (baseline - best_ms) / baseline * 100.0
        print(
            f"\nAgent search: baseline={baseline:.4f}, best={best_ms:.4f} ms "
            f"({improvement:.1f}% improvement) — {rejected_count} rejected trials"
        )
    else:
        print(
            f"\nAgent search complete. best={best_ms:.4f} ms/stream "
            f"— {rejected_count} rejected trials"
        )


if __name__ == "__main__":
    main()
