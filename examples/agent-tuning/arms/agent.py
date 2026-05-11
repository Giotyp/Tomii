"""Arm 4: Claude-driven optimisation over the stream-analytics knob space.

Uses the Anthropic Python SDK to ask Claude to suggest the next KnobConfig
to try, based on the current trial history and best result.

Requires:
    pip install anthropic
    export ANTHROPIC_API_KEY=sk-ant-...
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import anthropic
except ImportError:
    print(
        "ERROR: anthropic is not installed. Install it with: pip install anthropic",
        file=sys.stderr,
    )
    sys.exit(1)

from harness import (  # noqa: E402
    KnobConfig,
    TrialRecord,
    establish_baseline,
    evaluate,
    log_trial,
)

MODEL = "claude-sonnet-4-6"

_SYSTEM_PROMPT = """\
You are an expert performance-tuning assistant for the Tomii task-graph framework.

Your task: suggest runtime knob configurations for the stream-analytics workload \
to minimise ms_per_stream while keeping the verifier passing.

## Knob space (all allowed values)

workers:              [1, 2, 4, 8]
slots:                [1, 4, 16, 64]
inline_continuation:  [true, false]
coalesce_barriers:    [true, false]
fifo:                 [true, false]
custom:               [true, false]
no_fanout_bulk:       [true, false]
batching_size:        [1, 4, 8, 16]

## Knob semantics

- workers: Rayon worker thread count. Match physical cores for compute-bound graphs.
- slots: Concurrent in-flight streams. 1 minimises latency; higher values increase throughput.
- inline_continuation: Run single-successor tasks inline (reduces scheduling overhead).
- coalesce_barriers: Batch barrier fan-outs into bulk tasks (helps when factor >> workers).
- fifo: FIFO scheduling instead of depth-first (default depth-first usually better for latency).
- custom: Enable custom scheduling strategy.
- no_fanout_bulk: Disable fanout bulk dispatch.
- batching_size: Max tasks per scheduler batch (tune to reduce scheduling overhead).

## Response format

Reply with ONLY a JSON object — no prose, no markdown fences — like:
{"workers": 4, "slots": 4, "inline_continuation": true, "coalesce_barriers": true, \
"fifo": false, "custom": true, "no_fanout_bulk": false, "batching_size": 4}
"""


def _format_trial_summary(records: list[dict], n: int = 5) -> str:
    """Format the last n trials as a compact text block."""
    recent = records[-n:] if len(records) > n else records
    lines = []
    for r in recent:
        ok = r.get("verifier_ok", False)
        ms = r.get("ms_per_stream")
        ms_str = f"{ms:.4f} ms" if ms is not None else "N/A"
        k = r.get("knobs", {})
        reason = r.get("rejection_reason") or ""
        status = "OK" if ok else f"REJECTED ({reason})"
        lines.append(
            f"  iter={r['iteration']} {status} ms={ms_str} | "
            f"workers={k.get('workers')} slots={k.get('slots')} "
            f"inline={k.get('inline_continuation')} coalesce={k.get('coalesce_barriers')} "
            f"fifo={k.get('fifo')} custom={k.get('custom')} "
            f"no_fanout={k.get('no_fanout_bulk')} batching={k.get('batching_size')}"
        )
    return "\n".join(lines)


def _ask_claude(
    client: anthropic.Anthropic,
    baseline_ms: float,
    best_ms: float,
    iteration: int,
    trial_log: list[dict],
) -> KnobConfig | None:
    """Ask Claude for the next knob config. Returns None if parsing fails."""
    trial_summary = _format_trial_summary(trial_log)
    best_str = f"{best_ms:.4f}" if best_ms < float("inf") else "none yet"

    user_content = (
        f"Iteration: {iteration}\n"
        f"Baseline: {baseline_ms:.4f} ms/stream\n"
        f"Current best: {best_str} ms/stream\n\n"
        f"Last {min(5, len(trial_log))} trials:\n"
        f"{trial_summary if trial_summary else '  (none yet)'}\n\n"
        "Suggest the next KnobConfig to try. Reply with ONLY a JSON object."
    )

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=256,
            system=[
                {
                    "type": "text",
                    "text": _SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": user_content}],
        )
    except anthropic.APIError as exc:
        print(f"[agent] API error: {exc}", flush=True)
        return None

    raw = response.content[0].text.strip() if response.content else ""

    # Strip markdown fences if Claude adds them despite instructions
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(
            ln for ln in lines if not ln.startswith("```")
        ).strip()

    try:
        data = json.loads(raw)
        knobs = KnobConfig(
            workers=int(data.get("workers", 4)),
            slots=int(data.get("slots", 4)),
            inline_continuation=bool(data.get("inline_continuation", True)),
            coalesce_barriers=bool(data.get("coalesce_barriers", True)),
            fifo=bool(data.get("fifo", False)),
            custom=bool(data.get("custom", True)),
            no_fanout_bulk=bool(data.get("no_fanout_bulk", False)),
            batching_size=int(data.get("batching_size", 1)),
        )
        return knobs
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        print(f"[agent] failed to parse response ({exc}): {raw!r}", flush=True)
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

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(
            "ERROR: ANTHROPIC_API_KEY environment variable is not set.",
            file=sys.stderr,
        )
        sys.exit(1)

    args.results_dir.mkdir(parents=True, exist_ok=True)
    log_file = args.results_dir / "agent_trials.jsonl"

    baseline = establish_baseline(
        streams=args.streams,
        warmup=args.warmup,
        results_dir=args.results_dir,
    )
    best_ms = baseline if baseline > 0.0 else float("inf")

    client = anthropic.Anthropic(api_key=api_key)
    trial_log: list[dict] = []

    rejected_count = 0

    for i in range(args.iterations):
        knobs = _ask_claude(
            client=client,
            baseline_ms=baseline,
            best_ms=best_ms,
            iteration=i,
            trial_log=trial_log,
        )

        if knobs is None:
            rejected_count += 1
            print(f"[agent {i}] skipped — Claude response could not be parsed", flush=True)
            continue

        result = evaluate(knobs, streams=args.streams, warmup=args.warmup)
        record = TrialRecord(
            iteration=i,
            knobs=knobs,
            result=result,
            arm="agent",
            notes="claude-suggested",
        )
        log_trial(record, log_file)

        entry = {
            "iteration": i,
            "verifier_ok": result.verifier_ok,
            "ms_per_stream": result.ms_per_stream,
            "rejection_reason": result.rejection_reason,
            "knobs": asdict(knobs),
        }
        trial_log.append(entry)

        if result.verifier_ok and result.ms_per_stream is not None:
            if result.ms_per_stream < best_ms:
                best_ms = result.ms_per_stream
                if baseline > 0.0:
                    delta_pct = (baseline - best_ms) / baseline * 100.0
                    print(
                        f"[agent {i}] new best: {best_ms:.4f} ms/stream "
                        f"(delta: {delta_pct:.1f}%)",
                        flush=True,
                    )
                else:
                    print(
                        f"[agent {i}] new best: {best_ms:.4f} ms/stream",
                        flush=True,
                    )
        else:
            reason = result.rejection_reason or "verifier failed"
            rejected_count += 1
            print(f"[agent {i}] rejected — {reason}", flush=True)

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
