"""E8 pre-flight: prove each workload's per-trial gate is hard-gating.

For every workload, run (under the measurement lock):
  * positive controls — the default configuration, and the configuration
    that tripped the fanout-bulk bug #21 (inline_continuation off, fanout-bulk
    on), must PASS on the fixed runtime;
  * a negative control — the same default configuration with the reference
    deliberately corrupted must be REJECTED.

    stream-analytics  golden file with one digit changed
    pipeline          reference mean computed with 2x TRANSFORM_ITERS
    mimo              sender emits 10 fewer frames than the receiver expects
                      (the keep-up / frame-count soundness gate)

Also prints the objective each workload reports (mean per-frame latency).

Usage: python gate_check.py [--workloads stream-analytics pipeline mimo]
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from harness import server_lock
from workloads import get_workload


def _run(w, knobs, frames, warmup):
    with server_lock():
        return w.evaluate(knobs, frames=frames, warmup=warmup)


def check(name: str) -> list[dict]:
    w = get_workload(name)
    w.knob_space()
    frames, warmup = (500, 50) if name == "stream-analytics" else (200, 20)
    base = dict(w.baseline_knobs)
    bug21 = {**base, "inline_continuation": False, "no_fanout_bulk": False}
    rows = []

    for label, knobs in (("default", base), ("bug21-config", bug21)):
        r = _run(w, knobs, frames, warmup)
        rows.append(
            {"workload": name, "control": label, "expect": "pass",
             "verifier_ok": r.verifier_ok, "ms_per_frame": r.ms_per_frame,
             "reason": r.rejection_reason}
        )

    if name == "stream-analytics":
        # Work-reduction control: 8 of 32 readings still matches the golden
        # file (outputs are per-sensor constants); the invariant must reject.
        r = _run(w, {**base, "graph:init.total_readings": 8}, frames, warmup)
        rows.append(
            {"workload": name, "control": "work-reduction(8/32)", "expect": "reject",
             "verifier_ok": r.verifier_ok, "ms_per_frame": r.ms_per_frame,
             "reason": r.rejection_reason}
        )

    # Negative control.
    if name == "stream-analytics":
        tmp = Path(tempfile.mkdtemp(prefix="e8_gate_"))
        golden = (w.root / "result.golden.txt").read_text()
        idx = next(i for i, c in enumerate(golden) if c.isdigit())
        bad = golden[:idx] + str((int(golden[idx]) + 1) % 10) + golden[idx + 1 :]
        (tmp / "result.golden.txt").write_text(bad)
        orig_root = w.root
        w.root = tmp
        try:
            r = _run(w, base, frames, warmup)
        finally:
            w.root = orig_root
            shutil.rmtree(tmp)
    elif name == "pipeline":
        orig = w.transform_iters
        w.transform_iters = orig * 2
        try:
            r = _run(w, base, frames, warmup)
        finally:
            w.transform_iters = orig
    else:  # mimo
        orig = w._sender_config_for

        def short(num_frames, tmp_dir):
            return orig(num_frames - 10, tmp_dir)

        w._sender_config_for = short
        try:
            r = _run(w, base, frames, warmup)
        finally:
            w._sender_config_for = orig
    rows.append(
        {"workload": name, "control": "corrupted-reference", "expect": "reject",
         "verifier_ok": r.verifier_ok, "ms_per_frame": r.ms_per_frame,
         "reason": r.rejection_reason}
    )
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--workloads", nargs="+", default=["stream-analytics", "pipeline", "mimo"]
    )
    p.add_argument("--out", type=Path, default=Path("results/e8/gate_check.json"))
    args = p.parse_args()
    rows = []
    for name in args.workloads:
        rows += check(name)
    ok = all((r["verifier_ok"] is True) == (r["expect"] == "pass") for r in rows)
    for r in rows:
        print(
            f"{r['workload']:17s} {r['control']:20s} expect={r['expect']:6s} "
            f"ok={r['verifier_ok']!s:5s} ms={r['ms_per_frame']} "
            f"reason={(r['reason'] or '')[:90]}"
        )
    print("GATES HARD:", ok)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({"all_gates_hard": ok, "rows": rows}, indent=2))


if __name__ == "__main__":
    main()
