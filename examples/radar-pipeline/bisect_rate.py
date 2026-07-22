#!/usr/bin/env python3
"""Find the sustained-frame-rate boundary for the radar pipeline (Regime C).

Drives run_bench.py under *compressed streamed pacing*: for a target frame
period P, chirps are spread evenly across P (chirp gap = P / n_chirps) so the
packet stream is steady and never bursts the receiver socket buffer. Frame
period is then bisected downward to find P*, the smallest period the system
sustains. "Sustains" = the run exits cleanly AND the JSON report shows (close
to) every expected steady frame; a wedge/overload manifests as a watchdog kill
(non-zero exit) or a missing/short report, both of which are treated as
"did NOT sustain" rather than an error (the boundary is bistable, not smooth).

Because the crossover latency numbers come from --no-verify runs, the boundary
P* is re-confirmed with the coverage gate ON so the headline rests on a
*verified* sustaining run (--confirm, default on).

    python3 examples/radar-pipeline/bisect_rate.py --scene data/out_big/scene.json
    python3 examples/radar-pipeline/bisect_rate.py --gpu --scene data/out_big/scene.json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def run_at_period(args, period: float, *, verify: bool) -> tuple[bool, float | None, int]:
    """Run one cell at frame period `period`. Returns (sustained, p50_us, frames)."""
    gap_us = period / args.n_chirps * 1e6
    tag = f"s{args.slots}_w{args.workers}"
    report = args.results_dir / f"radar_report_{tag}.json"
    # Delete any stale report so a killed (wedged) run can't read the previous
    # run's numbers as if they were this run's.
    if report.exists():
        report.unlink()

    cmd = [
        sys.executable, str(HERE / "run_bench.py"),
        "--scene", str(args.scene),
        "--frames", str(args.frames),
        "--warmup", str(args.warmup),
        "--frame-period", f"{period:.6f}",
        "--chirp-gap-us", f"{gap_us:.3f}",
        "--slots", str(args.slots),
        "--workers", str(args.workers),
        "--results-dir", str(args.results_dir),
        "--no-clean",  # kernels/plugin already built by the first (build) run
    ]
    if args.gpu:
        cmd.append("--gpu")
    if not verify:
        cmd.append("--no-verify")

    expected = args.frames - args.warmup
    rc = subprocess.run(cmd).returncode

    if not report.exists():
        return False, None, 0
    rep = json.loads(report.read_text())
    frames = rep["summary"]["total_frames"]
    p50 = rep["summary"]["p50_latency_us"]
    # Sustained only if it exited cleanly AND covered (nearly) every steady frame.
    sustained = (rc == 0) and (frames >= expected * args.min_coverage)
    if verify:
        # Under the gate a non-zero exit already means a coverage/detection FAIL.
        sustained = rc == 0
    return sustained, p50, frames


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--scene", type=Path, required=True)
    p.add_argument("--gpu", action="store_true")
    p.add_argument("--frames", type=int, default=400)
    p.add_argument("--warmup", type=int, default=50)
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--hi", type=float, default=None,
                   help="upper frame period [s] that should sustain (default: physical CPI)")
    p.add_argument("--lo", type=float, default=0.004,
                   help="lower frame period [s] that should NOT sustain")
    p.add_argument("--tol-ms", type=float, default=1.0,
                   help="stop bisecting when the bracket is this narrow [ms]")
    p.add_argument("--min-coverage", type=float, default=0.99,
                   help="fraction of expected steady frames required to call it sustained")
    p.add_argument("--no-confirm", dest="confirm", action="store_false", default=True,
                   help="skip the verified re-run at the discovered P*")
    p.add_argument("--results-dir", type=Path, default=HERE / "results")
    args = p.parse_args()

    scene = json.loads(args.scene.read_text())["radar"]
    args.n_chirps = scene["n_chirps"]
    cpi = args.n_chirps * scene["chirp_interval_s"]
    if args.hi is None:
        args.hi = round(cpi, 6)
    label = "GPU" if args.gpu else "CPU"
    args.results_dir.mkdir(parents=True, exist_ok=True)

    print(f"[{label}] scene {args.scene.name}: n_chirps={args.n_chirps} "
          f"physical CPI={cpi*1e3:.1f}ms; bisecting frame period in "
          f"[{args.lo*1e3:.1f}, {args.hi*1e3:.1f}] ms", flush=True)

    hi, lo = args.hi, args.lo  # hi sustains, lo does not (invariant we maintain)
    s_hi, p50_hi, f_hi = run_at_period(args, hi, verify=False)
    print(f"[{label}] P={hi*1e3:6.1f}ms -> sustained={s_hi} "
          f"p50={p50_hi and p50_hi/1e3:.2f}ms frames={f_hi}", flush=True)
    if not s_hi:
        print(f"[{label}] WARNING: upper bound {hi*1e3:.1f}ms did not sustain; "
              f"raise --hi. Aborting.", flush=True)
        return

    best_sustained = hi
    p50_at_best = p50_hi
    while (hi - lo) * 1e3 > args.tol_ms:
        mid = (hi + lo) / 2.0
        sustained, p50, frames = run_at_period(args, mid, verify=False)
        print(f"[{label}] P={mid*1e3:6.1f}ms -> sustained={sustained} "
              f"p50={p50 and p50/1e3:.2f}ms frames={frames}", flush=True)
        if sustained:
            hi = mid
            best_sustained = mid
            p50_at_best = p50
        else:
            lo = mid

    print(f"\n[{label}] sustained-rate boundary P* ~= {best_sustained*1e3:.1f}ms "
          f"({1.0/best_sustained:.1f} fps), unverified p50={p50_at_best/1e3:.2f}ms",
          flush=True)

    if args.confirm:
        print(f"[{label}] confirming with coverage gate ON at P*={best_sustained*1e3:.1f}ms ...",
              flush=True)
        ok, p50, frames = run_at_period(args, best_sustained, verify=True)
        verdict = "VERIFIED-sustains" if ok else "FAILED verification (nudge P* up)"
        print(f"[{label}] {verdict}: p50={p50 and p50/1e3:.2f}ms frames={frames}", flush=True)


if __name__ == "__main__":
    main()
