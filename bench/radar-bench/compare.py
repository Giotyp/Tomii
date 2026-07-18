#!/usr/bin/env python3
"""Tomii vs GNU Radio 3.10 radar-pipeline comparison (verifier-gated).

Runs both systems over the same scene and sender pacing, verifies detections
against ground truth (identical tolerance), and prints per-frame latency
percentiles. Assumes Tomii + kernels are already built (run the Tomii example
once first) and a `gnuradio` conda env exists (see gnuradio/radar_rx.py).

Usage: python3 bench/radar-bench/compare.py --frames 500
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RADAR = ROOT / "examples" / "radar-pipeline"
GR_PY = Path.home() / "miniconda3" / "envs" / "gnuradio" / "bin" / "python"


def pct(sorted_v, p):
    return sorted_v[min(round(p / 100 * (len(sorted_v) - 1)), len(sorted_v) - 1)]


def run_tomii(frames, period, warmup):
    subprocess.run(
        [sys.executable, str(RADAR / "run_bench.py"), "--no-clean",
         "--frames", str(frames), "--frame-period", str(period),
         "--warmup", str(warmup)],
        check=True,
    )
    rep = json.loads((RADAR / "results" / "radar_report_s2_w8.json").read_text())
    lat = rep["frame_latencies_us"]
    return {"system": "tomii", "frames": len(lat), "lat": sorted(lat)}


def run_gnuradio(frames, period, warmup, cpp=False):
    out_dir = HERE / "gnuradio" / "results"
    if cpp:
        cmd = [str(HERE / "gnuradio" / "radar_rx_cpp"), "8101", "1024", "128",
               "8", "2", "8", "15.0", str(frames),
               str(out_dir / "gr_detections.txt"), str(out_dir / "gr_latency.csv")]
    else:
        cmd = [str(GR_PY), str(HERE / "gnuradio" / "radar_rx.py"),
               "--frames", str(frames), "--port", "8101"]
    gr = subprocess.Popen(cmd)
    time.sleep(6)
    subprocess.run(
        [sys.executable, str(RADAR / "sender.py"), "--frames", str(frames),
         "--frame-period", str(period), "--port", "8101", "--quiet"],
        check=True,
    )
    gr.wait(timeout=600)
    rc = subprocess.run(
        [sys.executable, str(RADAR / "verify.py"),
         "--detections", str(out_dir / "gr_detections.txt")],
    ).returncode
    if rc != 0:
        raise RuntimeError("GNU Radio detections failed verification")
    lat = []
    for line in (out_dir / "gr_latency.csv").read_text().splitlines()[1:]:
        fid, us = line.split(",")
        if int(fid) >= warmup and int(fid) != frames - 1:  # last frame: EOS flush artifact
            lat.append(float(us))
    return {"system": "gnuradio-3.10", "frames": len(lat), "lat": sorted(lat)}


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--frames", type=int, default=500)
    p.add_argument("--frame-period", type=float, default=0.01)
    p.add_argument("--warmup", type=int, default=20)
    p.add_argument("--skip-tomii", action="store_true")
    p.add_argument("--repeats", type=int, default=5, help="median-of-N runs")
    args = p.parse_args()

    import statistics

    systems = ([] if args.skip_tomii else [("tomii", run_tomii)]) + [
        ("gnuradio-3.10", run_gnuradio)
    ]
    print(f"\n{'system':<14} {'runs':>4} {'frames':>6} {'p50 us':>9} "
          f"{'p99 us':>9} {'p99.9 us':>9} {'max us':>9}   (median of runs)")
    for name, fn in systems:
        reps = []
        for rep in range(args.repeats):
            r = fn(args.frames, args.frame_period, args.warmup)
            v = r["lat"]
            reps.append((pct(v, 50), pct(v, 99), pct(v, 99.9), v[-1], r["frames"]))
        med = [statistics.median(x[i] for x in reps) for i in range(4)]
        print(f"{name:<14} {len(reps):>4} {reps[0][4]:>6} {med[0]:>9.0f} "
              f"{med[1]:>9.0f} {med[2]:>9.0f} {med[3]:>9.0f}")


if __name__ == "__main__":
    main()
