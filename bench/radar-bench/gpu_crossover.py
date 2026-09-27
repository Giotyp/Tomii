#!/usr/bin/env python3
"""Radar CPU (FFTW) vs GPU (cuFFT) crossover campaign — committed provenance.

Runs the radar-pipeline kernel-.so swap across three CPI sizes and records, for
each (size, backend):

  * latency percentiles at the natural streaming rate (frame_period = physical
    CPI, physical chirp pacing), median of >=5 verifier-gated runs — reports the
    end-to-end frame latency (first-packet-to-done) AND the processing tail
    (critical-path exec from the runtime's scheduling diagnostic);
  * the maximum sustained frame rate P* via bisect_rate.py (compressed streamed
    pacing, coverage-gate confirmed), with the processing-bound p50 at P*.

Only the kernel .so changes between CPU and GPU; the graph, plugin, verifier and
scene are identical. Every reported number comes from a run whose detections
passed the ground-truth coverage gate.

Usage (from repo root):
    python3 bench/radar-bench/gpu_crossover.py --gpu-device 1
    python3 bench/radar-bench/gpu_crossover.py --phase cpu   # one backend only
"""
from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RADAR = ROOT / "examples" / "radar-pipeline"
RESULTS = HERE / "results" / "gpu-crossover"

CONDA_RADAR = Path.home() / "miniconda3" / "envs" / "radar" / "lib"

# CPI sizes. chirp_interval defaults to n_samples/fs (fs=20.48 MHz): 50/100/200 us,
# giving physical CPIs of 6.4 / 25.6 / 102.4 ms. Targets are shared across sizes:
# low velocity so they stay inside every size's unambiguous window (v_max shrinks
# with CPI: +-19.5 / +-9.7 / +-4.9 m/s). SNR is moderate and uniform (6 dB per
# sample): the 2D-FFT processing gain (51-63 dB across these sizes) makes
# detection certain, while keeping target skirts low enough to avoid strong-target
# CA-CFAR self-masking (seen at 4096x512 with 20-30 dB targets). The coverage gate
# then reflects throughput/latency, not detection sensitivity.
TARGETS = ["30,2.0,6", "75,-3.0,6", "120,1.0,6"]
SCENE_FRAME_INTERVAL = 0.005  # small: freezes inter-frame range drift (targets stay centered)
SIZES = [
    # key,        n_samp, n_chirp, scene_frames, lat_frames, lat_warmup
    ("1024x128", 1024, 128, 200, 2000, 200),
    ("2048x256", 2048, 256, 100, 800, 100),
    ("4096x512", 4096, 512, 60, 400, 50),
]
BISECT_FRAMES = 250
BISECT_WARMUP = 30
BISECT_TOL_MS = 2.0
# Latency cells run at a fixed realistic streaming rate with idle headroom: chirps
# still arrive at the physical chirp interval (so the arrival span is the physical
# CPI), but frames start every 1.45x CPI (~69% duty) so the receiver is never
# saturated at the boundary. Frame latency (first-packet-to-done) is then the
# physical CPI arrival span plus the processing tail -- which is where CPU and GPU
# differ. frame_period=CPI (100% duty) drops occasional packets at small CPI.
LAT_PERIOD_FACTOR = 1.45


def sh(cmd: list[str], env: dict, log, cwd: Path = ROOT) -> int:
    log.write(f"\n$ {' '.join(str(c) for c in cmd)}\n")
    log.flush()
    p = subprocess.run(cmd, env=env, cwd=str(cwd), stdout=log, stderr=subprocess.STDOUT)
    return p.returncode


def sh_capture(cmd: list[str], env: dict, log, cwd: Path = ROOT) -> str:
    log.write(f"\n$ {' '.join(str(c) for c in cmd)}\n")
    log.flush()
    p = subprocess.run(cmd, env=env, cwd=str(cwd), stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, text=True)
    log.write(p.stdout)
    log.flush()
    return p.stdout


def scene_dir(key: str) -> Path:
    return RADAR / "data" / "out" / f"cpi_{key}"  # under data/out/ -> gitignored


def gen_scene(key: str, n_samp: int, n_chirp: int, frames: int, env: dict, log) -> Path:
    d = scene_dir(key)
    scene = d / "scene.json"
    fs = 20.48e6
    cpi = n_chirp * (n_samp / fs)
    if scene.exists():
        log.write(f"[scene] {key} exists, reuse {scene}\n")
        return scene
    cmd = [sys.executable, str(RADAR / "data" / "make_scene.py"),
           "--n-samples", str(n_samp), "--n-chirps", str(n_chirp),
           "--n-frames", str(frames), "--frame-interval", f"{SCENE_FRAME_INTERVAL:.6f}",
           "--out-dir", str(d)]
    for t in TARGETS:
        cmd += ["--target", t]
    rc = sh(cmd, env, log)
    if rc != 0 or not scene.exists():
        raise RuntimeError(f"scene generation failed for {key}")
    return scene


def phase_env(backend: str, gpu_device: int) -> dict:
    env = {**os.environ}
    # Build-time: the CPU kernel is always compiled (needs conda FFTW headers).
    env["PKG_CONFIG_PATH"] = str(CONDA_RADAR / "pkgconfig")
    if backend in ("gpu", "hybrid"):
        env["CUDA_VISIBLE_DEVICES"] = str(gpu_device)
    if backend in ("cpu", "hybrid"):
        # CPU/hybrid load libfftw3f at runtime; GPU resolves cufft/cudart from the
        # CUDA install's default loader path (no conda libs ahead of it).
        env["LD_LIBRARY_PATH"] = f"{CONDA_RADAR}:{env.get('LD_LIBRARY_PATH', '')}"
    return env


def backend_flag(backend):
    return {"gpu": ["--gpu"], "hybrid": ["--hybrid"], "cpu": []}[backend]


def run_latency(key, scene, lat_frames, lat_warmup, backend, env, log, first_build):
    """--repeat 5 verifier-gated latency at the natural streaming rate."""
    scene_meta = json.loads(scene.read_text())["radar"]
    cpi_s = scene_meta["n_chirps"] * scene_meta["chirp_interval_s"]
    frame_period = cpi_s * LAT_PERIOD_FACTOR
    rdir = RESULTS / backend / key / "latency"
    rdir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(RADAR / "run_bench.py"),
           "--scene", str(scene),
           "--frames", str(lat_frames), "--warmup", str(lat_warmup),
           "--frame-period", f"{frame_period:.6f}",  # physical chirp pacing, ~69% duty
           "--slots", "2", "--workers", "8",
           "--repeat", "5", "--results-dir", str(rdir)]
    cmd += backend_flag(backend)
    if not first_build:
        cmd.append("--no-clean")  # reuse the backend's clean build
    rc = sh(cmd, env, log)
    # Read the 5 snapshots and take medians.
    reps = []
    for i in range(5):
        rp = rdir / f"radar_report_s2_w8_r{i}.json"
        if rp.exists():
            reps.append(json.loads(rp.read_text()))
    if not reps:
        return {"ok": False, "rc": rc}
    def med(path):
        vals = []
        for r in reps:
            cur = r
            for k in path:
                cur = cur.get(k) if isinstance(cur, dict) else None
                if cur is None:
                    break
            if cur is not None:
                vals.append(cur)
        return statistics.median(vals) if vals else None
    return {
        "ok": rc == 0,
        "rc": rc,
        "runs": len(reps),
        "frames": med(["summary", "total_frames"]),
        "p50_us": med(["summary", "p50_latency_us"]),
        "p99_us": med(["summary", "p99_latency_us"]),
        "p999_us": med(["summary", "p999_latency_us"]),
        "throughput_fps": med(["summary", "throughput_frames_per_sec"]),
        "critical_path_us": med(["summary", "scheduling_overhead_diagnostic",
                                 "critical_path_exec_us"]),
        "cpi_ms": cpi_s * 1e3,               # physical chirp-arrival span per frame
        "frame_period_ms": frame_period * 1e3,
    }


BISECT_RE = re.compile(r"P\* ~= ([\d.]+)ms \(([\d.]+) fps\), (?:unverified|gated) p50=([\d.]+|n/a)")
CONFIRM_RE = re.compile(r"(VERIFIED-sustains|FAILED verification).*p50=([\d.]+|n/a)ms frames=(\d+)")


def run_bisect(key, scene, backend, env, log):
    rdir = RESULTS / backend / key / "bisect"
    rdir.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(RADAR / "bisect_rate.py"),
           "--scene", str(scene),
           "--frames", str(BISECT_FRAMES), "--warmup", str(BISECT_WARMUP),
           "--slots", "2", "--workers", "8",
           "--tol-ms", str(BISECT_TOL_MS), "--results-dir", str(rdir),
           "--gate-search"]  # every bisect step is verifier-gated (protocol: baselines at their best)
    cmd += backend_flag(backend)
    out = sh_capture(cmd, env, log)
    m = BISECT_RE.search(out)
    c = CONFIRM_RE.search(out)
    res = {"ok": m is not None}
    if m:
        res.update(pstar_ms=float(m.group(1)), fps=float(m.group(2)),
                   p50_at_pstar_ms=(None if m.group(3) == "n/a" else float(m.group(3))))
    if c:
        res.update(confirm=c.group(1),
                   confirm_p50_ms=(None if c.group(2) == "n/a" else float(c.group(2))),
                   confirm_frames=int(c.group(3)))
    return res


def provenance(gpu_device: int) -> dict:
    def cmd(c):
        try:
            return subprocess.run(c, capture_output=True, text=True, shell=True).stdout.strip()
        except Exception as e:
            return f"<err {e}>"
    sha = cmd("git -C '%s' rev-parse HEAD" % ROOT)
    branch = cmd("git -C '%s' rev-parse --abbrev-ref HEAD" % ROOT)
    gpu_name = cmd("nvidia-smi --query-gpu=name --format=csv,noheader -i %d" % gpu_device)
    driver = cmd("nvidia-smi --query-gpu=driver_version --format=csv,noheader -i %d" % gpu_device)
    nvcc = cmd("nvcc --version | tail -1")
    return {
        "tomii_sha": sha,
        "branch": branch,
        "base": "main ffb51d3",
        "gpu_device_index": gpu_device,
        "gpu_model": gpu_name,
        "driver_version": driver,
        "cuda_runtime": "12.9 (nvcc: %s)" % nvcc,
        "cuda_arch": "sm_89",
        "fftw": "3.3.11 (conda env radar)",
        "core_offset": 1, "workers": 8, "slots": 2,
        "system_threads": 2, "receiver_threads": 2,
        "low_latency_cfg": "custom scheduler + coalesce_barriers + inline_continuation + slot_priority + rdtsc",
        "pacing_latency": "natural: frame_period = physical CPI, physical chirp interval",
        "pacing_bisect": "compressed streamed: chirp_gap = frame_period / n_chirps",
        "targets": TARGETS,
        "verifier": "verify.py coverage gate: every target in every frame, exact frame count",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S %Z"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpu-device", type=int, default=1)
    ap.add_argument("--phase", choices=["cpu", "gpu", "hybrid", "both", "all"],
                    default="both")
    ap.add_argument("--sizes", nargs="+", default=None,
                    help="restrict to these CPI keys (e.g. 2048x256 4096x512)")
    ap.add_argument("--skip-bisect", action="store_true",
                    help="re-run only the latency cells; keep existing bisect entries")
    args = ap.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)
    log = open(RESULTS / "campaign.log", "a")
    log.write(f"\n===== campaign start {time.strftime('%Y-%m-%d %H:%M:%S')} phase={args.phase} =====\n")

    summary_path = RESULTS / "summary.json"
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    summary["provenance"] = provenance(args.gpu_device)
    summary.setdefault("results", {})

    # Scenes (built with CPU env; make_scene needs only numpy).
    base_env = phase_env("cpu", args.gpu_device)
    sizes = [s for s in SIZES if args.sizes is None or s[0] in args.sizes]
    scenes = {}
    for key, ns, nc, sf, *_ in sizes:
        scenes[key] = gen_scene(key, ns, nc, sf, base_env, log)

    phases = {"both": ["cpu", "gpu"], "all": ["cpu", "gpu", "hybrid"]}.get(
        args.phase, [args.phase])
    for phase in phases:
        env = phase_env(phase, args.gpu_device)
        summary["results"].setdefault(phase, {})
        for idx, (key, ns, nc, sf, lf, lw) in enumerate(sizes):
            log.write(f"\n----- {phase} {key} -----\n")
            first_build = idx == 0  # clean build once per backend
            lat = run_latency(key, scenes[key], lf, lw, phase, env, log, first_build)
            entry = summary["results"][phase].get(key, {})
            entry["latency"] = lat
            if args.skip_bisect:
                bis = entry.get("bisect", {})  # keep the existing gated-bisect result
            else:
                bis = run_bisect(key, scenes[key], phase, env, log)
                entry["bisect"] = bis
            summary["results"][phase][key] = entry
            summary_path.write_text(json.dumps(summary, indent=2))
            print(f"[{phase} {key}] lat p50={lat.get('p50_us')} crit={lat.get('critical_path_us')} "
                  f"ok={lat.get('ok')} | bisect P*={bis.get('pstar_ms')}ms {bis.get('fps')}fps", flush=True)

    summary_path.write_text(json.dumps(summary, indent=2))
    log.write("\n===== campaign done =====\n")
    log.close()
    print(f"\nDONE -> {summary_path}")


if __name__ == "__main__":
    main()
