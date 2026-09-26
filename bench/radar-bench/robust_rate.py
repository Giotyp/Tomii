#!/usr/bin/env python3
"""Robust max-sustained-rate: fastest frame period passing 3/3 verifier-gated
repeats, per CPI size and backend (cpu / gpu / hybrid).

The single-run gated bisect (bisect_rate.py --gate-search) over-reports because the
sustained-rate boundary is bistable near P* (a period can pass once and fail on a
re-run). This tool requires N consecutive gated passes at a period, stepping down a
grid until the first failure, so the reported rate is defensible.

    python3 bench/radar-bench/robust_rate.py --gpu-device 1
"""
from __future__ import annotations
import argparse, json, os, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RADAR = ROOT / "examples" / "radar-pipeline"
CONDA = Path.home() / "miniconda3" / "envs" / "radar" / "lib"

# key -> (scene subdir, n_chirps, descending period grid [ms])
GRIDS = {
    "1024x128": ("cpi_1024x128", 128, [7.0, 6.5, 6.0, 5.6, 5.2, 4.8]),
    "2048x256": ("cpi_2048x256", 256, [11, 10, 9, 8, 7, 6.5, 6.0]),
    "4096x512": ("cpi_4096x512", 512, [26, 24, 22, 20, 18, 16]),
}
BACKENDS = ["cpu", "gpu", "hybrid"]


def env_for(be, dev):
    e = {**os.environ, "PKG_CONFIG_PATH": str(CONDA.parent / "pkgconfig")}
    if be in ("gpu", "hybrid"):
        e["CUDA_VISIBLE_DEVICES"] = str(dev)
    if be in ("cpu", "hybrid"):
        e["LD_LIBRARY_PATH"] = f"{CONDA}:{os.environ.get('LD_LIBRARY_PATH','')}"
    return e


def flag(be):
    return {"cpu": [], "gpu": ["--gpu"], "hybrid": ["--hybrid"]}[be]


def run(be, size, scene, nchirps, pm, first, repeat, frames, warmup, rdir, env, log):
    P = pm / 1e3
    gap_us = P / nchirps * 1e6
    d = rdir / be / size
    d.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, str(RADAR / "run_bench.py"), "--scene", str(scene),
           "--frames", str(frames), "--warmup", str(warmup),
           "--frame-period", f"{P:.6f}", "--chirp-gap-us", f"{gap_us:.3f}",
           "--slots", "2", "--workers", "8", "--repeat", str(repeat),
           "--results-dir", str(d)] + flag(be)
    if not first:
        cmd.append("--no-clean")
    rc = subprocess.run(cmd, env=env, cwd=str(ROOT), stdout=log,
                        stderr=subprocess.STDOUT).returncode
    p50 = None
    if rc == 0:
        ps = [json.loads((d / f"radar_report_s2_w8_r{i}.json").read_text())
              ["summary"]["p50_latency_us"] for i in range(repeat)
              if (d / f"radar_report_s2_w8_r{i}.json").exists()]
        p50 = sorted(ps)[len(ps) // 2] if ps else None
    return rc == 0, p50


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gpu-device", type=int, default=1)
    ap.add_argument("--sizes", nargs="+", default=list(GRIDS))
    ap.add_argument("--backends", nargs="+", default=BACKENDS)
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--frames", type=int, default=200)
    ap.add_argument("--warmup", type=int, default=30)
    args = ap.parse_args()
    rdir = ROOT / "bench/radar-bench/results/gpu-crossover/robust"
    rdir.mkdir(parents=True, exist_ok=True)
    log = open(rdir / "robust.log", "w")
    out = {}
    for be in args.backends:
        env = env_for(be, args.gpu_device)
        out[be] = {}
        first = True
        for size in args.sizes:
            sub, nchirps, grid = GRIDS[size]
            scene = RADAR / "data" / "out" / sub / "scene.json"
            best = None
            for pm in grid:
                ok, p50 = run(be, size, scene, nchirps, pm, first, args.repeat,
                              args.frames, args.warmup, rdir, env, log)
                first = False
                print(f"[{be} {size}] P={pm}ms -> {args.repeat}/{args.repeat}={ok} "
                      f"p50={None if p50 is None else round(p50/1e3,2)}ms", flush=True)
                if ok:
                    best = {"period_ms": pm, "fps": round(1000 / pm, 1),
                            "p50_ms": None if p50 is None else round(p50 / 1e3, 2)}
                else:
                    break
            out[be][size] = best
            (rdir / "robust.json").write_text(json.dumps(out, indent=2))
    print("\nROBUST P* (fastest %d/%d-passing period):" % (args.repeat, args.repeat))
    for be in args.backends:
        print(f"  {be:7s}: {out[be]}")
    log.close()


if __name__ == "__main__":
    main()
