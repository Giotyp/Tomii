#!/usr/bin/env python3
"""Firm-up of the shared-GPU result: 3/3-robust concurrency sweep with and without
CUDA MPS (Part B), and a 5-repeat adversarial-hog measurement (Part C). Reuses the
harness in shared_gpu.py. See RESULTS-GPU.md "Shared-GPU" section.

MPS is scoped to this session's processes on GPU 1 via a private pipe directory, so
other users' jobs on the box are unaffected unless they opt into the same pipe. The
daemon is stopped and the pipe removed on exit.
"""
from __future__ import annotations
import argparse, json, os, shutil, statistics, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import shared_gpu as sg  # noqa: E402

RES = sg.RES  # bench/radar-bench/results/gpu-crossover/shared_gpu
MPS_PIPE = "/tmp/tomii_mps_pipe"
MPS_LOG = "/tmp/tomii_mps_log"

# 3/3 sweep grids (descending per-pipeline period ms), wide enough to bracket both
# backends' boundaries at each (size, N).
GRIDS = {
    "2048x256": {1: [8, 7, 6.5, 6], 2: [18, 16, 14, 12, 10], 4: [34, 30, 26, 22, 20]},
    "4096x512": {1: [26, 24, 22, 20], 2: [56, 52, 48, 44], 4: [116, 108, 100, 92, 84]},
}


def mps_env(base, on):
    if not on:
        return base
    # The daemon was started with CUDA_VISIBLE_DEVICES=1, so it manages GPU 1 and
    # exposes it to clients as device index 0. Clients must therefore select 0.
    e = {**base, "CUDA_VISIBLE_DEVICES": "0",
         "CUDA_MPS_PIPE_DIRECTORY": MPS_PIPE, "CUDA_MPS_LOG_DIRECTORY": MPS_LOG}
    return e


def start_mps(log):
    os.makedirs(MPS_PIPE, exist_ok=True)
    os.makedirs(MPS_LOG, exist_ok=True)
    e = {**os.environ, "CUDA_VISIBLE_DEVICES": "1",
         "CUDA_MPS_PIPE_DIRECTORY": MPS_PIPE, "CUDA_MPS_LOG_DIRECTORY": MPS_LOG}
    subprocess.run(["nvidia-cuda-mps-control", "-d"], env=e,
                   stdout=log, stderr=subprocess.STDOUT)
    time.sleep(3)
    log.write("[mps] daemon started\n"); log.flush()


def stop_mps(log):
    e = {**os.environ, "CUDA_MPS_PIPE_DIRECTORY": MPS_PIPE, "CUDA_MPS_LOG_DIRECTORY": MPS_LOG}
    try:
        subprocess.run(["nvidia-cuda-mps-control"], input="quit\n", text=True,
                       env=e, timeout=15, stdout=log, stderr=subprocess.STDOUT)
    except Exception as ex:
        log.write(f"[mps] stop error {ex}\n")
    time.sleep(2)
    for d in (MPS_PIPE, MPS_LOG):
        shutil.rmtree(d, ignore_errors=True)
    log.write("[mps] daemon stopped\n"); log.flush()


def sweep_3of3(backend, size, N, env, dylib, binary, frames, warmup, mlab, log):
    ns, nc = sg.SIZES[size][1], sg.SIZES[size][2]
    best = None
    for pm in GRIDS[size][N]:            # descending: getting harder
        ok3 = True
        for rep in range(3):
            r = sg.run_set(backend, size, ns, nc, N, pm / 1e3, frames, warmup,
                           dylib, binary, env, f"FB_{mlab}_{backend}_{size}_N{N}_{pm}_r{rep}", log)
            if not r["all_pass"]:
                ok3 = False
                break
        if ok3:
            best = {"period_ms": pm, "agg_fps": round(N / (pm / 1e3), 1),
                    "per_pipe_fps": round(1000 / pm, 1)}
        else:
            break
    return best


def hog_5rep(backend, size, N, env, dylib, binary, frames, warmup, log):
    ns, nc = sg.SIZES[size][1], sg.SIZES[size][2]
    per = sg.FIXED_PERIOD_MS[size] / 1e3
    hog = sg.start_hog(log)
    time.sleep(4)
    reps = []
    for rep in range(5):
        r = sg.run_set(backend, size, ns, nc, N, per, frames, warmup, dylib, binary,
                       env, f"FC_{backend}_{size}_N{N}_r{rep}", log, sample_gpu=(rep == 0))
        s = sg.summarize(r["per_pipe"])
        reps.append({"all_pass": r["all_pass"], "min_frames": s.get("min_frames", 0),
                     "p50_ms": s.get("p50_ms"), "p99_ms": s.get("p99_ms"),
                     "p999_ms": s.get("p999_ms")})
    hog.terminate()
    try:
        hog.wait(timeout=5)
    except subprocess.TimeoutExpired:
        hog.kill()
    def med(k):
        v = [r[k] for r in reps if r.get(k) is not None]
        return round(statistics.median(v), 2) if v else None
    return {"n_pass": sum(r["all_pass"] for r in reps), "reps": 5,
            "p50_ms": med("p50_ms"), "p99_ms": med("p99_ms"), "p999_ms": med("p999_ms"),
            "min_frames_med": med("min_frames"), "per_rep": reps}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", default=["2048x256", "4096x512"])
    ap.add_argument("--ns", nargs="+", type=int, default=[1, 2, 4])
    ap.add_argument("--backends", nargs="+", default=["gpu", "hybrid"])
    ap.add_argument("--frames", type=int, default=150)
    ap.add_argument("--warmup", type=int, default=30)
    ap.add_argument("--parts", nargs="+", default=["B", "C"])
    ap.add_argument("--mps", nargs="+", default=["off", "on"])
    args = ap.parse_args()
    RES.mkdir(parents=True, exist_ok=True)
    log = open(RES / "firm.log", "a")
    op = RES / "firm_results.json"
    out = json.loads(op.read_text()) if op.exists() else {"B": {}, "C": {}}

    for backend in args.backends:
        dylib, binary = sg.build_backend(backend, log)
        base = sg.base_env(backend)
        # Part B: with/without MPS
        if "B" in args.parts:
            for m in args.mps:
                on = (m == "on")
                if on:
                    start_mps(log)
                env = mps_env(base, on)
                try:
                    for size in args.sizes:
                        for N in args.ns:
                            b = sweep_3of3(backend, size, N, env, dylib, binary,
                                           args.frames, args.warmup, m, log)
                            out["B"].setdefault(m, {}).setdefault(size, {}) \
                               .setdefault(backend, {})[str(N)] = b
                            op.write_text(json.dumps(out, indent=2))
                            print(f"[B mps={m} {backend} {size} N={N}] {b}", flush=True)
                finally:
                    if on:
                        stop_mps(log)
        # Part C: 5-rep hog (no MPS)
        if "C" in args.parts:
            env = base
            for size in args.sizes:
                for N in [1, 2]:
                    c = hog_5rep(backend, size, N, env, dylib, binary,
                                 args.frames, args.warmup, log)
                    out["C"].setdefault(size, {}).setdefault(backend, {})[str(N)] = c
                    op.write_text(json.dumps(out, indent=2))
                    print(f"[C hog {backend} {size} N={N}] pass={c['n_pass']}/5 "
                          f"p50={c['p50_ms']} p99={c['p99_ms']} p999={c['p999_ms']} "
                          f"minframes={c['min_frames_med']}", flush=True)
    op.write_text(json.dumps(out, indent=2))
    print(f"DONE -> {op}")
    log.close()


if __name__ == "__main__":
    main()
