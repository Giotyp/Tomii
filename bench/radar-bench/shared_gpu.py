#!/usr/bin/env python3
"""Shared-GPU concurrent-pipelines experiment: does launch-frugal mixed placement
let more radar pipelines share one GPU at GPU-class tail latency?

Runs N independent Tomii radar processes (N in {1,2,4}) that share ONE RTX 4090,
each pinned to a disjoint CPU core set with its own UDP port, sender, graph and
report. Compares all-GPU vs hybrid (CPU range + GPU doppler/cfar) placement — only
the kernel .so differs. Hypothesis: all-GPU launches ~n_chirps range kernels/frame
per pipeline, so N pipelines contend on the GPU launch/queue path; hybrid launches
~n_tiles/frame, freeing that contended resource.

Parts:
  A  fixed per-pipeline rate, N in {1,2,4}: per-pipeline p50/p99/p99.9 + jitter +
     verifier pass + coverage, and mean GPU utilization. Shows tail/coverage
     degradation vs N.
  B  aggregate max sustained rate: fastest per-pipeline period at which ALL N
     pipelines pass the gate (aggregate fps = N / period).
  C  adversarial variant: Part A with a background GPU hog (torch) on the device.

Only the kernel .so changes between backends (no graph/plugin/tomii-core change).
"""
from __future__ import annotations
import argparse, json, os, statistics, subprocess, sys, threading, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RADAR = ROOT / "examples" / "radar-pipeline"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(RADAR))
from tomii._runner import build_command  # noqa: E402
from build_graph import build_radar_graph, radar_dims_from_scene  # noqa: E402
import run_bench  # noqa: E402

# Override paths with TOMII_RADAR_CONDA_LIB (fftw env lib dir) and TOMII_HOG_PY
# (a python with CUDA torch, for the Part C GPU hog).
CONDA = Path(os.environ.get(
    "TOMII_RADAR_CONDA_LIB",
    str(Path.home() / "miniconda3" / "envs" / "radar" / "lib")))
HOG_PY = Path(os.environ.get(
    "TOMII_HOG_PY",
    str(Path.home() / "miniconda3" / "envs" / "verl" / "bin" / "python")))
RES = ROOT / "bench/radar-bench/results/gpu-crossover/shared_gpu"
SENDER_DELAY = 5
WORKERS = 4          # per pipeline; stride keeps N=4 on disjoint physical cores
CORE_STRIDE = 6
BASE_PORT = 8200
SIZES = {"4096x512": ("cpi_4096x512", 4096, 512),
         "2048x256": ("cpi_2048x256", 2048, 256)}


def base_env(backend, dev=1):
    e = {**os.environ}
    e["CUDA_VISIBLE_DEVICES"] = str(dev)
    e["LD_LIBRARY_PATH"] = f"{CONDA}:{os.environ.get('LD_LIBRARY_PATH','')}"
    e["PKG_CONFIG_PATH"] = str(CONDA / "pkgconfig")
    return e


def build_backend(backend, log, dev=1):
    """Clean-build the plugin+core for this backend once (kernel .so is dim-agnostic)."""
    os.environ["PKG_CONFIG_PATH"] = str(CONDA / "pkgconfig")
    os.environ["LD_LIBRARY_PATH"] = f"{CONDA}:{os.environ.get('LD_LIBRARY_PATH','')}"
    os.environ["CUDA_VISIBLE_DEVICES"] = str(dev)
    log.write(f"\n=== build {backend} ===\n"); log.flush()
    dylib, binary = run_bench.build_all(clean=True, gpu=(backend == "gpu"),
                                        hybrid=(backend == "hybrid"))
    return dylib, binary


def make_graph(size, n_samples, n_chirps, port, slots=2):
    g = build_radar_graph(n_samples, n_chirps, frame_wnd=slots, port=port,
                          address="127.0.0.1")
    p = RES / "graphs" / f"{size}_p{port}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(g.to_json())
    return p


class GpuSampler(threading.Thread):
    """Poll GPU utilization on device 1 while a run is in flight."""
    def __init__(self, dev=1, interval=0.25):
        super().__init__(daemon=True)
        self.dev, self.interval, self.stop_f, self.samples = dev, interval, False, []
    def run(self):
        while not self.stop_f:
            try:
                o = subprocess.run(["nvidia-smi",
                                    "--query-gpu=utilization.gpu,power.draw",
                                    "--format=csv,noheader,nounits", "-i", str(self.dev)],
                                   capture_output=True, text=True, timeout=2).stdout.strip()
                util, pwr = (x.strip() for x in o.split(","))
                self.samples.append((float(util), float(pwr)))
            except Exception:
                pass
            time.sleep(self.interval)
    def stop(self):
        self.stop_f = True
        self.join(timeout=2)
        if not self.samples:
            return {}
        u = [s[0] for s in self.samples]; p = [s[1] for s in self.samples]
        return {"gpu_util_mean": round(statistics.mean(u), 1),
                "gpu_util_max": max(u), "power_w_mean": round(statistics.mean(p), 1),
                "samples": len(u)}


def run_set(backend, size, n_samples, n_chirps, N, period_s, frames, warmup,
            dylib, binary, env, tag, log, sample_gpu=False, gpu_device=1):
    """Launch N concurrent pipelines; return per-pipeline stats + all_pass.
    gpu_device is the PHYSICAL device for nvidia-smi sampling (the launched procs'
    own CUDA_VISIBLE_DEVICES comes from `env`, which differs under MPS)."""
    d = RES / tag
    d.mkdir(parents=True, exist_ok=True)
    gap_us = period_s / n_chirps * 1e6
    max_rt = int(SENDER_DELAY + frames * period_s + 30)
    procs, senders, meta = [], [], []
    sampler = GpuSampler(dev=gpu_device) if sample_gpu else None
    for i in range(N):
        port = BASE_PORT + i
        graph = make_graph(size, n_samples, n_chirps, port)
        rep = d / f"report_{i}.json"; det = d / f"det_{i}.txt"; tim = d / f"tim_{i}.txt"
        if det.exists(): det.unlink()
        cmd = build_command(binary, str(graph), dylib, workers=WORKERS,
                            core_offset=1 + CORE_STRIDE * i, system_threads=2,
                            receiver_threads=2, slots=2, max_frames=frames,
                            exclude_frames=warmup, max_runtime=max_rt,
                            timing=str(tim), report=str(rep), use_rdtsc=True,
                            custom=True, coalesce_barriers=True,
                            inline_continuation=True, slot_priority=True)
        e = {**env, "TOMII_VERIFY_PATH": str(det)}
        lf = open(d / f"tomii_{i}.log", "w")
        procs.append(subprocess.Popen(cmd, env=e, stdout=lf, stderr=lf))
        meta.append((port, rep, det))
    time.sleep(SENDER_DELAY)
    if sampler:
        sampler.start()
    scene = RADAR / "data" / "out" / SIZES[size][0] / "scene.json"
    for i in range(N):
        sc = [sys.executable, str(RADAR / "sender.py"), "--scene", str(scene),
              "--host", "127.0.0.1", "--port", str(BASE_PORT + i),
              "--frames", str(frames), "--frame-period", f"{period_s:.6f}",
              "--chirp-gap-us", f"{gap_us:.3f}", "--quiet"]
        senders.append(subprocess.Popen(sc, env=env))
    watchdog = max_rt + 20
    for p in procs:
        try:
            p.wait(timeout=watchdog)
        except subprocess.TimeoutExpired:
            p.kill()
    for s in senders:
        if s.poll() is None:
            s.terminate()
    gpu = sampler.stop() if sampler else {}

    per = []
    for i, (port, rep, det) in enumerate(meta):
        st = {"pipe": i, "verified": False, "frames": 0}
        if rep.exists():
            s = json.loads(rep.read_text())["summary"]
            st.update(frames=s["total_frames"], p50_us=s["p50_latency_us"],
                      p99_us=s["p99_latency_us"], p999_us=s.get("p999_latency_us"),
                      thr_fps=s["throughput_frames_per_sec"],
                      crit_us=s["scheduling_overhead_diagnostic"]["critical_path_exec_us"])
        # Soft coverage gate: separate detection-correctness (verify.py without the
        # exact-frame coverage check) from coverage (>= COVERAGE_FRAC of sent
        # frames). This tolerates the 1-frame EOS-flush artifact (compare.py drops
        # the last frame for the same reason) while still failing on real overload
        # drops, which lose many frames.
        present = sum(1 for _ in open(det)) if det.exists() else 0
        rc = subprocess.run([sys.executable, str(RADAR / "verify.py"),
                             "--scene", str(scene), "--detections", str(det)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
        st["frames_present"] = present
        st["verified"] = (rc == 0) and (present >= COVERAGE_FRAC * frames)
        per.append(st)
    all_pass = all(p["verified"] for p in per)
    log.write(f"[{tag}] N={N} period={period_s*1e3:.1f}ms all_pass={all_pass} gpu={gpu}\n")
    log.flush()
    return {"all_pass": all_pass, "per_pipe": per, "gpu": gpu,
            "period_ms": period_s * 1e3, "aggregate_offered_fps": round(N / period_s, 1)}


def summarize(per):
    v = [p for p in per if p.get("p50_us") is not None]
    if not v:
        return {}
    return {"p50_ms": round(max(p["p50_us"] for p in v) / 1e3, 2),   # worst pipe
            "p99_ms": round(max(p["p99_us"] for p in v) / 1e3, 2),
            "p999_ms": round(max(p["p999_us"] for p in v) / 1e3, 2),
            "crit_ms": round(max(p["crit_us"] for p in v) / 1e3, 2),
            "min_frames": min(p["frames"] for p in per),
            "n_verified": sum(p["verified"] for p in per)}


def start_hog(log, dev=1):
    """Background GPU contender on `dev` (persistent large matmuls). CUDA_VISIBLE_DEVICES
    pins it to the one device, which is then index 0 inside the process."""
    code = ("import torch,time\n"
            "torch.cuda.set_device(0)\n"
            "a=torch.randn(4096,4096,device='cuda');b=torch.randn(4096,4096,device='cuda')\n"
            "while True:\n c=a@b; torch.cuda.synchronize()\n")
    e = {**os.environ, "CUDA_VISIBLE_DEVICES": str(dev)}
    return subprocess.Popen([str(HOG_PY), "-c", code], env=e,
                            stdout=open(RES / "hog.log", "w"), stderr=subprocess.STDOUT)


COVERAGE_FRAC = 0.98  # tolerate <=2% (EOS-flush) shortfall; real overload loses far more

# fixed per-pipeline period for Part A/C (N=1 sustains comfortably for both backends)
FIXED_PERIOD_MS = {"4096x512": 30.0, "2048x256": 8.0}
# Part B: per-(size, N) descending per-pipeline period grid [ms]. Higher N shares
# one GPU, so the per-pipeline period must be slower — grids scale ~linearly with N.
AGG_GRID_MS = {
    "4096x512": {1: [26, 24, 22, 20], 2: [52, 48, 44, 40, 36],
                 4: [104, 96, 88, 80, 72]},
    "2048x256": {1: [8, 7, 6.5, 6.0], 2: [16, 14, 12, 11, 10],
                 4: [32, 28, 24, 22, 20]},
}


def apply_agg_grid(overrides):
    """overrides: 'SIZE:N:p1,p2,...' strings → mutate AGG_GRID_MS[SIZE][N]."""
    for ov in overrides or []:
        parts = ov.split(":")
        if len(parts) != 3 or parts[0] not in AGG_GRID_MS:
            raise SystemExit(f"--agg-grid: expected SIZE:N:p1,p2,... got {ov!r}")
        AGG_GRID_MS[parts[0]][int(parts[1])] = [float(x) for x in parts[2].split(",")]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", nargs="+", default=["4096x512", "2048x256"])
    ap.add_argument("--ns", nargs="+", type=int, default=[1, 2, 4])
    ap.add_argument("--backends", nargs="+", default=["gpu", "hybrid"])
    ap.add_argument("--frames", type=int, default=250)
    ap.add_argument("--warmup", type=int, default=40)
    ap.add_argument("--parts", nargs="+", default=["B", "C"])  # A is confounded by GPU saturation
    ap.add_argument("--gpu-device", type=int, default=1)
    ap.add_argument("--agg-grid", action="append", metavar="SIZE:N:p1,p2,...",
                    help="override a Part-B (size,N) period grid [ms], repeatable")
    args = ap.parse_args()
    apply_agg_grid(args.agg_grid)
    dev = args.gpu_device
    RES.mkdir(parents=True, exist_ok=True)
    log = open(RES / "shared_gpu.log", "a")
    out = {"A": {}, "B": {}, "C": {}}
    op = RES / "results.json"
    if op.exists():
        out.update(json.loads(op.read_text()))

    for backend in args.backends:
        dylib, binary = build_backend(backend, log, dev)
        env = base_env(backend, dev)
        for size in args.sizes:
            sub, ns, nc = SIZES[size]
            # Part A: fixed rate, scale N
            if "A" in args.parts:
                per_ms = FIXED_PERIOD_MS[size]
                for N in args.ns:
                    r = run_set(backend, size, ns, nc, N, per_ms / 1e3, args.frames,
                                args.warmup, dylib, binary, env,
                                f"A_{backend}_{size}_N{N}", log, sample_gpu=True,
                                gpu_device=dev)
                    out["A"].setdefault(size, {}).setdefault(backend, {})[str(N)] = \
                        {"summary": summarize(r["per_pipe"]), "all_pass": r["all_pass"],
                         "gpu": r["gpu"], "period_ms": per_ms,
                         "offered_agg_fps": round(N / (per_ms/1e3), 1)}
                    op.write_text(json.dumps(out, indent=2))
                    print(f"[A {backend} {size} N={N}] pass={r['all_pass']} "
                          f"{out['A'][size][backend][str(N)]['summary']} "
                          f"util={r['gpu'].get('gpu_util_mean')}", flush=True)
            # Part B: aggregate sustained boundary
            if "B" in args.parts:
                for N in args.ns:
                    best = None
                    for pm in AGG_GRID_MS[size][N]:  # descending: getting harder
                        r = run_set(backend, size, ns, nc, N, pm / 1e3, args.frames,
                                    args.warmup, dylib, binary, env,
                                    f"B_{backend}_{size}_N{N}_{pm}", log, gpu_device=dev)
                        if r["all_pass"]:
                            best = {"period_ms": pm, "agg_fps": round(N / (pm/1e3), 1),
                                    "per_pipe_fps": round(1000/pm, 1)}
                        else:
                            break  # first fail: faster periods only harder
                    out["B"].setdefault(size, {}).setdefault(backend, {})[str(N)] = best
                    op.write_text(json.dumps(out, indent=2))
                    print(f"[B {backend} {size} N={N}] sustained={best}", flush=True)
            # Part C: adversarial hog, fixed rate
            if "C" in args.parts:
                hog = start_hog(log, dev)
                time.sleep(4)
                per_ms = FIXED_PERIOD_MS[size]
                for N in [1, 2]:
                    r = run_set(backend, size, ns, nc, N, per_ms / 1e3, args.frames,
                                args.warmup, dylib, binary, env,
                                f"C_{backend}_{size}_N{N}", log, sample_gpu=True,
                                gpu_device=dev)
                    out["C"].setdefault(size, {}).setdefault(backend, {})[str(N)] = \
                        {"summary": summarize(r["per_pipe"]), "all_pass": r["all_pass"],
                         "gpu": r["gpu"]}
                    op.write_text(json.dumps(out, indent=2))
                    print(f"[C hog {backend} {size} N={N}] pass={r['all_pass']} "
                          f"{out['C'][size][backend][str(N)]['summary']}", flush=True)
                hog.terminate()
                try:
                    hog.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    hog.kill()
    op.write_text(json.dumps(out, indent=2))
    print(f"\nDONE -> {op}")
    log.close()


if __name__ == "__main__":
    main()
