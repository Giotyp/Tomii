#!/usr/bin/env python3
"""E1 campaign driver: MIMO uplink, Tomii vs Taskflow vs oneTBB, live UDP input.

One *run* = one system processing N frames streamed by the Agora sender:
  1. start the system under test on NUMA0 (cores 0-31) with MKL_NUM_THREADS=1;
  2. wait 5 s (receiver binds its sockets first), start the sender on a fixed
     NUMA1 core set (taskset 52-63, sender pins itself to 55-57);
  3. wait for both to exit, collect the per-frame record file written by the
     system's probe (tomii/src/e1probe.rs or cpp/e1_runtime.hpp) and the
     sender's own per-frame transmit-end stamps (~/Agora/files/experiment/
     tx_result.txt, CLOCK_MONOTONIC — the same clock the probes use).
Every run is wrapped in `flock /home/george/Tomii/.eval-server.lock` by the
caller (see `cell`), per paper/experiments/EVAL_PROTOCOL.md.

Subcommands
  golden   produce the golden demod file for a config (Tomii, W=1, S=1)
  cell     run R repetitions of one (system, config, S, W, pacing, knobs) cell
           inside ONE flock (the lock is held for the whole cell)
  analyze  score one run directory -> run.json
  table    aggregate cells -> markdown rows
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent            # bench/mimo-bench/e1
BENCH = HERE.parent                               # bench/mimo-bench
ROOT = BENCH.parents[1]                           # worktree root
AGORA = Path("~/Agora").expanduser().resolve()
LOCK = "/home/george/Tomii/.eval-server.lock"
RESULTS = HERE / "results"
CPP_BUILD = BENCH / "cpp" / "build"
TOMII_BIN = ROOT / "target" / "release" / "main"
TOMII_DYLIB = BENCH / "tomii" / "target" / "release" / "libmimo_bench_tomii.so"
# Built with --features legacy-demul-symbol --target-dir tomii/target/legacy
TOMII_DYLIB_LEGACY = BENCH / "tomii" / "target" / "legacy" / "release" / "libmimo_bench_tomii.so"
SENDER_TASKSET = "52-63"      # NUMA1; sender pins master 55, workers 56-57
SUT_TASKSET = "0-31"          # NUMA0 for every system under test
SENDER_CORE_OFFSET = 55
SENDER_THREADS = 2
CONFIGS = {
    "4x4": AGORA / "files/config/ci/tddconfig-4x4.json",
    "16x16": AGORA / "files/config/ci/tddconfig-16x16.json",
    "64x16": AGORA / "files/config/ci/tddconfig-64x16.json",
}
BENCH_ENV = {
    "MKL_NUM_THREADS": "1",
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "GOTO_NUM_THREADS": "1",
}

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(BENCH / "tomii"))


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def cfg_info(cfg_path: Path) -> dict:
    c = json.loads(Path(cfg_path).read_text())
    sched = c["frame_schedule"][0]
    bs = c["bs_radio_num"]
    return {"bs": bs, "ue": c["ue_radio_num"], "ppf": bs * len(sched), "sched": sched}


def udp_counters() -> dict:
    lines = [l.split() for l in Path("/proc/net/snmp").read_text().splitlines() if l.startswith("Udp:")]
    return dict(zip(lines[0][1:], map(int, lines[1][1:])))


def fixed_frame_config(cfg: Path, n_frames: int, dst: Path) -> Path:
    c = json.loads(cfg.read_text())
    c["max_frame"] = n_frames
    dst.write_text(json.dumps(c, indent=1))
    return dst


def tomii_graph(cfg: Path, dst: Path) -> Path:
    if not dst.exists():
        from build_graph import build_mimo_graph
        dst.write_text(build_mimo_graph(config_path=str(cfg)).to_json())
    return dst


def kill_tree(p: subprocess.Popen | None) -> None:
    if p is None or p.poll() is not None:
        return
    try:
        p.kill()
        p.wait(timeout=5)
    except Exception:
        pass


def pctl(xs: list[float], q: float) -> float:
    """Nearest-rank on (n-1) — same convention as tomii-core's report."""
    if not xs:
        return float("nan")
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(q / 100.0 * (len(s) - 1))))]


# ---------------------------------------------------------------------------
# one run
# ---------------------------------------------------------------------------
def system_cmd(sys_name: str, a, run_dir: Path, cfg_run: Path, info: dict) -> tuple[list[str], dict]:
    frames_out = run_dir / "frames.csv"
    env = {**os.environ, **BENCH_ENV}
    if sys_name == "tomii":
        from tomii._runner import build_command
        graph = tomii_graph(Path(a.config_path), RESULTS / f"graph_{a.config}.json")
        kn = dict(kv.split("=", 1) for kv in (a.knob or []))
        max_rt = 5 + math.ceil(a.frames * (a.frame_duration + a.inter_frame_delay) / 1e6) + 20
        # knob dylib=legacy: the pre-fix demul-symbol plugin (bug-fix impact only).
        dylib = {"legacy": TOMII_DYLIB_LEGACY, None: TOMII_DYLIB}.get(kn.get("dylib"), None) or Path(kn["dylib"])
        # knob bin=<rev>: run results/bins/main-<rev> instead (P0 validation: the
        # pre-fix ffb51d3 build, copied from the eval/mimo-taskflow-tbb worktree, as a control).
        cmd = build_command(
            str(RESULTS / "bins" / f"main-{kn['bin']}") if "bin" in kn else str(TOMII_BIN),
            str(graph), str(dylib),
            workers=a.workers, core_offset=int(kn.get("core_offset", 1)),
            system_threads=int(kn.get("system_threads", 2)),
            receiver_threads=int(kn.get("receiver_threads", 4)),
            slots=a.slots, max_frames=a.frames, exclude_frames=a.warmup, max_runtime=max_rt,
            batching_size=int(kn["batching_size"]) if "batching_size" in kn else None,
            batching_limit=int(kn["batching_limit"]) if "batching_limit" in kn else None,
            # Tomii's own recorder is OFF by default in E1: the e1probe gives every
            # metric, identically for all systems (knob timing=1 re-enables it).
            timing=str(run_dir / "tomii_timing.txt") if kn.get("timing", "0") == "1" else None,
            report=str(run_dir / "tomii_report.json") if kn.get("timing", "0") == "1" else None,
            use_rdtsc=True, custom=kn.get("custom", "1") == "1",
            frame_timeout_ms=int(kn["frame_timeout_ms"]) if "frame_timeout_ms" in kn else None,
            coalesce_barriers=kn.get("coalesce_barriers", "1") == "1",
            inline_continuation=kn.get("inline_continuation", "1") == "1",
            slot_priority=kn.get("slot_priority", "1") == "1",
            # Runtime state snapshot at shutdown (+ numbered live snapshots on
            # SIGUSR1, sent by run_one on a late exit or timeout): the per-slot
            # post-mortem for any stall/hang. knob dump=0 disables it.
            dump_state=str(run_dir / "state.json") if kn.get("dump", "1") == "1" else None,
        )
        # BLAS parity: libbeamfuncs -> armadillo -> cherk_/cposv_/... In the C++
        # baselines those Fortran symbols bind to MKL (MKL is a DT_NEEDED of the
        # executable, so it is in the global lookup scope). In Tomii the plugin
        # (and with it MKL) is dlopen'ed with local scope, so armadillo's BLAS/LAPACK
        # calls bind to the system OpenBLAS instead. Preloading MKL puts it in the
        # global scope so every system runs the beam stage on the same MKL code.
        # knob blas=openblas reproduces the previous (mismatched) setup.
        if kn.get("blas", "mkl") == "mkl":
            m = "/opt/intel/oneapi/mkl/2024.0/lib"
            env["LD_PRELOAD"] = f"{m}/libmkl_intel_lp64.so.2 {m}/libmkl_sequential.so.2 {m}/libmkl_core.so.2"
        # Plugin buffer windows (tomii/src/common/symbols.rs frame_wnd): 2*S, the
        # same number of frame windows the C++ baselines allocate (S slots x 2).
        env["TOMII_MIMO_FRAME_WND"] = kn.get("fw", str(2 * a.slots))
        if kn.get("probe", "1") == "1":  # probe=0 only for the probe-overhead check
            env.update({
                "E1_FRAMES_OUT": str(frames_out),
                "E1_PKTS_PER_FRAME": str(info["ppf"]),
                "E1_NFRAMES": str(a.frames),
            })
        if a.golden:
            env["E1_GOLDEN"] = a.golden
        if a.dump_golden:
            env["E1_DUMP_GOLDEN"] = a.dump_golden
    else:
        binary = CPP_BUILD / ("tbb_e1" if sys_name.startswith("tbb") else "tf_e1")
        cmd = [str(binary), "--mode", sys_name, "--slots", str(a.slots), "--workers", str(a.workers),
               "--frames", str(a.frames), "--config", str(cfg_run), "--out", str(frames_out)]
        for kv in a.knob or []:
            k, v = kv.split("=", 1)
            cmd += [f"--{k}", v]
        if a.golden:
            cmd += ["--golden", a.golden]
        if a.dump_golden:
            cmd += ["--dump-golden", a.dump_golden]
    return ["taskset", "-c", SUT_TASKSET] + cmd, env


def run_one(a, run_dir: Path) -> dict:
    run_dir.mkdir(parents=True, exist_ok=True)
    info = cfg_info(Path(a.config_path))
    cfg_run = fixed_frame_config(Path(a.config_path), a.frames, run_dir / "tddconfig.json")
    cmd, env = system_cmd(a.system, a, run_dir, cfg_run, info)
    (run_dir / "cmd.txt").write_text(" ".join(cmd) + "\n")
    tx = AGORA / "files" / "experiment" / "tx_result.txt"
    tx.unlink(missing_ok=True)
    udp0 = udp_counters()
    log = open(run_dir / "system.log", "w")
    t0 = time.time()
    sut = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
    sender = None
    status = "ok"
    late_snapshot = False
    try:
        time.sleep(a.sender_delay)
        if sut.poll() is not None:
            raise RuntimeError(f"system exited early rc={sut.returncode}")
        scmd = ["taskset", "-c", SENDER_TASKSET, str(AGORA / "build" / "sender"),
                f"--num_threads={SENDER_THREADS}", f"--core_offset={SENDER_CORE_OFFSET}",
                f"--frame_duration={a.frame_duration}", "--enable_slow_start=0",
                f"--inter_frame_delay={a.inter_frame_delay}", f"--conf_file={cfg_run}"]
        (run_dir / "sender_cmd.txt").write_text(" ".join(scmd) + "\n")
        slog = open(run_dir / "sender.log", "w")
        sender = subprocess.Popen(scmd, cwd=str(AGORA), stdout=slog, stderr=subprocess.STDOUT)
        send_s = a.frames * (a.frame_duration + a.inter_frame_delay) / 1e6
        sender.wait(timeout=send_s + 60)
        if a.system == "tomii":
            # A healthy Tomii run exits right after the last frame (done + dropped
            # reaches --max-frames). Still alive 5 s after the sender finished =
            # suspected stall/hang: take a live per-slot snapshot (state.json.N)
            # before it reaches --max-runtime, then keep waiting.
            try:
                sut.wait(timeout=5)
            except subprocess.TimeoutExpired:
                late_snapshot = True
                sut.send_signal(signal.SIGUSR1)
        sut.wait(timeout=60 if a.system != "tomii" else send_s + 60)
    except subprocess.TimeoutExpired:
        status = "timeout"
        if a.system == "tomii" and sut.poll() is None:
            sut.send_signal(signal.SIGUSR1)  # snapshot the wedged state before the kill
            time.sleep(1.5)
    except RuntimeError as e:
        status = f"error: {e}"
    finally:
        kill_tree(sender)
        kill_tree(sut)
        log.close()
    wall = time.time() - t0
    udp1 = udp_counters()
    if tx.exists():
        shutil.copy(tx, run_dir / "tx_result.txt")
    meta = {
        "system": a.system, "config": a.config, "slots": a.slots, "workers": a.workers,
        "frames": a.frames, "warmup": a.warmup, "frame_duration_us": a.frame_duration,
        "inter_frame_delay_us": a.inter_frame_delay, "knobs": a.knob or [], "status": status,
        "rc": sut.returncode, "wall_s": wall,
        "udp_rcvbuf_errors": udp1["RcvbufErrors"] - udp0["RcvbufErrors"],
        "udp_in_datagrams": udp1["InDatagrams"] - udp0["InDatagrams"],
        "golden": a.golden,
        "late_exit_snapshot": late_snapshot,
    }
    (run_dir / "meta.json").write_text(json.dumps(meta, indent=1))
    time.sleep(2)  # let in-flight UDP drain before the next bind
    return analyze(run_dir)


# ---------------------------------------------------------------------------
# scoring
# ---------------------------------------------------------------------------
def read_frames(path: Path) -> tuple[dict, list[dict]]:
    head, rows = {}, []
    if not path.exists():
        return head, rows
    lines = path.read_text().splitlines()
    for ln in lines:
        if ln.startswith("# "):
            head = json.loads(ln[2:])
            break
    hdr = None
    for ln in lines:
        if ln.startswith("#"):
            continue
        if hdr is None:
            hdr = ln.split(",")
            continue
        rows.append(dict(zip(hdr, map(int, ln.split(",")))))
    return head, rows


def analyze(run_dir: Path) -> dict:
    meta = json.loads((run_dir / "meta.json").read_text())
    head, rows = read_frames(run_dir / "frames.csv")
    N, K = meta["frames"], meta["warmup"]
    ppf = head.get("ppf", 0)
    tx_end = []
    if (run_dir / "tx_result.txt").exists():
        tx_end = [float(x) * 1e3 for x in (run_dir / "tx_result.txt").read_text().split()]  # ns
    byf = {r["frame"]: r for r in rows}
    done = {f: r for f, r in byf.items() if r["done_ns"] > 0 and f < N}
    verify_ok = sum(1 for r in done.values() if r["verify"] == 1)
    verify_bad = sum(1 for r in done.values() if r["verify"] == 2)
    depviol_frames = sum(1 for r in done.values() if r.get("depviol", 0) > 0)
    winviol_frames = sum(1 for r in done.values() if r.get("winviol", 0) > 0)
    dropped = N - len(done)
    # Frames whose packets all arrived (decoded) but that never completed, and
    # the subset in which not a single FFT/CSI task executed (#29 symptom).
    stuck = [f for f, r in byf.items() if f < N and r["done_ns"] == 0 and ppf and r["rx_pkts"] >= ppf]
    stuck_noexec = [f for f in stuck if byf[f].get("n_fftcsi", 0) == 0]
    meas = [done[f] for f in sorted(done) if f >= K]
    L = [(r["done_ns"] - r["first_rx_ns"]) / 1e6 for r in meas]
    T = [(r["done_ns"] - r["last_rx_ns"]) / 1e6 for r in meas if r["last_rx_ns"] > 0]
    # Sender-referenced (system-independent) stamps. The sender records
    # frame_end(f) one symbol period AFTER scheduling the last symbol, and a
    # frame spans exactly P = frame_duration by schedule, so
    #   first symbol sent  = frame_end(f) - P
    #   last symbol sent   = frame_end(f) - P / n_symbols
    P_ns = meta["frame_duration_us"] * 1e3
    nsym = len(json.loads((run_dir / "tddconfig.json").read_text())["frame_schedule"][0])
    Ln = [(r["done_ns"] - (tx_end[r["frame"]] - P_ns)) / 1e6 for r in meas if r["frame"] < len(tx_end)]
    Tn = [(r["done_ns"] - (tx_end[r["frame"]] - P_ns / nsym)) / 1e6
          for r in meas if r["frame"] < len(tx_end)]
    A = [(r["last_rx_ns"] - r["first_rx_ns"]) / 1e6 for r in meas if r["last_rx_ns"] > 0]
    ov = [r["overlap"] / ppf for r in meas if ppf]
    thr = float("nan")
    if len(meas) > 1:
        span = (meas[-1]["done_ns"] - meas[0]["done_ns"]) / 1e9
        thr = (len(meas) - 1) / span if span > 0 else float("nan")
    golden_used = bool(meta.get("golden"))
    failed = []
    if meta["status"] != "ok":
        failed.append(meta["status"])
    if dropped:
        failed.append(f"dropped {dropped}/{N}")
    if golden_used and (verify_bad or verify_ok != len(done)):
        failed.append(f"verify {verify_ok}/{len(done)} ok, {verify_bad} mismatch")
    if depviol_frames:
        failed.append(f"dependency violations in {depviol_frames} frames")
    if winviol_frames:
        failed.append(f"buffer-window violations in {winviol_frames} frames")
    if head.get("stalled"):
        failed.append("stalled")
    tomii_avg = None
    tf = run_dir / "tomii_timing.txt"
    if tf.exists():
        import re
        m = re.search(r"Avg Time Per Frame:\s+([\d.]+)(ms|µs|us|s)", tf.read_text())
        if m:
            v = float(m.group(1))
            tomii_avg = v / 1e3 if m.group(2) in ("µs", "us") else (v * 1e3 if m.group(2) == "s" else v)
    res = {
        **{k: meta[k] for k in ("system", "config", "slots", "workers", "frames", "warmup",
                                "frame_duration_us", "inter_frame_delay_us", "knobs")},
        "run_dir": str(run_dir), "pass": not failed, "fail_reasons": failed,
        "completed": len(done), "dropped": dropped, "verify_ok": verify_ok, "verify_bad": verify_bad,
        "depviol_frames": depviol_frames, "winviol_frames": winviol_frames,
        "measured": len(meas),
        "stuck_frames": len(stuck), "stuck_noexec_frames": len(stuck_noexec),
        "first_stuck": stuck[:5],
        "late_exit_snapshot": meta.get("late_exit_snapshot", False),
        "lat_mean": statistics.fmean(L) if L else float("nan"),
        "lat_p50": pctl(L, 50), "lat_p99": pctl(L, 99), "lat_p999": pctl(L, 99.9),
        "lat_max": max(L) if L else float("nan"),
        "tail_mean": statistics.fmean(T) if T else float("nan"),
        "tail_p50": pctl(T, 50), "tail_p99": pctl(T, 99),
        "e2e_mean": statistics.fmean(Ln) if Ln else float("nan"),
        "e2e_p50": pctl(Ln, 50), "e2e_p99": pctl(Ln, 99), "e2e_p999": pctl(Ln, 99.9),
        "txtail_mean": statistics.fmean(Tn) if Tn else float("nan"),
        "txtail_p50": pctl(Tn, 50), "txtail_p99": pctl(Tn, 99),
        "arrival_span_mean": statistics.fmean(A) if A else float("nan"),
        "overlap_frac_mean": statistics.fmean(ov) if ov else float("nan"),
        "throughput_fps": thr,
        "udp_rcvbuf_errors": meta["udp_rcvbuf_errors"],
        "frames_parked": head.get("frames_parked"),
        "tomii_reported_avg_ms": tomii_avg,
    }
    (run_dir / "run.json").write_text(json.dumps(res, indent=1))
    return res


# ---------------------------------------------------------------------------
# cell = R runs under one lock
# ---------------------------------------------------------------------------
def cell_dir(a) -> Path:
    kn = "_".join(k.replace("=", "") for k in (a.knob or []))
    name = f"{a.config}/{a.system}{('_' + kn) if kn else ''}/S{a.slots}_W{a.workers}_P{a.frame_duration}"
    if a.inter_frame_delay:
        name += f"_G{a.inter_frame_delay}"
    return RESULTS / (a.tag or "main") / name


def cmd_cell(a) -> None:
    if os.environ.get("E1_LOCKED") != "1":
        # Re-exec ourselves under the machine-wide measurement lock.
        env = {**os.environ, "E1_LOCKED": "1"}
        rc = subprocess.call(["flock", LOCK, sys.executable, *sys.argv], env=env)
        sys.exit(rc)
    a.config_path = str(CONFIGS[a.config])
    base = cell_dir(a)
    out = []
    for rep in range(a.reps):
        rd = base / f"rep{rep}"
        if rd.exists():
            shutil.rmtree(rd)
        r = run_one(a, rd)
        out.append(r)
        print(f"[{a.system} {a.config} S{a.slots} W{a.workers} P{a.frame_duration} {a.knob or ''} rep{rep}] "
              f"pass={r['pass']} {r['fail_reasons']} lat mean={r['lat_mean']:.3f} p50={r['lat_p50']:.3f} "
              f"p99={r['lat_p99']:.3f} e2e={r['e2e_mean']:.3f}/{r['e2e_p99']:.3f} tail={r['tail_mean']:.3f} "
              f"txtail={r['txtail_mean']:.3f} "
              f"ov={r['overlap_frac_mean']:.2f} done={r['completed']}/{a.frames} "
              f"rcvbuf_err={r['udp_rcvbuf_errors']} stuck={r['stuck_frames']}/{r['stuck_noexec_frames']} "
              f"late={r['late_exit_snapshot']}", flush=True)
    (base / "cell.json").write_text(json.dumps(summarize(out), indent=1))


def summarize(runs: list[dict]) -> dict:
    ok = [r for r in runs if r["pass"]]
    def med(k):
        v = [r[k] for r in ok if r[k] is not None and not math.isnan(r[k])]
        return statistics.median(v) if v else float("nan")
    def rng(k):
        v = [r[k] for r in ok if r[k] is not None and not math.isnan(r[k])]
        return [min(v), max(v)] if v else None
    keys = ["lat_mean", "lat_p50", "lat_p99", "lat_p999", "lat_max", "tail_mean", "tail_p50",
            "tail_p99", "e2e_mean", "e2e_p50", "e2e_p99", "e2e_p999", "txtail_mean", "txtail_p50",
            "txtail_p99", "arrival_span_mean",
            "overlap_frac_mean", "throughput_fps", "tomii_reported_avg_ms"]
    s = {k: runs[0][k] for k in ("system", "config", "slots", "workers", "frames", "warmup",
                                 "frame_duration_us", "inter_frame_delay_us", "knobs")}
    s.update({
        "reps": len(runs), "reps_pass": len(ok),
        "fail_reasons": [r["fail_reasons"] for r in runs if not r["pass"]],
        "dropped_per_rep": [r["dropped"] for r in runs],
        "completed_per_rep": [r["completed"] for r in runs],
        "verify_ok_per_rep": [r["verify_ok"] for r in runs],
        "udp_rcvbuf_errors_per_rep": [r["udp_rcvbuf_errors"] for r in runs],
        "stuck_per_rep": [r.get("stuck_frames", 0) for r in runs],
        "late_exit_per_rep": [r.get("late_exit_snapshot", False) for r in runs],
    })
    for k in keys:
        s[k] = med(k)
    s["lat_mean_range"] = rng("lat_mean")
    s["lat_p99_range"] = rng("lat_p99")
    s["e2e_mean_range"] = rng("e2e_mean")
    s["e2e_p99_range"] = rng("e2e_p99")
    return s


def cmd_golden(a) -> None:
    """Golden = demod output of frame 10 from a slow-paced Tomii W=1 S=1 run."""
    a.system, a.slots, a.workers = "tomii", 1, 1
    a.golden = None
    a.dump_golden = str(RESULTS / f"golden_{a.config}.bin")
    if os.environ.get("E1_LOCKED") != "1":
        env = {**os.environ, "E1_LOCKED": "1"}
        sys.exit(subprocess.call(["flock", LOCK, sys.executable, *sys.argv], env=env))
    a.config_path = str(CONFIGS[a.config])
    r = run_one(a, RESULTS / "golden" / a.config / a.system)
    print(json.dumps(r, indent=1))
    print("golden:", a.dump_golden, Path(a.dump_golden).stat().st_size if Path(a.dump_golden).exists() else "MISSING")


def main() -> None:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("cell", "golden"):
        q = sub.add_parser(name)
        q.add_argument("--system", default="tomii",
                       choices=["tomii", "tf-orig", "tf-dag", "tf-async", "tbb-flow", "tbb-dag"])
        q.add_argument("--config", required=True, choices=list(CONFIGS))
        q.add_argument("--slots", type=int, default=1)
        q.add_argument("--workers", type=int, default=24)
        q.add_argument("--frames", type=int, default=520)
        q.add_argument("--warmup", type=int, default=20)
        q.add_argument("--frame-duration", type=int, required=True, help="sender frame period, µs")
        q.add_argument("--inter-frame-delay", type=int, default=0)
        q.add_argument("--reps", type=int, default=5)
        q.add_argument("--sender-delay", type=float, default=5.0)
        q.add_argument("--knob", action="append", help="key=value (engine knob / tomii flag)")
        q.add_argument("--golden", default=None)
        q.add_argument("--dump-golden", default=None)
        q.add_argument("--tag", default=None, help="results sub-directory (default: main)")
    q = sub.add_parser("analyze")
    q.add_argument("run_dir", type=Path)
    a = p.parse_args()
    if a.cmd == "cell":
        cmd_cell(a)
    elif a.cmd == "golden":
        cmd_golden(a)
    elif a.cmd == "analyze":
        print(json.dumps(analyze(a.run_dir), indent=1))


if __name__ == "__main__":
    main()
