"""Tomii 4-stage linear pipeline benchmark.

Sweeps S (concurrent slots) and W (workers) for a fan-out/fan-in pipeline
of T total streams, each processing N items.  Writes per-stream timing to
results/pipeline_sweep.csv so it can be compared with the Taskflow output.

Pipeline topology
-----------------
  ingest[0..N]  (factor=N, each produces one f64)
      |
  transform[0..N]  (factor=N, 1:1 from ingest)
      |
  aggregate  (variadic fan-in of all N transform results -> mean f64)
      |
  emit  (writes mean, returns mean)

Usage (from bench worktree root):
    python pipeline-bench/tomii/run_bench.py
    python pipeline-bench/tomii/run_bench.py --slots 1 4 16 --workers 4 8
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent        # pipeline-bench/tomii/
BENCH_ROOT = HERE.parents[2]                  # workspace root
DEVELOP_ROOT = BENCH_ROOT                     # same as workspace root on develop
sys.path.insert(0, str(DEVELOP_ROOT))

import tomii as tm
from tomii._types import TypedValue
from tomii._runner import build_command, _find_binary
from tomii._serialize import to_json


# ---------------------------------------------------------------------------
# Timing parser
# ---------------------------------------------------------------------------

def _parse_avg_ms(timing_file: Path) -> float:
    """Extract 'Avg Time Per Stream' (latency) from a Tomii timing file."""
    text = timing_file.read_text()
    m = re.search(r"Avg Time Per Stream:\s+([\d.]+)(ms|µs|us|s)", text)
    if not m:
        return float("nan")
    val, unit = float(m.group(1)), m.group(2)
    if unit in ("µs", "us"):
        return val / 1e3
    if unit == "s":
        return val * 1e3
    return val  # already ms


# ---------------------------------------------------------------------------
# RSS probe — measure peak RSS of the tomii-core binary via /usr/bin/time -v
# ---------------------------------------------------------------------------

def _probe_binary_rss(
    graph: "tm.Graph",
    *,
    dylib: str,
    workers: int,
    slots: int,
    streams: int = 200,
    warmup: int = 50,
) -> int | None:
    """Return peak RSS (kB) of the tomii-core binary for the given config.

    Runs a short probe invocation (not a full timed sweep) under
    /usr/bin/time -v and parses VmHWM from the output.  Returns None if
    measurement fails.  The probe streams are enough to stabilize RSS but
    not so many that the probe dominates wall time.
    """
    import tempfile

    binary = str(BENCH_ROOT / "target" / "release" / "main")
    json_str = to_json(graph)

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write(json_str)
        tmp_path = tmp.name

    rss_log = Path(tmp_path + ".rss.log")
    try:
        timing_probe = Path(tmp_path).with_suffix(".timing.txt")
        cmd = build_command(
            binary, tmp_path, dylib,
            workers=workers,
            core_offset=1,
            system_threads=1,
            slots=slots,
            max_streams=streams + warmup,
            batching_size=1,
            exclude_streams=warmup,
            custom=True,
            use_rdtsc=True,
            coalesce_barriers=True,
            inline_continuation=True,
            timing=str(timing_probe),
        )
        full_cmd = ["/usr/bin/time", "-v", "-o", str(rss_log)] + cmd
        result = subprocess.run(full_cmd, capture_output=True)
        timing_probe.unlink(missing_ok=True)
        if result.returncode != 0:
            return None
        if rss_log.exists():
            for line in rss_log.read_text().splitlines():
                if "Maximum resident set size" in line:
                    return int(line.strip().split()[-1])
    except Exception:
        return None
    finally:
        Path(tmp_path).unlink(missing_ok=True)
        rss_log.unlink(missing_ok=True)

    return None


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------

def build_pipeline(n: int) -> tm.Graph:
    """Return a Tomii Graph for the 4-stage fan-out/fan-in pipeline.

    Parameters
    ----------
    n:
        Number of items per stream (= factor width for ingest/transform).
    """
    app = tm.Graph()

    # $index placeholder — runtime fills in the instance index [0, factor).
    _index = TypedValue("$ref", "$index")

    # Stage 1: ingest N items in parallel.
    # factor=n (concrete int) and n as a usize arg for the kernel.
    ingest = app.node("ingest", func="pl_ingest", factor=n,
                      args=[_index, tm.usize(n)])

    # Stage 2: transform each item 1:1 (factor=N, depends on ingest[i]).
    transform = app.node("transform", func="pl_transform", factor=n,
                         args=[ingest.out()])

    # Stage 3: aggregate — variadic fan-in of all N transform results.
    # transform.out(0, n) uses the concrete Python int as the range bound.
    aggregate = app.node("aggregate", func="pl_aggregate",
                         args=[transform.out(0, n)])

    # Stage 4: emit — scalar result from aggregate; stream_id=0 placeholder.
    app.node("emit", func="pl_emit",
             args=[aggregate.out(), tm.usize(0)])

    return app


# ---------------------------------------------------------------------------
# Single (S, W) run
# ---------------------------------------------------------------------------

def run_one(
    *,
    n: int,
    slots: int,
    workers: int,
    total_streams: int,
    warmup: int,
    results_dir: Path,
    dylib: str,
    transform_iters: int,
    measure_rss: bool = False,
) -> tuple[float, int | None]:
    """Run one (slots, workers) config; return (ms_per_stream, peak_rss_kb).

    Wall time is measured externally around the single graph.run() call and
    divided by total_streams (not total_streams+warmup) for direct comparison
    with Taskflow's total_ms/total_streams metric.  The warmup phase runs
    inside the same graph.run(), so runtime and Rayon pool startup cost is
    paid once — warmup streams add ~10 % upward bias to the wall time at
    warmup=200/total=2000, which is consistent across all S×W cells and
    does not affect relative comparisons.

    When measure_rss=True, runs a short probe invocation under
    /usr/bin/time -v after the main timing run to capture the tomii binary's
    peak RSS. Returns None for peak_rss_kb if measure_rss=False.
    """
    timing_file = (
        results_dir / f"tomii_pipeline_n{n}_s{slots}_w{workers}.txt"
    )
    print(
        f"\n=== Tomii | n={n}  slots={slots}  workers={workers}"
        f"  iters={transform_iters} ===",
        flush=True,
    )

    graph = build_pipeline(n)

    bench_binary = str(BENCH_ROOT / "target" / "release" / "main")
    t0 = time.monotonic()
    graph.run(
        dylib=dylib,
        binary=bench_binary,
        workers=workers,
        core_offset=1,
        system_threads=1,
        slots=slots,
        max_streams=total_streams + warmup,
        exclude_streams=warmup,
        batching_size=1,
        timing=str(timing_file),
        use_rdtsc=True,
        custom=True,
        coalesce_barriers=True,
        inline_continuation=True,
    )
    t1 = time.monotonic()

    wall_ms = (t1 - t0) * 1000.0
    throughput_ms = wall_ms / total_streams
    latency_ms = _parse_avg_ms(timing_file)
    print(
        f"  throughput: {throughput_ms:.4f} ms/stream  "
        f"(latency: {latency_ms:.4f} ms/stream,  "
        f"wall: {wall_ms:.1f} ms)",
        flush=True,
    )

    rss_kb: int | None = None
    if measure_rss:
        print("  measuring binary RSS...", flush=True)
        rss_kb = _probe_binary_rss(
            graph,
            dylib=dylib,
            workers=workers,
            slots=slots,
            streams=max(50, min(200, total_streams // 10)),
            warmup=20,
        )
        print(f"  binary RSS: {rss_kb} kB", flush=True)

    return throughput_ms, rss_kb


# ---------------------------------------------------------------------------
# Main sweep
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(
        description="Tomii pipeline benchmark sweep over slots and workers."
    )
    p.add_argument("--n", type=int, default=256,
                   help="items per stream (pipeline width)")
    p.add_argument("--slots", type=int, nargs="+", default=[1, 4, 16, 64],
                   help="concurrent slot counts to sweep")
    p.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8],
                   help="worker thread counts to sweep")
    p.add_argument("--streams", type=int, default=2000,
                   help="total streams to process (excluding warmup)")
    p.add_argument("--warmup", type=int, default=200,
                   help="warmup streams excluded from timing")
    p.add_argument("--results-dir", type=Path, default=HERE / "results")
    p.add_argument("--csv-out", type=Path, default=None,
                   help="output CSV path (default: results-dir/pipeline_sweep_heavy.csv)")
    p.add_argument("--no-clean", dest="clean", action="store_false",
                   default=True,
                   help="skip cargo clean before building")
    p.add_argument("--transform-iters", type=int, default=None,
                   help="value of TRANSFORM_ITERS compiled into the plugin "
                        "(tagged in CSV; does not change the binary)")
    p.add_argument("--measure-rss", action="store_true", default=False,
                   help="run a short binary probe under /usr/bin/time -v to "
                        "capture the tomii-core binary's peak RSS; adds "
                        "peak_rss_kb column to the CSV")
    args = p.parse_args()

    args.results_dir.mkdir(parents=True, exist_ok=True)

    # Detect transform_iters from lib.rs if not supplied on the CLI.
    if args.transform_iters is None:
        import re as _re
        src = (HERE / "src" / "lib.rs").read_text()
        m = _re.search(r"const TRANSFORM_ITERS\s*:\s*usize\s*=\s*(\d+)", src)
        args.transform_iters = int(m.group(1)) if m else 0

    # ------------------------------------------------------------------
    # Build the plugin
    # ------------------------------------------------------------------
    print("Building Tomii plugin...", flush=True)

    if args.clean:
        subprocess.run(
            ["cargo", "clean",
             "--manifest-path", str(HERE / "Cargo.toml")],
            check=True,
        )

    subprocess.run(
        ["cargo", "build",
         "--manifest-path", str(HERE / "Cargo.toml"),
         "--release"],
        check=True,
        env={**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")},
    )

    dylib = str(HERE / "target" / "release" / "libpl_bench.so")
    print(f"  dylib: {dylib}", flush=True)

    # Build the bench worktree's tomii-core binary with pipeline function wrappers.
    # The bench worktree binary is what graph.run() uses (bench_binary); it must be
    # built with FUNC_PATH so its function registry includes pl_ingest, pl_transform, etc.
    bench_binary = BENCH_ROOT / "target" / "release" / "main"
    print("Building bench tomii-core binary with pipeline FUNC_PATH...", flush=True)
    subprocess.run(
        ["cargo", "build", "-p", "tomii-core", "--bin", "main", "--release"],
        check=True,
        cwd=str(BENCH_ROOT),
        env={**os.environ, "FUNC_PATH": str(HERE / "src" / "lib.rs")},
    )
    print(f"  bench binary: {bench_binary}", flush=True)

    # ------------------------------------------------------------------
    # CSV header
    # ------------------------------------------------------------------
    csv_path = args.csv_out or (args.results_dir / "pipeline_sweep_heavy.csv")
    rss_col = ",peak_rss_kb" if args.measure_rss else ""
    with open(csv_path, "w") as f:
        f.write(f"system,n,items_per_stream,slots,workers,streams,ms_per_stream,transform_iters{rss_col}\n")

    # ------------------------------------------------------------------
    # Sweep
    # ------------------------------------------------------------------
    for w in args.workers:
        for s in args.slots:
            ms, rss_kb = run_one(
                n=args.n,
                slots=s,
                workers=w,
                total_streams=args.streams,
                warmup=args.warmup,
                results_dir=args.results_dir,
                dylib=dylib,
                transform_iters=args.transform_iters,
                measure_rss=args.measure_rss,
            )
            rss_field = f",{rss_kb if rss_kb is not None else 0}" if args.measure_rss else ""
            with open(csv_path, "a") as f:
                f.write(
                    f"tomii,{args.n},{args.n},{s},{w},"
                    f"{args.streams},{ms:.6f},{args.transform_iters}{rss_field}\n"
                )

    print(f"\nResults written to: {csv_path}", flush=True)


if __name__ == "__main__":
    main()
