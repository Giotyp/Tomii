"""Sensor pipeline — Tomii graph definition.

See TASK.md for the full computation spec.
Run: python run_bench.py [--workers N] [--max-streams N] [--exclude-streams N]
"""

from __future__ import annotations
import argparse
from pathlib import Path

import tomii as tm  # installed via `pip install -e .` at repo root

HERE = Path(__file__).resolve().parent


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--workers", type=int, default=2)
    p.add_argument("--slots", type=int, default=2)
    p.add_argument("--max-streams", type=int, default=5)
    p.add_argument("--exclude-streams", type=int, default=2)
    p.add_argument("--batching-size", type=int, default=1)
    p.add_argument("--no-clean", dest="clean", action="store_false", default=True)
    p.add_argument("--build-only", action="store_true", default=False)
    p.add_argument("--report", default="report.json")
    p.add_argument("--timing", default="timing.txt")
    return p.parse_args()


def build_graph() -> tm.Graph:
    app = tm.Graph()

    # --- Initializations ---
    # TODO: add the graph variables and nodes following TASK.md

    return app


def main() -> None:
    args = _parse_args()

    for f in ["result.txt", args.report, args.timing]:
        Path(f).unlink(missing_ok=True)

    app = build_graph()
    env = {"SCRIPT_DIR": str(HERE)}

    app.build(
        func_path=str(HERE / "src" / "lib.rs"),
        plugin_manifest=str(HERE / "Cargo.toml"),
        release=True,
        clean=args.clean,
        env=env,
    )

    if args.build_only:
        return

    app.run(
        env=env,
        workers=args.workers,
        slots=args.slots,
        max_streams=args.max_streams,
        exclude_streams=args.exclude_streams,
        batching_size=args.batching_size,
        output=str(HERE / "result.txt"),
        report=str(HERE / args.report),
        timing=str(HERE / args.timing),
    )


if __name__ == "__main__":
    main()
