#!/usr/bin/env python3
"""Verify Tomii radar pipeline detections against scene ground truth.

Parses the detection dump written by the cluster node (one line per frame:
``frame N: range,doppler,power ...``) and checks every ground-truth target is
detected within a bin tolerance in every processed frame. This is the
hard-coded verifier that gates performance measurement.

Exit code 0 iff no frame is missing a target (false alarms are reported but
only fail with --max-fa).
"""

import argparse
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "data"))

from reference_check import bin_distance  # noqa: E402

LINE_RE = re.compile(r"^frame (\d+):((?: \d+,\d+,[\d.eE+-]+)*)$")


def parse_detections(path: Path) -> dict[int, list[tuple[int, int, float]]]:
    frames: dict[int, list[tuple[int, int, float]]] = {}
    for line in path.read_text().splitlines():
        m = LINE_RE.match(line)
        if not m:
            raise ValueError(f"unparseable detection line: {line!r}")
        frame_id = int(m.group(1))
        dets = []
        for entry in m.group(2).split():
            r, d, p = entry.split(",")
            dets.append((int(r), int(d), float(p)))
        frames[frame_id] = dets
    return frames


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--scene", type=Path, default=HERE / "data" / "out" / "scene.json")
    p.add_argument(
        "--detections", type=Path, default=HERE / "results" / "detections.txt"
    )
    p.add_argument("--tol", type=float, default=2.0, help="bin match tolerance")
    p.add_argument(
        "--expect-frames",
        type=int,
        default=None,
        help="fail unless exactly this many frames were processed (coverage gate)",
    )
    p.add_argument(
        "--max-fa",
        type=float,
        default=None,
        help="fail if mean false alarms per frame exceeds this",
    )
    args = p.parse_args(argv)

    scene = json.loads(args.scene.read_text())
    radar = scene["radar"]
    n, m = radar["n_samples"], radar["n_chirps"]
    file_frames = radar["n_frames"]
    truth = scene["ground_truth"]

    if not args.detections.exists():
        print(f"FAIL: detections file {args.detections} does not exist")
        return 1
    frames = parse_detections(args.detections)
    if not frames:
        print("FAIL: no frames in detections file")
        return 1
    if args.expect_frames is not None and len(frames) != args.expect_frames:
        print(
            f"FAIL: coverage — {len(frames)}/{args.expect_frames} frames processed "
            "(overload drops or early exit)"
        )
        return 1

    total_miss = 0
    total_fa = 0
    for frame_id in sorted(frames):
        dets = frames[frame_id]
        expected = truth[frame_id % file_frames]
        matched: set[int] = set()
        misses = []
        for t in expected:
            hit = None
            for i, (r, d, _) in enumerate(dets):
                if (
                    i not in matched
                    and bin_distance(r, t["range_bin"], n) <= args.tol
                    and bin_distance(d, t["doppler_bin"], m) <= args.tol
                ):
                    hit = i
                    break
            if hit is None:
                misses.append(t)
            else:
                matched.add(hit)
        fa = len(dets) - len(matched)
        total_fa += fa
        total_miss += len(misses)
        for t in misses:
            print(
                f"frame {frame_id}: MISSED target {t['target']} "
                f"(range_bin {t['range_bin']:.1f}, doppler_bin {t['doppler_bin']:.1f}, "
                f"snr {t['snr_db']} dB); got {dets}"
            )

    n_frames = len(frames)
    n_targets = len(scene["targets"])
    mean_fa = total_fa / n_frames
    print(
        f"verify: {n_frames} frames, "
        f"{n_frames * n_targets - total_miss}/{n_frames * n_targets} targets detected, "
        f"{total_fa} false alarms ({mean_fa:.2f}/frame)"
    )

    if total_miss:
        print("FAIL: missed targets")
        return 1
    if args.max_fa is not None and mean_fa > args.max_fa:
        print(f"FAIL: mean false alarms {mean_fa:.2f} > {args.max_fa}")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
