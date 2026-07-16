#!/usr/bin/env python3
"""Reference range-Doppler processing over a generated scene.

Validates make_scene.py output: runs the same processing chain the Tomii
pipeline implements (Hann window -> range FFT -> Doppler FFT -> magnitude ->
2D CA-CFAR), then checks every ground-truth target produces a detection at
its expected (range_bin, doppler_bin) and reports false alarms.

Exit code 0 iff all ground-truth targets are detected in every frame.
This module is also imported by the pipeline verifier as the golden reference.
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np


def load_scene(scene_path: Path):
    scene = json.loads(scene_path.read_text())
    radar = scene["radar"]
    n, m = radar["n_samples"], radar["n_chirps"]
    raw = np.fromfile(scene_path.parent / scene["iq_file"], dtype="<i2")
    frames = raw.astype(np.float64).reshape(radar["n_frames"], m, 2 * n)
    iq = frames[:, :, 0::2] + 1j * frames[:, :, 1::2]  # (F, M, N)
    return scene, iq


def range_doppler_map(frame: np.ndarray) -> np.ndarray:
    """(M, N) chirp-major IQ -> (M, N) power map (Doppler x range bins)."""
    m, n = frame.shape
    win_r = np.hanning(n)[None, :]
    win_d = np.hanning(m)[:, None]
    rfft = np.fft.fft(frame * win_r, axis=1)
    dfft = np.fft.fft(rfft * win_d, axis=0)
    return np.abs(dfft) ** 2


def ca_cfar_2d(power: np.ndarray, guard=2, train=8, pfa_scale=15.0) -> np.ndarray:
    """2D cell-averaging CFAR. Returns boolean detection mask.

    Noise is estimated per cell as the mean power over the surrounding
    (2*(guard+train)+1)^2 window minus the guard region, computed with summed-
    area tables on the circularly-extended map (both FFT axes wrap). A cell
    detects when power > pfa_scale * noise_estimate.
    """
    k_out = guard + train

    def window_sum(p, k):
        pad = np.pad(p, k, mode="wrap")
        s = pad.cumsum(axis=0).cumsum(axis=1)
        s = np.pad(s, ((1, 0), (1, 0)))
        w = 2 * k + 1
        return (
            s[w:, w:] - s[:-w, w:] - s[w:, :-w] + s[:-w, :-w]
        )

    outer = window_sum(power, k_out)
    inner = window_sum(power, guard)
    n_train = (2 * k_out + 1) ** 2 - (2 * guard + 1) ** 2
    noise = (outer - inner) / n_train
    return power > pfa_scale * noise


def cluster_detections(mask: np.ndarray, power: np.ndarray):
    """Group adjacent detection cells (8-connected, wrapping) into peaks.

    Returns a list of (doppler_bin, range_bin, peak_power) per cluster,
    keeping the strongest cell of each cluster.
    """
    m, n = mask.shape
    visited = np.zeros_like(mask, dtype=bool)
    peaks = []
    idxs = np.argwhere(mask)
    neighbor = [(dm, dn) for dm in (-1, 0, 1) for dn in (-1, 0, 1) if (dm, dn) != (0, 0)]
    for start in map(tuple, idxs):
        if visited[start]:
            continue
        stack = [start]
        visited[start] = True
        cells = []
        while stack:
            cell = stack.pop()
            cells.append(cell)
            for dm, dn in neighbor:
                nb = ((cell[0] + dm) % m, (cell[1] + dn) % n)
                if mask[nb] and not visited[nb]:
                    visited[nb] = True
                    stack.append(nb)
        best = max(cells, key=lambda c: power[c])
        peaks.append((best[0], best[1], float(power[best])))
    return peaks


def bin_distance(a: float, b: float, size: int) -> float:
    """Circular distance between two FFT bin indices."""
    d = abs(a - b) % size
    return min(d, size - d)


def check_frame(power, truth, guard, train, pfa_scale, tol):
    mask = ca_cfar_2d(power, guard=guard, train=train, pfa_scale=pfa_scale)
    peaks = cluster_detections(mask, power)
    m, n = power.shape
    matched = set()
    misses = []
    for t in truth:
        hit = None
        for i, (db, rb, _) in enumerate(peaks):
            if (
                i not in matched
                and bin_distance(rb, t["range_bin"], n) <= tol
                and bin_distance(db, t["doppler_bin"], m) <= tol
            ):
                hit = i
                break
        if hit is None:
            misses.append(t)
        else:
            matched.add(hit)
    false_alarms = len(peaks) - len(matched)
    return misses, false_alarms, peaks


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--scene",
        type=Path,
        default=Path(__file__).resolve().parent / "out" / "scene.json",
    )
    p.add_argument("--guard", type=int, default=2, help="CFAR guard cells per side")
    p.add_argument("--train", type=int, default=8, help="CFAR training cells per side")
    p.add_argument("--pfa-scale", type=float, default=15.0, help="CFAR threshold factor")
    p.add_argument("--tol", type=float, default=2.0, help="bin tolerance for truth matching")
    args = p.parse_args(argv)

    scene, iq = load_scene(args.scene)
    total_miss = 0
    total_fa = 0
    for f in range(iq.shape[0]):
        power = range_doppler_map(iq[f])
        misses, fa, peaks = check_frame(
            power, scene["ground_truth"][f], args.guard, args.train,
            args.pfa_scale, args.tol,
        )
        total_fa += fa
        total_miss += len(misses)
        status = "ok" if not misses else "MISS"
        print(f"frame {f:3d}: {len(peaks)} detections, {fa} false alarms  [{status}]")
        for t in misses:
            print(
                f"    missed target {t['target']}: range_bin {t['range_bin']:.1f} "
                f"doppler_bin {t['doppler_bin']:.1f} (snr {t['snr_db']} dB)"
            )

    n_frames = iq.shape[0]
    n_targets = len(scene["targets"])
    print(
        f"summary: {n_frames * n_targets - total_miss}/{n_frames * n_targets} "
        f"targets detected, {total_fa} false alarms across {n_frames} frames"
    )
    return 1 if total_miss else 0


if __name__ == "__main__":
    sys.exit(main())
