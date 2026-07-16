#!/usr/bin/env python3
"""Synthetic FMCW radar scene generator.

Produces a dechirped (beat-signal) complex IQ capture for an FMCW radar plus a
ground-truth JSON, so the pipeline verifier knows the expected detections by
construction.

Signal model (per frame f, chirp m, sample n), for each target k:

    R_k(f)  = range + velocity * f * frame_interval          (target motion)
    rb_k(f) = 2 * B * R_k(f) / c                              (range bin, fractional)
    db_k    = 2 * v_k * fc / c * T_chirp * n_chirps           (Doppler bin, signed)
    x[m,n] += A_k * exp(j*(2*pi*(rb*n/N + db*m/M) - 4*pi*fc*R_k(f)/c))

with complex white Gaussian noise of total power noise_sigma^2 added per
sample. A_k = noise_sigma * 10^(snr_db/20), i.e. snr_db is per-sample SNR
before the 2D FFT processing gain of 10*log10(N*M) dB.

Output layout: little-endian interleaved int16 I/Q, frame-major then
chirp-major ("scene.iq"), plus "scene.json" with radar parameters and
per-frame ground-truth bins.
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np

C = 299_792_458.0

DEFAULT_TARGETS = [
    # (range m, velocity m/s, per-sample SNR dB)
    (80.0, 5.0, -5.0),
    (200.0, -12.0, -10.0),
    (350.0, 15.0, 0.0),
]


def parse_target(spec: str):
    parts = [float(p) for p in spec.split(",")]
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"target must be 'range_m,velocity_mps,snr_db', got {spec!r}"
        )
    return tuple(parts)


def build_parser():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--n-samples", type=int, default=1024, help="samples per chirp (range FFT size)")
    p.add_argument("--n-chirps", type=int, default=128, help="chirps per frame/CPI (Doppler FFT size)")
    p.add_argument("--n-frames", type=int, default=20, help="number of frames (CPIs) to generate")
    p.add_argument("--fs", type=float, default=20.48e6, help="ADC sample rate [Hz]")
    p.add_argument("--bandwidth", type=float, default=307.2e6, help="chirp sweep bandwidth [Hz]")
    p.add_argument("--carrier", type=float, default=77e9, help="carrier frequency [Hz]")
    p.add_argument(
        "--chirp-interval",
        type=float,
        default=None,
        help="chirp repetition interval [s] (default: n_samples/fs, i.e. no idle time)",
    )
    p.add_argument(
        "--frame-interval",
        type=float,
        default=10e-3,
        help="frame (CPI) repetition interval [s]; sets the sender's default pacing",
    )
    p.add_argument("--noise-sigma", type=float, default=512.0, help="total noise std in ADC counts")
    p.add_argument(
        "--target",
        action="append",
        type=parse_target,
        metavar="R,V,SNR",
        help="target as 'range_m,velocity_mps,snr_db' (repeatable; default: 3 built-in targets)",
    )
    p.add_argument("--seed", type=int, default=1, help="RNG seed (deterministic output)")
    p.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "out",
        help="output directory for scene.iq / scene.json",
    )
    return p


def target_bins(rng_m, vel_mps, frame_idx, radar):
    """Fractional (range_bin, doppler_bin_signed) for one target in one frame."""
    r = rng_m + vel_mps * frame_idx * radar["frame_interval_s"]
    range_bin = 2.0 * radar["bandwidth_hz"] * r / C
    doppler_bin = (
        2.0 * vel_mps * radar["carrier_hz"] / C
        * radar["chirp_interval_s"] * radar["n_chirps"]
    )
    return r, range_bin, doppler_bin


def main(argv=None):
    args = build_parser().parse_args(argv)
    n, m = args.n_samples, args.n_chirps
    t_chirp = args.chirp_interval if args.chirp_interval is not None else n / args.fs
    targets = args.target if args.target else DEFAULT_TARGETS

    radar = {
        "n_samples": n,
        "n_chirps": m,
        "n_frames": args.n_frames,
        "fs_hz": args.fs,
        "bandwidth_hz": args.bandwidth,
        "carrier_hz": args.carrier,
        "chirp_interval_s": t_chirp,
        "frame_interval_s": args.frame_interval,
        "noise_sigma": args.noise_sigma,
        "range_resolution_m": C / (2.0 * args.bandwidth),
        "max_range_m": n * C / (2.0 * args.bandwidth),
        "max_velocity_mps": C / (4.0 * args.carrier * t_chirp),
    }

    v_max = radar["max_velocity_mps"]
    r_max = radar["max_range_m"]
    for r, v, _ in targets:
        if not 0.0 < r < r_max:
            sys.exit(f"target range {r} m outside unambiguous range (0, {r_max:.1f}) m")
        if abs(v) >= v_max:
            sys.exit(f"target velocity {v} m/s outside unambiguous (+-{v_max:.2f}) m/s")

    rng = np.random.default_rng(args.seed)
    sample_idx = np.arange(n)[None, :]  # (1, N)
    chirp_idx = np.arange(m)[:, None]  # (M, 1)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    iq_path = args.out_dir / "scene.iq"
    ground_truth = []
    peak = 0.0

    with open(iq_path, "wb") as fh:
        for f in range(args.n_frames):
            frame = np.zeros((m, n), dtype=np.complex128)
            truth = []
            for k, (r0, v, snr_db) in enumerate(targets):
                r, rb, db = target_bins(r0, v, f, radar)
                amp = args.noise_sigma * 10.0 ** (snr_db / 20.0)
                phase = (
                    2.0 * np.pi * (rb * sample_idx / n + db * chirp_idx / m)
                    - 4.0 * np.pi * radar["carrier_hz"] * r / C
                )
                frame += amp * np.exp(1j * phase)
                truth.append(
                    {
                        "target": k,
                        "range_m": r,
                        "velocity_mps": v,
                        "snr_db": snr_db,
                        "range_bin": rb,
                        "doppler_bin_signed": db,
                        "doppler_bin": db % m,
                    }
                )
            noise = rng.normal(scale=args.noise_sigma / np.sqrt(2.0), size=(m, n, 2))
            frame += noise[..., 0] + 1j * noise[..., 1]

            interleaved = np.empty((m, 2 * n), dtype=np.float64)
            interleaved[:, 0::2] = frame.real
            interleaved[:, 1::2] = frame.imag
            peak = max(peak, float(np.abs(interleaved).max()))
            fh.write(np.round(interleaved).astype("<i2").tobytes())
            ground_truth.append(truth)

    if peak > 0.9 * 32767:
        print(f"WARNING: peak amplitude {peak:.0f} near int16 clipping", file=sys.stderr)

    scene = {
        "radar": radar,
        "iq_file": iq_path.name,
        "dtype": "int16-interleaved-le",
        "layout": "frame-major, chirp-major",
        "targets": [
            {"range_m": r, "velocity_mps": v, "snr_db": s} for r, v, s in targets
        ],
        "seed": args.seed,
        "ground_truth": ground_truth,
    }
    scene_path = args.out_dir / "scene.json"
    scene_path.write_text(json.dumps(scene, indent=2))

    size_mb = iq_path.stat().st_size / 1e6
    print(
        f"wrote {iq_path} ({size_mb:.1f} MB, {args.n_frames} frames of "
        f"{m}x{n} IQ) and {scene_path}"
    )
    print(
        f"radar: dR={radar['range_resolution_m']:.3f} m  Rmax={r_max:.0f} m  "
        f"Vmax=+-{v_max:.2f} m/s  CPI={m * t_chirp * 1e3:.2f} ms"
    )


if __name__ == "__main__":
    main()
