#!/usr/bin/env python3
"""GNU Radio 3.10 baseline for the radar-pipeline workload.

Same UDP chirp stream as examples/radar-pipeline (sender.py), same DSP:
  udp chirp source (py, header strip + i16->c64)
    -> stream_to_vector(n_samples)
    -> fft.fft_vcc (native C++/FFTW, Hann window)      # range FFT per chirp
    -> stream_to_vector(n_chirps)                       # assemble one CPI
    -> radar_detect (py: corner turn + Doppler FFT/CFAR/cluster via ctypes
                     into the SAME libradar_kernels.so used by Tomii)
Each stage is a separate GR block so the GNU Radio scheduler orchestrates the
pipeline; per-frame latency = first-packet arrival -> detections done.

Outputs: detections file (verify.py format) and a latency CSV (frame_id,us).
"""

import argparse
import ctypes as C
import json
import socket
import struct
import sys
import time
from pathlib import Path

import numpy as np
import pmt
from gnuradio import blocks, fft, gr

HERE = Path(__file__).resolve().parent
RADAR = HERE.parents[2] / "examples" / "radar-pipeline"
HEADER = struct.Struct("<4I48x")
DATA_OFFSET = 64


class Det(C.Structure):
    _fields_ = [
        ("range_bin", C.c_uint32),
        ("doppler_bin", C.c_uint32),
        ("power", C.c_float),
    ]


def load_kernels():
    lib = C.CDLL(str(RADAR / "kernels" / "libradar_kernels.so"))
    lib.rk_init.restype = C.c_void_p
    lib.rk_init.argtypes = [C.c_uint32] * 6 + [C.c_float, C.c_uint32]
    for f in ("rk_make_doppler_ws", "rk_alloc_rd", "rk_alloc_power", "rk_alloc_dets"):
        getattr(lib, f).restype = C.c_void_p
        getattr(lib, f).argtypes = [C.c_void_p]
    lib.rk_doppler_fft.argtypes = [C.c_void_p, C.c_void_p, C.c_uint32, C.c_void_p, C.c_void_p, C.c_uint32]
    lib.rk_cfar.restype = C.c_uint32
    lib.rk_cfar.argtypes = [C.c_void_p, C.c_uint32, C.c_void_p, C.c_void_p, C.c_uint32]
    lib.rk_cluster.restype = C.c_uint32
    lib.rk_cluster.argtypes = [C.c_void_p, C.c_void_p, C.c_uint32, C.POINTER(Det), C.c_uint32]
    return lib


class ChirpUdpSource(gr.sync_block):
    """Receives chirp packets, strips headers, emits complex64 samples.

    Tags the first sample of each frame with the frame_id and arrival time
    (time.perf_counter_ns) for end-to-end latency measurement downstream.
    """

    def __init__(self, port, n_samples, n_chirps, expected_frames):
        gr.sync_block.__init__(self, "chirp_udp_source", [], [np.complex64])
        self.n_samples = n_samples
        self.frame_len = n_samples * n_chirps
        self.expected_frames = expected_frames
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 32 << 20)
        self.sock.bind(("0.0.0.0", port))
        self.sock.settimeout(0.2)
        from collections import deque
        self.chunks = deque()  # fixed-size per-packet sample arrays
        self.chunk_off = 0  # consumed samples of chunks[0]
        self.tags = deque()  # (abs_sample_index, frame_id, t_ns)
        self.abs_in = 0  # samples enqueued
        self.abs_out = 0  # samples emitted
        self.frames_seen = set()
        self.done = False

    def work(self, input_items, output_items):
        out = output_items[0]
        produced = 0
        while produced < len(out):
            if self.chunks:
                chunk = self.chunks[0]
                avail = len(chunk) - self.chunk_off
                n = min(len(out) - produced, avail)
                out[produced : produced + n] = chunk[self.chunk_off : self.chunk_off + n]
                produced += n
                self.chunk_off += n
                if self.chunk_off == len(chunk):
                    self.chunks.popleft()
                    self.chunk_off = 0
                continue
            if self.done:
                break
            try:
                pkt, _ = self.sock.recvfrom(65536)
            except socket.timeout:
                if self.frames_seen and len(self.frames_seen) >= self.expected_frames:
                    self.done = True
                break
            t_ns = time.perf_counter_ns()
            frame_id, chirp_id, _chan, _n_samp = struct.unpack_from("<4I", pkt)
            iq = np.frombuffer(pkt, dtype="<i2", offset=DATA_OFFSET).astype(np.float32)
            self.chunks.append((iq[0::2] + 1j * iq[1::2]).astype(np.complex64))
            if chirp_id == 0:
                self.tags.append((self.abs_in, frame_id, t_ns))
            self.abs_in += self.n_samples
            self.frames_seen.add(frame_id)
        # attach tags whose absolute sample index falls in this window
        while self.tags and self.tags[0][0] < self.abs_out + produced:
            idx, fid, t_ns = self.tags.popleft()
            self._add_tag(idx - self.abs_out, fid, t_ns)
        self.abs_out += produced
        if produced == 0 and self.done:
            return -1
        return produced

    def _add_tag(self, offset, frame_id, t_ns):
        self.add_item_tag(
            0,
            self.nitems_written(0) + offset,
            pmt.intern("frame"),
            pmt.to_pmt({"id": int(frame_id), "t_ns": int(t_ns)}),
        )


class RadarDetect(gr.sync_block):
    """Consumes one CPI (n_chirps x n_samples range-FFT'd vectors) per item.

    Corner-turns to range-major, then Doppler FFT + CFAR + cluster via the
    same C kernels Tomii uses. Writes detections + per-frame latency.
    """

    def __init__(self, lib, n_samples, n_chirps, tiles, guard, train, pfa_scale,
                 det_path, lat_path, expected_frames, tb):
        vec = n_samples * n_chirps
        gr.sync_block.__init__(self, "radar_detect", [(np.complex64, vec)], [])
        self.n, self.m, self.tiles = n_samples, n_chirps, tiles
        self.lib = lib
        self.ctx = lib.rk_init(n_samples, n_chirps, 1, tiles, guard, train,
                               C.c_float(pfa_scale), 64)
        self.dws = lib.rk_make_doppler_ws(self.ctx)
        self.rd = lib.rk_alloc_rd(self.ctx)
        self.power = lib.rk_alloc_power(self.ctx)
        self.dets = lib.rk_alloc_dets(self.ctx)
        self.rd_view = np.ctypeslib.as_array(
            (C.c_float * (2 * vec)).from_address(self.rd)
        ).view(np.complex64).reshape(n_samples, n_chirps)
        self.out = (Det * 1024)()
        self.det_f = open(det_path, "w")
        self.lat_f = open(lat_path, "w")
        self.lat_f.write("frame_id,latency_us\n")
        self.frames_done = 0
        self.expected_frames = expected_frames
        self.tb = tb

    def work(self, input_items, output_items):
        frames = input_items[0]
        tags = self.get_tags_in_window(0, 0, len(frames))
        for i, frame in enumerate(frames):
            # one tag per frame, order preserved through vectorization
            meta = pmt.to_python(tags[i].value) if i < len(tags) else None
            # corner turn: [chirp][range] -> [range][chirp]
            self.rd_view[:] = frame.reshape(self.m, self.n).T
            for t in range(self.tiles):
                self.lib.rk_doppler_fft(self.ctx, self.dws, t, self.rd, self.power, 0)
            for t in range(self.tiles):
                self.lib.rk_cfar(self.ctx, t, self.power, self.dets, 0)
            ndet = self.lib.rk_cluster(self.ctx, self.dets, 0, self.out, 1024)
            dets = sorted(
                (self.out[k].range_bin, self.out[k].doppler_bin, self.out[k].power)
                for k in range(ndet)
            )
            fid = meta["id"] if meta else self.frames_done
            line = f"frame {fid}:" + "".join(f" {r},{d},{p:.3e}" for r, d, p in dets)
            self.det_f.write(line + "\n")
            if meta:
                lat_us = (time.perf_counter_ns() - meta["t_ns"]) / 1e3
                self.lat_f.write(f"{fid},{lat_us:.2f}\n")
            self.frames_done += 1
        if self.frames_done >= self.expected_frames:
            self.det_f.flush()
            self.lat_f.flush()
        return len(frames)


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--scene", type=Path, default=RADAR / "data" / "out" / "scene.json")
    p.add_argument("--port", type=int, default=8101)
    p.add_argument("--frames", type=int, required=True)
    p.add_argument("--tiles", type=int, default=8)
    p.add_argument("--guard", type=int, default=2)
    p.add_argument("--train", type=int, default=8)
    p.add_argument("--pfa-scale", type=float, default=15.0)
    p.add_argument("--out-dir", type=Path, default=HERE / "results")
    p.add_argument("--max-noutput", type=int, default=32768)
    args = p.parse_args()

    radar = json.loads(args.scene.read_text())["radar"]
    n, m = radar["n_samples"], radar["n_chirps"]
    args.out_dir.mkdir(parents=True, exist_ok=True)

    lib = load_kernels()
    tb = gr.top_block()
    src = ChirpUdpSource(args.port, n, m, args.frames)
    to_vec = blocks.stream_to_vector(np.dtype(np.complex64).itemsize, n)
    win = np.hanning(n).astype(np.float32)
    range_fft = fft.fft_vcc(n, True, win.tolist(), False, 1)
    to_frame = blocks.stream_to_vector(np.dtype(np.complex64).itemsize * n, m)
    sink = RadarDetect(
        lib, n, m, args.tiles, args.guard, args.train, args.pfa_scale,
        args.out_dir / "gr_detections.txt", args.out_dir / "gr_latency.csv",
        args.frames, tb,
    )
    tb.connect(src, to_vec, range_fft, to_frame, sink)

    t0 = time.monotonic()
    tb.start(args.max_noutput)
    while sink.frames_done < args.frames:
        time.sleep(0.05)
        if time.monotonic() - t0 > 600:
            print("TIMEOUT waiting for frames", file=sys.stderr)
            break
    tb.stop()
    tb.wait()
    print(f"gnuradio: processed {sink.frames_done} frames in {time.monotonic() - t0:.1f}s")


if __name__ == "__main__":
    main()
