"""Build the Tomii FMCW radar pipeline graph using the Python API.

Topology:
    $network (1 UDP socket, 1 packet = 1 chirp)
        └──► range_fft   (factor=n_chirps, 1:1 from chirp packets;
        │                 corner-turned write into the rd buffer)
        └──► doppler_fft (factor=n_tiles, $barrier on all range_fft;
        │                 per-tile chirp-axis FFT + power map)
        └──► cfar        (factor=n_tiles, $barrier on all doppler_fft;
        │                 2D CA-CFAR over the tile's range rows)
        └──► cluster     (factor=1, $barrier on all cfar;
                          strongest-cell-per-cluster detection list)

The graph is generated fresh per run (radar dimensions come from the scene
metadata) rather than committed as static JSON.

Usage:
    python build_graph.py --scene data/out/scene.json            # print JSON
    python build_graph.py --scene ... --out /tmp/radar.json      # save
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[1]
sys.path.insert(0, str(REPO_ROOT))

import tomii as tm
from tomii._node import Node as _Node
from tomii._types import String, TypedValue

# $network is a virtual predecessor node — not added to the graph,
# just used as a handle for out() calls that reference incoming packets.
_net = _Node("$network", func="")

# Runtime-provided current-instance index placeholder
_index = TypedValue("$ref", "$index")


def build_radar_graph(
    n_samples: int,
    n_chirps: int,
    *,
    n_tiles: int = 8,
    frame_wnd: int = 2,
    guard: int = 2,
    train: int = 8,
    pfa_scale: float = 15.0,
    max_dets_per_tile: int = 64,
    address: str = "127.0.0.1",
    port: int = 8100,
) -> tm.Graph:
    """Return a Graph for the 4-node FMCW radar pipeline.

    frame_wnd sizes the shared rd/power/detection buffers and must be >= the
    runtime --slots value (buffers are indexed frame_id % frame_wnd).
    CFAR parameters (guard/train/pfa_scale) must match data/reference_check.py
    defaults for verify.py parity.
    """
    app = tm.Graph()

    # ------------------------------------------------------------------
    # Scalar initialisations
    # ------------------------------------------------------------------
    config = app.var(
        "config",
        func="make_radar_config",
        args=[
            n_samples,
            n_chirps,
            n_tiles,
            frame_wnd,
            guard,
            train,
            float(pfa_scale),
            max_dets_per_tile,
            String(address),
            port,
        ],
    )
    packet_length = app.var("packet_length", func="get_packet_length", args=[config])
    frame_packets = app.var("frame_packets", func="get_frame_packets", args=[config])
    n_chirps_v = app.var("n_chirps", func="get_n_chirps", args=[config])
    n_tiles_v = app.var("n_tiles", func="get_n_tiles", args=[config])
    num_channels = app.var("num_channels", func="get_num_channels", args=[config])
    server_address = app.var("server_address", func="get_address", args=[config])
    base_port = app.var("base_port", func="get_port", args=[config])

    # ------------------------------------------------------------------
    # Kernel context + shared buffers (frame_wnd concurrent frames)
    # ------------------------------------------------------------------
    ctx = app.var("kernel_ctx", func="create_kernel_ctx", args=[config])
    rd_buffer = app.var("rd_buffer", func="create_rd_buffer", args=[ctx])
    power_buffer = app.var("power_buffer", func="create_power_buffer", args=[ctx])
    dets_buffer = app.var("dets_buffer", func="create_dets_buffer", args=[ctx])

    # Per-instance FFT workspaces (plan + scratch; one per task instance)
    range_ws = app.var("range_ws", func="create_range_ws", factor=n_chirps_v, args=[ctx])
    doppler_ws = app.var(
        "doppler_ws", func="create_doppler_ws", factor=n_tiles_v, args=[ctx]
    )

    # ------------------------------------------------------------------
    # Network config — one UDP socket, one packet per chirp
    # ------------------------------------------------------------------
    app.network(
        socket_type="udp",
        num_sockets=num_channels,
        packet_length=packet_length,
        frame_packets=frame_packets,
        buffer_depth=2000,
        address=server_address,
        start_port=base_port,
        extract_packet_func="process_packet",
        id_function="get_frame_id",
        index_function=tm.IndexFunc("get_chirp_index"),
    )

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------
    range_fft = app.node(
        "range_fft",
        func="range_fft",
        factor=n_chirps_v,
        args=[
            _net.out(0, n_chirps_v),  # $res from $network: this chirp's packet
            config,
            ctx,
            range_ws,
            rd_buffer,
            _index,
        ],
    )

    doppler_fft = app.node(
        "doppler_fft",
        func="doppler_fft",
        factor=n_tiles_v,
        args=[
            config,
            ctx,
            doppler_ws,
            rd_buffer,
            power_buffer,
            range_fft.out(0),  # frame_id
            _index,
            range_fft.wait(0, n_chirps_v),  # barrier: all chirps transformed
        ],
    )

    cfar = app.node(
        "cfar",
        func="cfar",
        factor=n_tiles_v,
        args=[
            config,
            ctx,
            power_buffer,
            dets_buffer,
            range_fft.out(0),  # frame_id
            _index,
            doppler_fft.wait(0, n_tiles_v),  # barrier: full power map ready
        ],
    )

    app.node(
        "cluster",
        func="cluster",
        factor=1,
        args=[
            config,
            ctx,
            dets_buffer,
            range_fft.out(0),  # frame_id
            cfar.wait(0, n_tiles_v),  # barrier: all tiles CFAR'd
        ],
    )

    return app


def radar_dims_from_scene(scene_path: Path) -> tuple[int, int]:
    radar = json.loads(scene_path.read_text())["radar"]
    return radar["n_samples"], radar["n_chirps"]


# ---------------------------------------------------------------------------
# CLI: dump JSON for inspection
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Print or save the radar graph JSON built via Python API."
    )
    p.add_argument(
        "--scene",
        type=Path,
        default=HERE / "data" / "out" / "scene.json",
        help="scene.json (radar dimensions source)",
    )
    p.add_argument("--tiles", type=int, default=8)
    p.add_argument("--frame-wnd", type=int, default=2, help="buffered frames (>= --slots)")
    p.add_argument("--guard", type=int, default=2)
    p.add_argument("--train", type=int, default=8)
    p.add_argument("--pfa-scale", type=float, default=15.0)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8100)
    p.add_argument("--out", type=Path, default=None)
    args = p.parse_args()

    n_samples, n_chirps = radar_dims_from_scene(args.scene)
    graph = build_radar_graph(
        n_samples,
        n_chirps,
        n_tiles=args.tiles,
        frame_wnd=args.frame_wnd,
        guard=args.guard,
        train=args.train,
        pfa_scale=args.pfa_scale,
        address=args.host,
        port=args.port,
    )
    json_str = graph.to_json()

    if args.out:
        args.out.write_text(json_str, encoding="utf-8")
        print(f"Saved: {args.out}")
    else:
        print(json_str)
