"""Build the Tomii MIMO 4-node graph using the Python API.

Topology:
    $network (bs_ant_num UDP sockets)
        ├──► fft   (factor=total_ul_symbols,   1:1 from UL data packets)
        │              └──┐
        ├──► csi   (factor=total_pilot_symbols, 1:1 from pilot packets)
        │              ↓
        │          beam  (factor=beam_events_per_symbol,
        │                 $barrier on all csi tasks)
        │              └──┐
        └────────────────┴──► demul  (factor=total_demul_tasks,
                                       $barrier on fft group_by antennas,
                                       $barrier on all beam tasks)

This Python builder is the single source of truth for the MIMO graph. It is
preferred over a committed static JSON because the tddconfig path is resolved at
build time, so the graph carries no machine-specific absolute path.

Usage:
    python build_graph.py                         # print JSON to stdout
    python build_graph.py --out graphs/out.json   # save to file
    python build_graph.py --config graphs/tddconfig-16x16.json --dump  # 16x16 + verify node
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BENCH_ROOT = HERE.parents[1]
DEVELOP_ROOT = BENCH_ROOT.parents[1]
sys.path.insert(0, str(DEVELOP_ROOT))

import tomii as tm
from tomii._node import Node as _Node
from tomii._types import String, TypedValue

# $network is a virtual predecessor node — not added to the graph,
# just used as a handle for out() calls that reference incoming packets.
_net = _Node("$network", func="")

# Runtime-provided current-instance index placeholder
_index = TypedValue("$ref", "$index")

AGORA_CONFIG = "~/Agora/files/config/ci/tddconfig-4x4.json"


def build_mimo_graph(config_path: str = AGORA_CONFIG, dump: bool = False) -> tm.Graph:
    """Return a Graph for the 4-node MIMO uplink pipeline.

    Parameters
    ----------
    config_path:
        Path to the tddconfig JSON consumed by Agora's packet sender. The path is
        expanded (``~``) and resolved to an absolute path at build time, so the
        emitted graph carries no machine-specific literal — it is generated fresh
        on each machine rather than committed as a static file.
    dump:
        When True, append a terminal ``dump`` node (``dump_demod_if_env``) that
        serialises the post-demul buffer to ``$TOMII_VERIFY_PATH``. Used by
        ``verify.py`` for byte-for-bit determinism checks; left off for perf runs.
    """
    config_path = str(Path(config_path).expanduser().resolve())
    app = tm.Graph()

    # ------------------------------------------------------------------
    # Scalar initialisations
    # ------------------------------------------------------------------
    config_file = app.var("config_file", value=String(config_path))
    config = app.var("config", func="create_config", args=[config_file])
    packet_config = app.var("packet_config", func="create_packet_config", args=[config])
    framestats = app.var("framestats", func="create_framestats", args=[config])

    packets_per_frame = app.var(
        "packets_per_frame", func="get_packets_per_frame", args=[config, packet_config]
    )
    antennas = app.var("antennas", func="get_antennas", args=[config])
    packet_length = app.var(
        "packet_length", func="get_packet_length", args=[packet_config]
    )
    server_address = app.var("server_address", func="get_server_address", args=[config])
    base_port = app.var("base_port", func="get_base_port", args=[config])
    pilot_packet_count = app.var(
        "pilot_packet_count", func="get_pilot_packet_count", args=[config, framestats]
    )

    total_pilot_symbols = app.var(
        "total_pilot_symbols", func="total_pilot_symbols", args=[config, framestats]
    )
    total_ul_symbols = app.var(
        "total_ul_symbols", func="total_uplink_symbols", args=[config, framestats]
    )
    ul_symbols = app.var("ul_symbols", func="get_uplink_symbols", args=[framestats])

    beam_events_per_symbol = app.var(
        "beam_events_per_symbol", func="beam_events_per_symbol", args=[config]
    )
    demul_events_per_symbol = app.var(
        "demul_events_per_symbol", func="demul_events_per_symbol", args=[config]
    )
    total_demul_tasks = app.var(
        "total_demul_tasks", func="total_demul_tasks", args=[config, framestats]
    )

    # ------------------------------------------------------------------
    # Buffer initialisations (shared across instances)
    # ------------------------------------------------------------------
    fft_buffer = app.var(
        "fft_buffer", func="create_fft_buffer", args=[config, framestats]
    )
    csi_buffer = app.var("csi_buffer", func="create_csi_buffer", args=[config])
    demod_buffers = app.var(
        "demod_buffers", func="create_demod_buffers", args=[config, framestats]
    )
    ul_beam_matrices = app.var(
        "ul_beam_matrices", func="create_ul_beam_matrices", args=[config]
    )
    ul_base_scs = app.var("ul_base_scs", func="create_ul_base_scs", args=[config])
    demul_base_scs = app.var(
        "demul_base_scs", func="create_demul_base_scs", args=[config]
    )

    # ------------------------------------------------------------------
    # Per-instance (factored) initialisations
    # ------------------------------------------------------------------
    fft_struct = app.var(
        "fft_struct", func="create_fft_struct", factor=total_ul_symbols, args=[config]
    )
    csi_struct = app.var(
        "csi_struct",
        func="create_fft_struct",
        factor=total_pilot_symbols,
        args=[config],
    )
    beam_struct = app.var(
        "beam_struct", func="create_beam_struct", factor=beam_events_per_symbol, args=[]
    )
    demul_struct = app.var(
        "demul_struct",
        func="create_demul_struct",
        factor=total_demul_tasks,
        args=[config],
    )
    ul_symbol = app.var(
        "ul_symbol", func="get_ul_symbol", factor=ul_symbols, args=[framestats, _index]
    )

    # ------------------------------------------------------------------
    # Network config — one UDP socket per antenna
    # ------------------------------------------------------------------
    app.network(
        socket_type="udp",
        num_sockets=antennas,
        packet_length=packet_length,
        stream_packets=packets_per_frame,
        buffer_depth=2000,
        address=server_address,
        start_port=base_port,
        extract_packet_func="process_packet",
        id_function="get_frame_id",
        index_function=tm.IndexFunc("get_packet_slot", args=[config]),
    )

    # ------------------------------------------------------------------
    # Nodes
    # ------------------------------------------------------------------

    # fft — UL data packets (pilot_packet_count .. packets_per_frame)
    fft = app.node(
        "fft",
        func="fft_op",
        factor=total_ul_symbols,
        args=[
            _net.out("pilot_packet_count", packets_per_frame),  # $res from $network
            config,
            framestats,
            fft_struct,
            fft_buffer,
            _index,
        ],
    )

    # csi — pilot packets (0 .. pilot_packet_count)
    csi = app.node(
        "csi",
        func="csi_op",
        factor=total_pilot_symbols,
        args=[
            _net.out(0, pilot_packet_count),  # $res from $network
            config,
            framestats,
            csi_struct,
            csi_buffer,
        ],
    )

    # beam — ZF beamweights; fires after ALL csi tasks complete
    beam = app.node(
        "beam",
        func="beam_op",
        factor=beam_events_per_symbol,
        priority="high",
        args=[
            config,
            ul_base_scs,
            beam_struct,
            csi_buffer,
            ul_beam_matrices,
            csi.out(0),  # data from csi[0]
            _index,
            csi.wait(0, total_pilot_symbols),  # barrier: all csi done
        ],
    )

    # demul — equalization + demap; fires after all fft (grouped) AND all beam
    demul = app.node(
        "demul",
        func="demul_op",
        factor=total_demul_tasks,
        group_size=demul_events_per_symbol,
        priority="low",
        args=[
            config,
            framestats,
            demul_base_scs,
            demul_struct,
            fft_buffer,
            demod_buffers,
            ul_beam_matrices,
            fft.out(0),  # data from fft[0]
            ul_symbol,
            _index,
            fft.wait(
                0, total_ul_symbols, group_by=antennas
            ),  # barrier: fft group_by antennas
            beam.wait(0, beam_events_per_symbol),  # barrier: all beam done
        ],
    )

    # dump — optional terminal node: serialise the demod buffer to
    # $TOMII_VERIFY_PATH after all demul tasks complete (verify.py only).
    if dump:
        app.node(
            "dump",
            func="dump_demod_if_env",
            factor=1,
            args=[
                demod_buffers,
                demul.wait(0, total_demul_tasks),  # barrier: all demul done
            ],
        )

    return app


# ---------------------------------------------------------------------------
# CLI: dump JSON for inspection
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Print or save the MIMO graph JSON built via Python API."
    )
    p.add_argument("--config", default=AGORA_CONFIG, help="tddconfig JSON path")
    p.add_argument(
        "--dump",
        action="store_true",
        help="append the terminal dump_demod_if_env node (verify.py determinism check)",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="write JSON to this file instead of stdout",
    )
    args = p.parse_args()

    graph = build_mimo_graph(args.config, dump=args.dump)
    json_str = graph.to_json()

    if args.out:
        args.out.write_text(json_str, encoding="utf-8")
        print(f"Saved: {args.out}")
    else:
        print(json_str)
