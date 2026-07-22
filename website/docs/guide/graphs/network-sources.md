---
title: Network sources
sidebar_label: Network sources
---

A network-driven graph starts frames from UDP or TCP packets instead of a
counter. Dedicated receiver threads ingest the incoming packet stream, group
packets into frames by id, assign each frame to a slot, and dispatch consumer
tasks as packets arrive — a consumer does not wait for its whole frame. This
packet-level overlap is the mechanism the MIMO benchmark measures
(`bench/mimo-bench/README.md`).

## Configuring the source

In Python, `app.network(**cfg)` sets the configuration; in JSON it is the
top-level `network_config` block. From the MIMO benchmark graph, generated
by `bench/mimo-bench/tomii/build_graph.py` (trimmed):

```json
"network_config": {
    "socket_type": "udp",
    "num_sockets": "antennas",
    "packet_length": "packet_length",
    "frame_packets": "packets_per_frame",
    "buffer_depth": 2000,
    "address": "server_address",
    "start_port": "base_port",
    "extract_packet_func": "process_packet",
    "id_function": "get_frame_id",
    "index_function": {
        "function": "get_packet_slot",
        "args": [ { "type": "$ref", "value": "config" } ]
    }
}
```

Fields (from `NetworkConfigJson` in `tomii-core/src/json_structs.rs`):

| Field | Meaning |
|---|---|
| `socket_type` | `"udp"` or `"tcp"` |
| `num_sockets` | Sockets to open (factor: literal or variable name) |
| `packet_length` | Bytes per packet |
| `frame_packets` | Packets that make up one frame |
| `buffer_depth` | Receive buffer depth per socket (default 128) |
| `address`, `start_port` | Bind address; socket `k` uses `start_port + k` |
| `extract_packet_func` | Plugin function: raw bytes → packet value |
| `id_function` | Plugin function: packet → frame id |
| `index_function` | Plugin function: packet → consuming node-instance index (required) |

Factor-valued fields accept variable names, so socket counts and packet
sizes can come from graph initializations — the MIMO graph derives all of
them from a config object.

From Python, the same block is built with `app.network(...)`, passing `Var`
objects where the JSON uses variable names and a `tm.IndexFunc(function,
args)` for `index_function`. The serializer rejects a config without
`index_function`, since the runtime requires it (`tomii/_serialize.py`).

## How a packet becomes a task

1. A receiver thread reads a packet and calls `extract_packet_func`.
2. `id_function` returns the frame id. The first packet of a new frame
   claims a slot; later packets of the same frame route to that slot.
3. `index_function` returns which instance of the network node this packet
   is. It receives the packet plus its configured args and must return a
   `usize` (`tomii-core/src/runtime/packet_processing.rs`).
4. Nodes that consume the packet are dispatched immediately.

## Consuming packets: `$network`

Nodes reference packet data with a `$res` dependency whose predecessor is the
reserved name `$network`. From the MIMO graph:

```json
{
    "name": "fft",
    "factor": "total_ul_symbols",
    "function": "fft_op",
    "args": [
        { "type": "$res", "predecessor": { "name": "$network", "indexes": "0" } },
        { "type": "$ref", "value": "config" }
    ]
}
```

Instance `i` of `fft` runs when packet index `i` of the current frame has
arrived. `$network` also works inside `condition` args, which is how the MIMO
graph routes pilot symbols to `csi` and data symbols to `fft`.

## Receiver threads

Set `receiver_threads` in `run()` (CLI `--receiver-threads`, default 0).
Network packets are handled by these dedicated threads, not the task
scheduler. A network graph needs at least one receiver thread.

## Out-of-window packets

Only frames within the admission window (completed frames + `slots`) can
occupy a slot. Packets for frames beyond the window park in a bounded
`pending_frames` buffer and re-inject when the window advances. The buffer
holds at most `frame_packets × slots` packets; on overflow the furthest-out
frame is dropped whole, so a burst degrades the run instead of wedging it on
a permanently incomplete frame
(`tomii-core/src/runtime/shared_data.rs`,
`tomii-core/src/runtime/packet_processing.rs`). Dropped frames are counted
and reported.

## Complete examples

The 4×4 MIMO uplink benchmark under `bench/mimo-bench/` is the reference
network workload: an Agora sender emits frames over UDP, and per-packet FFT
tasks dispatch as packets arrive. Start the Tomii receiver first, then the
sender (`bench/mimo-bench/README.md`).

`examples/radar-pipeline/` is the self-contained one: an FMCW radar receiver
where one UDP packet carries one chirp. The packet header holds `frame_id`
and `chirp_id`; `id_function` (`get_frame_id`) groups packets into frames and
`index_function` (`get_chirp_index`) maps each packet to its `range_fft`
instance, so a range FFT dispatches the moment its chirp arrives — the graph
does not wait for the frame. Barriers then gate the Doppler FFT, CFAR
detection, and clustering stages. The sender paces chirps at the scene's
physical chirp interval; a whole-frame burst can overrun the kernel socket
buffer and drop packets before Tomii ever sees them. The example README has
the signal-chain diagram, the runbook, and the CPU/GPU kernel swap; measured
results are on the [benchmarks page](/docs/overview/benchmarks).
