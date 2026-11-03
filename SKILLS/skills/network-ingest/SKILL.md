---
name: network-ingest
description: "Use when tuning a Tomii graph that reads packets from the network (graph has network_config / a $network predecessor). Sizes receiver, resolution and worker threads and searches the ingest flags (rx_decode, ingest_shards, dispatch_chunk, reclaim_core, membind) that only help in combination."
---

# Skill: network-ingest

Tune a network-input Τομί application. For these graphs, per-frame latency is usually
set by packet ingest and frame admission, not by kernel compute, and the ingest flags
interact: several only help in combination. Searching them one at a time misses the
good configurations.

## Trigger

- The graph has `network_config` (JSON) / `app.network(...)` (Python)
- Latency is far above the sum of kernel times, frames are lost, or the run does not keep up
- [diagnose](../diagnose/SKILL.md) routes a network-input graph here

## Background: the threads of a network run

| Threads | Flag | Role |
|---------|------|------|
| main | — | parses the graph, initializes, then supervises |
| receivers | `receiver_threads` | own the UDP sockets, read packets (and decode them with `rx_decode`) |
| resolution | `system_threads` | admit packets into frames, resolve dependencies, dispatch tasks |
| workers | `workers` | run kernels |
| reclaim | `reclaim_core=C` | frees completed frames' results on core C |

All of them need their own cores. Workers are pinned from `core_offset`; keep the total
(workers + system + receivers + main + reclaim) within the CPUs the run may use, or
threads share cores and latency degrades sharply. Network graphs require
`receiver_threads >= 1`.

## Ingest flags and how they interact

| Knob | Effect | Interaction |
|------|--------|-------------|
| `rx_decode` | runs `extract_packet_func` on the receiver threads instead of resolution threads | needs enough receivers (≈ one per busy socket); with one receiver serving many sockets it falls behind and frames are lost. The extract function must be thread-safe |
| `receiver_threads` | number of socket readers | raise together with `rx_decode`; one receiver can serve several sockets when not decoding |
| `system_threads` | resolution threads | default 1; raise for many sockets / high packet rates |
| `ingest_shards` | one ingest channel per resolution thread | no effect unless `system_threads > 1` |
| `dispatch_chunk N` | dispatch admitted packets every N packets instead of once per poll | lowers admission latency for frames with many packets; try 4–16 |
| `reclaim_core C` | moves result freeing off the resolution thread | use a spare core not used by any other thread |
| `slots` | frames in flight | must cover frames that overlap in time (≥ 2–4 at full rate) |
| `frame_timeout_ms` | evicts a frame whose slot has been idle this long | turns packet loss into a dropped frame instead of a wedged run |
| `membind` | binds memory to NUMA nodes and opts out of NUMA balancing | use on multi-socket hosts (or set `kernel.numa_balancing=0`) |
| `no_malloc_tune` | skips the runtime's glibc malloc tuning | set it when the environment already sets `MALLOC_*` tunables |
| `use_rdtsc` | TSC timestamps | measurement precision only |
| `socket_recv_buf_bytes`, `recv_pool_size` | kernel / user receive buffers | raise if packets drop in bursts |

Typical failure of one-at-a-time tuning: enabling `rx_decode` while keeping a single
receiver for several sockets loses frames, so `rx_decode` looks harmful; raising
`receiver_threads` without `rx_decode` gives little; only the two together help.

## Steps

### 1. Measurement setup

- Start the Τομί receiver **first**, wait until it is listening (a few seconds), then start
  the sender. Starting the sender first loses the first frames and can hang the run.
- Measure frame latency as first-packet arrival → last task done; `overhead_pct` in
  `report.json` does not separate ingest from scheduling for these graphs.
- Each run must complete every frame the sender sent: compare the frame count with the
  sender's, check `stale_task_drops == 0`, and run the application's own output check.
- Keep `max_frames`, frame rate and warm-up exclusion identical across configurations.

### 2. Budget cores

Fix the CPU set first. Start from `system_threads=1`, `receiver_threads = num_sockets`
(cap at the cores available), `rx_decode=True`, `slots=4`, and give the remaining cores to
`workers`.

### 3. Search the ingest group jointly

Search these together, not one at a time:

1. `(receiver_threads, rx_decode)`: `{(num_sockets, True), (num_sockets/2, True), (1, False), (num_sockets, False)}`
2. `(system_threads, ingest_shards)`: `{(1, False), (2, False), (2, True)}`
3. `dispatch_chunk`: `{0, 4, 8, 16}`
4. `reclaim_core`: unset vs. a spare core

Keep the best of each step, then re-check step 1 once with the final settings.

### 4. Then search the worker side

With ingest fixed: `workers` (fewer workers can be faster when they would share cores with
ingest threads), `slots` `{2, 4, 8}`, then the boolean knobs from
[knob-search](../knob-search/SKILL.md).

### 5. Verify

Run the best configuration 3 times with the full frame count. All runs must be valid.

## Live operation

- `join_stream=True` numbers frames from the first id received, so a process can join a
  stream already in progress.
- `handover_listen=PATH` on the running process and `takeover=PATH` on its replacement hand
  the bound sockets over without rebinding (Linux): reconfigure a live stream (new graph or
  flags) while losing about one frame.

## Output

```
NETWORK-INGEST RESULT
=====================
Cores: <cpu set>; sockets: N; packets/frame: N; frame period: X us
Best:  workers=N system_threads=N receiver_threads=N rx_decode=.. ingest_shards=..
       dispatch_chunk=N reclaim_core=.. slots=N
Latency mean / p99: X / X us   (baseline: X / X us)
Validity: 3/3 runs, all frames completed and verified
```

## See also

- [knob-search](../knob-search/SKILL.md) — scheduler knobs after ingest is tuned
- [run-validate](../run-validate/SKILL.md) — validity checks
- `website/docs/reference/cli.md` (network ingest and deployment flags),
  `website/docs/guide/graphs/network-sources.md`
