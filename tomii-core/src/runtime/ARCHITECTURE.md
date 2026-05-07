# Runtime Architecture

## Overview

The `tomii-core` runtime executes a task-graph described by a `Graph` struct
(parsed from JSON) by dispatching node instances to a Rayon worker pool and
tracking inter-node data dependencies with threshold-based atomic counters. It
does not use `async`/`await`, does not perform dynamic allocation on the task
hot path (all per-iteration buffers are preallocated as thread-locals), and
never touches the graph definition after startup — the graph and its derived
routing tables are immutable for the lifetime of `TomiiRt::run`.

---

## Thread model

| Thread type | Count | Role |
|---|---|---|
| Resolution thread | `system_threads` (default 1) | Drains the `batch_queue`, propagates dependencies, checks slot completion, manages slot lifecycle |
| Worker thread | `workers` (Rayon pool) | Executes plugin functions, optionally resolves successor dependencies inline (`worker_resolvable` path) |
| Receiver thread | `receiver_threads` (0 by default) | Reads UDP/TCP sockets, forwards packets as `PacketMessage` to the resolution thread via a flume channel |

Resolution threads run `resolution_loop::resolution`. Worker threads are
managed by `SchedulerImpl` (Rayon or plugin variant). Receiver threads run
`network::multi_socket_receiver_loop`.

All three thread types share a single `Arc<SharedData>` cloned at spawn time.
Hot-path functions receive narrow borrow bundles (`ResolveCtx`, `SchedCtx`)
instead of `&SharedData` directly to keep coupling visible at the type level.

---

## Dataflow: from packet/task to completion

```
Network socket
    |  (PacketMessage via flume)
    v
packet_processing.rs
    |
    +---> assign_stream_to_available_slot()
    |         |
    |         +-- Inactive slot: mark Active, spawn initial_nodes()
    |         +-- All slots busy: buffer packet in slot_buffers, mark Buffering
    |
    v
active_packet_batch  (Vec<(NodeInfo, Option<CmTypes>)>)
    |
    v
process_batch_resolution()
    |
    |  Phase 1: store result for network packets (Some(result))
    |           compute task results: already stored by execute_task (None)
    |
    |  Phase 2: decrement pending_tasks / pending_cond_tasks (SeqCst)
    |
    |  Phase 3: collect successors, call decrease_and_get_ready_into(),
    |           accumulate ready NodeInfos, flush to workers via dispatch_nodes()
    |
    |  Phase 4: decrement processing_count (SeqCst)
    |               |
    +---------------+
    |
    v
check_slots() [called unconditionally every iteration — Bug #21 fix]
    |
    +-- detect_and_claim_slot_completion(): load 3 SeqCst counters, CAS claim
    +-- reset_slot_state(): bump generation, reset counters
    +-- process_slot_completion(): reinit_slot() THEN release_slot()
    +-- activate_buffered_slot(): slot-priority mode only
    +-- restart_slot_nonnetwork(): non-network, non-slot-priority mode
```

---

## Dual completion path

When a worker finishes executing a plugin function, the result is stored via
`node_results.set()`. The worker then takes one of two paths:

### 1. Worker-resolvable path (fast path)

Condition: `NodeCacheEntry::worker_resolvable == true`, meaning every successor
of this node is a non-condition node.

The worker calls `worker_resolve_successors` in `task_execution.rs` directly,
without touching `batch_queue`. This path:

1. Increments `processing_count` to prevent premature completion detection.
2. Decrements `pending_tasks` or `pending_cond_tasks`.
3. Calls `collect_successors_for_node_into` + `decrement_and_collect_ready`.
4. Dispatches ready successors via `send_to_scheduler`.
5. Decrements `processing_count` after all successor dispatch is complete.

This is the fast path because it eliminates a `batch_queue` round-trip and a
context switch to the resolution thread.

### 2. Batch-queue path (slow path)

Condition: any successor of this node is a condition node (`is_condition == true`).

Condition evaluation requires reading predecessor results and calling a
condition function — this cannot safely happen on a Rayon worker thread in the
current design (condition evaluation uses `ResolveCtx` which borrows
`SharedData` fields not visible to workers). The worker sends a `NodeInfo`
token to `batch_queue_tx`. The resolution thread drains it in
`drain_and_process_batch_queue`, then calls `process_batch_resolution` which
runs all four phases including condition evaluation via
`dispatch_condition_successor`.

Condition nodes always take the slow path. This is intentional: condition nodes
are rare relative to compute nodes in production graphs.

---

## The four-phase batch protocol

`process_batch_resolution` (in `resolution_loop.rs`) wraps `process_batch_inner`
(in `batch_resolution.rs`) and the `processing_count` bookkeeping. The four
phases and the mandatory ordering between them:

**Phase 1 — Store result**
For network packets arriving as `Some(CmTypes)`, store the value in
`node_results`. For compute completions arriving as `None`, the result was
already stored by `execute_task` before sending to `batch_queue`. This phase
has no counter writes.

**Phase 2 — Decrement task counters**
Decrement `pending_cond_tasks[slot]` for condition nodes, or
`pending_tasks[slot]` for regular non-initial nodes (SeqCst). This is the
operation that races with `check_slots` completion detection.

**Phase 3 — Dispatch successors**
Call `collect_successors_for_node_into`, then `decrement_and_collect_ready` for
each successor. Any now-ready successor `NodeInfo`s are accumulated in
`batch_sched` and flushed to workers via `dispatch_nodes`. This phase is
protected by `processing_count > 0` (set before the batch loop in Phase 0).

**Phase 4 — Decrement `processing_count`** (outer, after `process_batch_inner` returns)
The `processing_count` decrement happens in `process_batch_resolution` after
`process_batch_inner` returns. This ordering is mandatory.

**Why Phase 4 must come after Phase 3:**

If `processing_count` were decremented before successor dispatch, another
thread running `check_slots` could observe `pending_tasks == 0`,
`pending_cond_tasks == 0`, and `processing_count == 0` simultaneously — and
conclude the slot is done. It would then call `reset_slot_state` (bumping
generation, resetting counters). Meanwhile, Phase 3 would still be calling
`decrease_and_get_ready_into` on the now-reset dependency counters of the next
stream, causing a threshold underflow: the counter would go from its reset
value downward by 1 without the correct initial decrement sequence, preventing
condition tasks from ever spawning. This was Bug #20 in the project history.

The invariant: **`processing_count` must be decremented AFTER all successor
processing for the batch is finished.**

---

## Slot lifecycle and generation counters

A **slot** is a processing lane. At most `MAX_SLOTS` (64) slots can be active
concurrently. Each slot processes exactly one stream at a time. The `slots`
config value is the maximum number of concurrently in-flight streams.

### Generation counter

`SlotData::generation[slot]` is a `u64` atomic that is incremented each time a
slot begins a new stream. Every `NodeInfo` token carries a `gen: u32` field
stamped at dispatch time in `send_to_scheduler`. When a worker or resolution
thread processes a token, it checks `gen == current_generation[slot]`. If they
differ, the token is from the previous stream (a stale task) and is silently
dropped. This prevents stale tasks that lingered in the Rayon queue from
decrementing the new stream's dependency counters.

The generation is bumped at the earliest safe point:
- `Inactive → Active` transition in `assign_stream_to_available_slot`
- `Buffering → Active` transition in `activate_next_slot`

This is done before counter resets in `reset_slot_state` so that old-stream
in-flight tasks see gen mismatch before the counters are cleared.

### Slot state machine

```
Inactive ──────────────────────────────> Active
    ^                                      |
    |   (slot-priority: another           |
    |    slot takes priority)             |
    |                             Buffering
    |                                 |
    |   (activate_next_slot on prior  |
    |    slot completion)             |
    |<--------------------------------+
    |
    +<-- (release_slot after stream completion)
```

In non-slot-priority mode: `Inactive → Active → Inactive`, cycling for each
new stream. `restart_slot_nonnetwork` re-registers the slot in-place without
going through `Inactive`.

In slot-priority mode: streams assigned when the preferred slot is busy go to
`Buffering`. They transition to `Active` only when the preceding slot completes,
via `activate_next_slot` called from `activate_buffered_slot`.

### Completion predicate

A slot is eligible for completion detection when all three of these conditions
hold (SeqCst loads, evaluated in `detect_and_claim_slot_completion`):

```
pending_tasks[slot] == 0
pending_cond_tasks[slot] == 0
processing_count[slot] == 0
```

A CAS via `resolution_state.try_complete_slot(slot)` then claims exclusive
ownership (exactly-once semantics). A double-check re-reads all three counters
after winning the CAS to rule out a stale win from a concurrent reset.

---

## Lock-ordering protocol

Two `parking_lot::RwLock` guards protect shared slot metadata:

- `SlotData::running_streams: RwLock<Vec<(stream_id, slot_id)>>`
- `SlotData::states: RwLock<Vec<SlotState>>`

**Invariant: always acquire `running_streams` before `slot_states`.**

Every function that holds both locks simultaneously must acquire them in this
order:

```
running_streams.write()  // first
slot_states.write()      // second
```

Violating this order causes deadlock when two threads each hold one lock and
wait for the other. This was the root cause of Bugs #11 and #12:

- Bug #11: `release_slot` acquired `slot_states` then `running_streams` (inverted).
- Bug #12: `activate_next_slot` acquired `slot_states` then `running_streams`
  (inverted), while `assign_stream_to_available_slot` used the correct order.

All three functions (`assign_stream_to_available_slot`, `activate_next_slot`,
`release_slot`) now acquire `running_streams` first.

---

## The `check_slots` invariant

`check_slots` (in `slot_lifecycle.rs`) must be called **unconditionally** on
every resolution-loop iteration, even when the batch queue is empty and no new
packets arrived.

The reason: consider the scenario where Thread A processes the last task for
slot 0, decrementing all counters to 0 and then decrementing
`processing_count` to 0 in Phase 4. Meanwhile all other resolution threads
drain an empty `batch_queue`, see no new packets, and — if `check_slots` were
conditional on "something arrived this iteration" — skip the completion check.
Now every thread is idle. No future event will trigger a re-check. The slot
hangs forever.

This was Bug #21. The fix: remove the conditional wrapper around `check_slots`
in the resolution loop. The function has internal fast-paths (`needs_check`
flag, active-bitmap filter) that make the unconditional call cheap on idle
slots.

---

## The `needs_check` fast path

`SlotData::needs_check[slot]` is an `AtomicBool` set to `true` by any
operation that might advance a slot toward completion:

- `process_batch_resolution` Phase 4: after decrementing `processing_count`
- `worker_resolve_successors` Step 7: after decrementing `processing_count`

`check_slots` reads `needs_check[slot].swap(false, AcqRel)`. If it was
`false`, the slot has had no task activity since the last check and the
three SeqCst counter loads are skipped entirely. This avoids loading three
`SeqCst` atomics per slot per iteration when threads are idle.

The unconditional `check_slots` call (Bug #21 fix) and the `needs_check`
fast-path are complementary: the unconditional call ensures liveness; the
flag ensures the idle cost is minimal.

---

## Memory ordering

`ordering.rs` provides two helpers that select between `Acquire`/`AcqRel` and
`SeqCst` based on `RuntimeConfig::single_slot_mode`:

- **`single_slot_mode == true`** (exactly one concurrent stream): pairwise
  `Acquire`/`AcqRel` is sufficient because there is no concurrent
  reinitialisation racing with completion detection.
- **`single_slot_mode == false`** (multiple concurrent slots): `SeqCst` is
  required on all slot-counter reads and writes. With multiple threads, Thread
  A's decrement on slot 0 must be globally visible to Thread C's completion
  check, even without a direct synchronisation edge between them. `SeqCst`
  establishes total order across all threads. This was confirmed after Bugs
  #14, #18, and #19.

The `SeqCst` requirement applies to `generation`, `pending_tasks`,
`pending_cond_tasks`, and `processing_count`. Other atomics (`needs_check`,
`active_bitmap`, `stream_id`) use weaker orderings documented at their
call sites.

---

## Where to put new code

**New scheduling strategy**
Implement the `TaskScheduler` trait in `tomii-core/src/scheduler.rs`, then add
a variant to `SchedulerImpl::Plugin`. Requires the `plugin-scheduler` feature.

**New argument type (e.g. `$foo`)**
Add a `CmTypes` variant in `tomii-types`, handle it in
`arg_resolution.rs::collect_arg_result` and `arg_resolution.rs::collect_res_from_cache`,
and add the JSON parsing in `json_structs.rs`.

**New per-slot state**
Add the field to `shared_data.rs::SlotData`. Initialize it in `mod.rs`
(`TomiiRtBuilder::build`). Add reset logic in
`slot_lifecycle.rs::reset_slot_state` and, if the field is result-bearing, a
reinit call in `slot_management.rs::process_slot_completion` before
`release_slot`.

**New graph-level flag on a node**
Add the field to `graph_struct.rs::Node`. If it needs a fast-path lookup,
mirror it into `node_cache.rs::NodeCacheEntry` and populate it in
`init.rs::build_node_cache`. If it affects dependency routing, update
`init.rs::build_predecessor_tables`.

---

## Key invariants checklist

Do not break these without fully understanding the consequences:

- `processing_count` decremented AFTER successor dispatch (Phase 4 ordering).
  Violation causes threshold underflow in the next stream (Bug #20).

- `check_slots` called unconditionally every resolution-loop iteration.
  Violation causes hangs when all threads simultaneously see an empty queue
  (Bug #21).

- Lock ordering: acquire `running_streams` before `slot_states` in every
  function that holds both. Violation causes deadlock (Bugs #11, #12).

- Generation bumped in `reset_slot_state` BEFORE counter resets. Violation
  creates a window where stale tasks pass the generation filter and decrement
  freshly-reset counters.

- `node_results.reinit_slot(slot)` called BEFORE `release_slot(slot)` in
  `process_slot_completion`. Violation allows a newly-assigned stream to read
  stale results from the previous stream before the buffers are cleared
  (Bug #16).

- `MAX_SLOTS = 64` is load-bearing for the `u64` completion bitmaps
  (`active_bitmap`) and the per-slot generation cache in
  `drain_and_process_batch_queue`. Raising this limit requires widening those
  structures to `u128` or a bit-vector type.

- `SeqCst` on all slot-counter operations in multi-slot mode. Weakening to
  `AcqRel` or `Release` causes non-deterministic counter corruption across
  stream boundaries (Bugs #14, #18, #19).

## Performance envelope

### Intrinsic costs

These costs are structural and cannot be removed without changing the programming
model or the safety invariants above.

**K-way SeqCst on `remaining_deps`.**  Every task arrival does an `AcqRel` fetch-sub
on `remaining_deps` followed by a comparison.  In multi-slot mode the counter
operations must be `SeqCst` (see invariants above).  At W workers and S concurrent
slots this produces W×S SeqCst RMWs competing on the same cache line per node,
which becomes the dominant cost for fan-in nodes with large K.

**Type-erased dispatch (~40–85 ns/call).** Task functions are stored as
`Box<dyn FnOnce()>` (Rayon path) or raw `fn` pointers (inline-continuation path).
The inline-continuation path eliminates the box allocation but retains one indirect
call per task.  Sub-µs workloads where per-task work is comparable to this overhead
will not amortise it.

**Resolution-thread state machine.**  By default a single resolution thread runs
the four-phase batch protocol (batch drain → completion detection → successor
collection → scheduling).  At high S or large fan-out graphs the resolution thread
becomes the bottleneck.  `--system-threads N` raises the resolution-thread count but
introduces cross-thread slot ownership checks at each phase boundary.

**Slot lifecycle overhead.**  Each stream requires one `reinit_slot` (generational
reset of all node buffers, O(nodes)) and one `release_slot` call.  For pipelines
with many nodes and short per-stream work this can dominate.

### Workload classes Tomii targets

- **Streaming MIMO pipelines**: large per-task work (≥10 µs), O(10–100) nodes, 1–64
  concurrent slots, long-running (minutes to hours).  The scheduling abstraction and
  slot lifecycle amortise well over these workloads.
- **Multi-slot fan-out**: pipelines where the same graph topology is applied to many
  independent streams concurrently.  Slot parallelism hides resolution-thread latency.
- **Heterogeneous DAGs with barriers**: mixed compute/network nodes, conditional
  routing via `$barrier`/`$dep`, grouped synchronisation across fan-in nodes.

### Workload classes Tomii does not target

- **Sub-µs micro-tasks**: per-task work below ~1 µs will be dominated by dispatch and
  resolution overhead.  Static-graph executors (Taskflow, TBB flow graph) with
  pre-compiled task graphs have lower per-invocation cost here.
- **Pure `parallel_for` workloads**: homogeneous loops over independent elements are
  better served by Rayon directly or a fork-join runtime without the slot/stream model.
- **Dynamic-topology DAGs**: graphs where the node set or edges change between streams
  are not supported; the graph is compiled once at startup and reused across all slots.
