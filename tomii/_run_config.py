"""Runtime configuration for the Τομί executor binary."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class RunConfig:
    # ── Graph / execution ────────────────────────────────────────────────────
    workers: int = 1
    core_offset: int = 1
    system_threads: int = 1
    receiver_threads: int = 1
    slots: int = 1
    max_streams: int = 1
    max_runtime: int = 0          # 0 = no limit
    # ── Scheduler ────────────────────────────────────────────────────────────
    fifo: bool = False
    custom: bool = False
    slot_priority: bool = False
    batching_size: int = 1
    batching_limit: int = 10
    coalesce_barriers: bool = False
    inline_continuation: bool = False
    # ── Timing / output ──────────────────────────────────────────────────────
    timing: Optional[str] = None
    record: bool = False
    record_stream: Optional[int] = None
    use_rdtsc: bool = False
    exclude_streams: int = 0
    report: Optional[str] = None
    # ── Tuning knobs ─────────────────────────────────────────────────────────
    batch_queue_capacity: int = 65536
    spin_iterations: int = 32
    sched_flush_threshold: int = 32
    socket_recv_buf_bytes: int = 16 * 1024 * 1024
    recv_pool_size: int = 1024
    spin_wait_spin_iters: int = 64
    spin_wait_yield_iters: int = 256
    spin_wait_park_ns: int = 100

    def to_args(self, json: str, dylib: str) -> List[str]:
        """Convert to CLI argument list for the tomii binary."""
        args = [
            "--json", json,
            "--dylib", dylib,
            "--workers", str(self.workers),
            "--core-offset", str(self.core_offset),
            "--system-threads", str(self.system_threads),
            "--receiver-threads", str(self.receiver_threads),
            "--slots", str(self.slots),
            "--max-streams", str(self.max_streams),
            "--max-runtime", str(self.max_runtime),
            "--batching-size", str(self.batching_size),
            "--batching-limit", str(self.batching_limit),
            "--exclude-streams", str(self.exclude_streams),
            # Tuning knobs
            "--batch-queue-capacity", str(self.batch_queue_capacity),
            "--spin-iterations", str(self.spin_iterations),
            "--sched-flush-threshold", str(self.sched_flush_threshold),
            "--socket-recv-buf-bytes", str(self.socket_recv_buf_bytes),
            "--recv-pool-size", str(self.recv_pool_size),
            "--spin-wait-spin-iters", str(self.spin_wait_spin_iters),
            "--spin-wait-yield-iters", str(self.spin_wait_yield_iters),
            "--spin-wait-park-ns", str(self.spin_wait_park_ns),
        ]
        if self.fifo:
            args.append("--fifo")
        if self.custom:
            args.append("--custom")
        if self.slot_priority:
            args.append("--slot-priority")
        if self.coalesce_barriers:
            args.append("--coalesce-barriers")
        if self.inline_continuation:
            args.append("--inline-continuation")
        if self.record:
            args.append("--record")
        if self.use_rdtsc:
            args.append("--use-rdtsc")
        if self.timing is not None:
            args += ["--timing", self.timing]
        if self.record_stream is not None:
            args += ["--record-stream", str(self.record_stream)]
        if self.report is not None:
            args += ["--report", self.report]
        return args
