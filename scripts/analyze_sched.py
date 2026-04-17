#!/usr/bin/env python3
"""
Scheduler CSV analyzer with validation and correctness checks.

Records produced by Τομί fall into four categories:
  - Worker records:             slot in [0, slots), task_id is a small node ID
  - System records:             slot in [slots, slots+system_threads)
      * resolution  task_id = IDTYPE_MAX
      * preparation task_id = IDTYPE_MAX - 1
      * idle-wait   task_id = IDTYPE_MAX - 2
  - Receiver records:           slot in [slots+system_threads, ...], task_id = 0
  - Scheduling latency records: task_id = IDTYPE_MAX - 3*k (k=node_id, 0<k<1000)
                                start_ns=spawn time, end_ns=exec start time

Scheduling latency records are separated before any analysis so they do not
corrupt busy/idle metrics or trigger false overlap errors.

Usage:
    python3 scripts/analyze_sched.py schedule_log.csv --system-threads 4
    python3 scripts/analyze_sched.py schedule_log.csv --system-threads 4 --slots 10
"""
import argparse
import sys
from collections import defaultdict
from utils import read_csv, group_by_slot, separate_worker_system_slots, group_by_worker


# ============================================================================
# Scheduling latency record detection (mirrors scheduler_visualize.py)
# ============================================================================

def detect_idtype_max(records):
    """Return the estimated IdType::MAX from the maximum task_id in the dataset."""
    return max(r[5] for r in records)


def split_scheduling_latency(records, idtype_max):
    """
    Separate scheduling latency records from execution records.

    Scheduling latency records have task_id = IDTYPE_MAX - 3*k where k is the
    original node id (small positive integer < 1000).  They encode:
        start_ns = spawn timestamp  (when task was submitted to scheduler)
        end_ns   = exec start time  (when a worker actually picked it up)

    Returns (execution_records, scheduling_latency_records).
    """
    execution_recs = []
    latency_recs = []

    for rec in records:
        slot, job_id, start_ns, end_ns, worker, task_id, index = rec
        offset = idtype_max - task_id

        # Scheduling latency: offset = 3*k, 0 < k < 1000 (not 0 itself = resolution)
        if offset > 0 and offset % 3 == 0:
            original_task_id = offset // 3
            if 0 < original_task_id < 1000:
                latency_recs.append({
                    "slot":             slot,
                    "job_id":           job_id,
                    "spawn_ns":         start_ns,
                    "exec_ns":          end_ns,
                    "worker":           worker,
                    "original_task_id": original_task_id,
                    "index":            index,
                })
                continue

        execution_recs.append(rec)

    return execution_recs, latency_recs


# ============================================================================
# Validation
# ============================================================================

class SchedulerValidator:
    """Validates per-worker execution records for correctness."""

    def __init__(self):
        self.errors = []
        self.warnings = []

    def validate_worker_records(self, worker_id, records):
        if not records:
            return True

        sorted_records = sorted(records, key=lambda r: r[2])

        # Check 1: start < end
        for rec in sorted_records:
            slot, job_id, start_ns, end_ns, worker, task_id, index = rec
            if start_ns >= end_ns:
                self.errors.append(
                    f"Worker {worker_id}: Invalid time range job_id={job_id} "
                    f"(start={start_ns} >= end={end_ns})"
                )

        # Check 2: No overlapping tasks on the same worker
        for i in range(1, len(sorted_records)):
            prev = sorted_records[i - 1]
            curr = sorted_records[i]
            if curr[2] < prev[3]:
                overlap_ns = prev[3] - curr[2]
                self.errors.append(
                    f"Worker {worker_id}: Overlapping tasks! "
                    f"job_id={prev[1]} ends at {prev[3]}, "
                    f"job_id={curr[1]} starts at {curr[2]} "
                    f"(overlap={overlap_ns}ns)"
                )

        # Check 3: Suspiciously short tasks (likely measurement error)
        for rec in sorted_records:
            duration = rec[3] - rec[2]
            if duration < 100:
                self.warnings.append(
                    f"Worker {worker_id}: Short task job_id={rec[1]} "
                    f"duration={duration}ns (possible timing artifact)"
                )

        return len(self.errors) == 0

    def validate_all(self, workers):
        all_valid = True
        for worker_id, records in workers.items():
            if not self.validate_worker_records(worker_id, records):
                all_valid = False
        return all_valid

    def print_report(self):
        if not self.errors and not self.warnings:
            print("  Validation passed: No issues detected")
            return True

        if self.errors:
            print(f"  Validation FAILED: {len(self.errors)} error(s) found")
            for err in self.errors[:10]:
                print(f"  ERROR: {err}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")

        if self.warnings:
            print(f"  {len(self.warnings)} warning(s)")
            for warn in self.warnings[:5]:
                print(f"  WARN: {warn}")
            if len(self.warnings) > 5:
                print(f"  ... and {len(self.warnings) - 5} more warnings")

        return len(self.errors) == 0


# ============================================================================
# Idle time calculation
# ============================================================================

def calculate_idle_time(worker_records, global_start_ns, global_end_ns,
                        gap_threshold_ns=1_000_000):
    """
    Compute busy / idle breakdown for a single worker.

    Gaps smaller than gap_threshold_ns are counted as scheduling overhead;
    larger ones are counted as inter-task idle (pipeline stall).
    """
    if not worker_records:
        total_span = global_end_ns - global_start_ns
        return {
            "num_tasks": 0,
            "total_span_ns": total_span,
            "worker_span_ns": 0,
            "busy_time_ns": 0,
            "idle_time_ns": total_span,
            "startup_delay_ns": 0,
            "inter_task_idle_ns": 0,
            "tail_idle_ns": total_span,
            "large_gaps": [],
            "scheduling_overhead_ns": 0,
            "num_gaps": 0,
            "max_gap_ns": 0,
            "min_gap_ns": 0,
            "avg_task_duration_ns": 0,
        }

    intervals = sorted((rec[2], rec[3]) for rec in worker_records)
    num_tasks = len(intervals)
    first_start = intervals[0][0]
    last_end = intervals[-1][1]

    busy_time = sum(end - start for start, end in intervals)
    startup_delay = first_start - global_start_ns
    tail_idle = global_end_ns - last_end

    inter_task_idle = 0
    scheduling_overhead = 0
    large_gaps = []
    gaps = []

    for i in range(1, len(intervals)):
        gap = intervals[i][0] - intervals[i - 1][1]
        if gap > 0:
            gaps.append(gap)
            if gap > gap_threshold_ns:
                large_gaps.append((i, gap))
                inter_task_idle += gap
            else:
                scheduling_overhead += gap

    total_idle = startup_delay + inter_task_idle + scheduling_overhead + tail_idle

    return {
        "num_tasks":              num_tasks,
        "total_span_ns":          global_end_ns - global_start_ns,
        "worker_span_ns":         last_end - first_start,
        "busy_time_ns":           busy_time,
        "idle_time_ns":           total_idle,
        "startup_delay_ns":       startup_delay,
        "inter_task_idle_ns":     inter_task_idle,
        "tail_idle_ns":           tail_idle,
        "large_gaps":             large_gaps,
        "scheduling_overhead_ns": scheduling_overhead,
        "num_gaps":               len(gaps),
        "max_gap_ns":             max(gaps) if gaps else 0,
        "min_gap_ns":             min(gaps) if gaps else 0,
        "avg_task_duration_ns":   busy_time / num_tasks if num_tasks else 0,
    }


# ============================================================================
# Per-type system thread summary
# ============================================================================

def summarize_system_slots(system_slots, idtype_max, global_start_ns, global_end_ns, scale, unit_label):
    """Print a breakdown of system thread time by record type (res/prep/wait)."""
    resolution_task_id  = idtype_max
    preparation_task_id = idtype_max - 1
    wait_task_id        = idtype_max - 2

    type_records = {
        "resolution":  [],
        "preparation": [],
        "idle-wait":   [],
        "other":       [],
    }

    for slot_records in system_slots.values():
        for rec in slot_records:
            tid = rec[5]
            if tid == resolution_task_id:
                type_records["resolution"].append(rec)
            elif tid == preparation_task_id:
                type_records["preparation"].append(rec)
            elif tid == wait_task_id:
                type_records["idle-wait"].append(rec)
            else:
                type_records["other"].append(rec)

    total_span = global_end_ns - global_start_ns

    print("System thread activity breakdown:")
    for type_name, recs in type_records.items():
        if not recs:
            continue
        busy_ns = sum(r[3] - r[2] for r in recs)
        count   = len(recs)
        pct     = busy_ns / total_span * 100 if total_span else 0
        print(f"  {type_name:12s}: {count:6d} records  "
              f"{busy_ns / scale:12.2f} {unit_label}  [{pct:6.2f}% of global span]")


# ============================================================================
# Scheduling latency statistics
# ============================================================================

def print_latency_stats(latency_recs, scale, unit_label):
    """Print per-task scheduling latency statistics."""
    if not latency_recs:
        return

    print(f"Total scheduling latency records: {len(latency_recs)}")
    print()

    by_task = defaultdict(list)
    for rec in latency_recs:
        by_task[rec["original_task_id"]].append(rec["exec_ns"] - rec["spawn_ns"])

    for task_id in sorted(by_task.keys()):
        lats = sorted(by_task[task_id])
        n = len(lats)
        avg = sum(lats) / n
        median = lats[n // 2]
        p95 = lats[int(n * 0.95)]
        p99 = lats[int(n * 0.99)]
        print(f"  Task {task_id}: n={n}  "
              f"avg={avg/scale:.2f}  median={median/scale:.2f}  "
              f"p95={p95/scale:.2f}  p99={p99/scale:.2f}  "
              f"min={lats[0]/scale:.2f}  max={lats[-1]/scale:.2f}  {unit_label}")


# ============================================================================
# Main analysis
# ============================================================================

def analyze_schedule(csv_path, system_threads=0, worker_slots_count=0,
                     units="us", validate=True):
    scale, unit_label = {
        "us": (1e3,  "µs"),
        "ms": (1e6,  "ms"),
        "s":  (1e9,  "s"),
    }.get(units, (1.0, "ns"))

    print(f"Analyzing schedule: {csv_path}")
    print(f"System threads: {system_threads}  Worker slots: {worker_slots_count or 'auto'}  Units: {unit_label}")
    print("=" * 80)

    records = read_csv(csv_path)
    if not records:
        print("ERROR: No records found")
        return

    # ---- Detect IdType::MAX and separate scheduling latency records ----------
    idtype_max = detect_idtype_max(records)
    execution_recs, latency_recs = split_scheduling_latency(records, idtype_max)

    print(f"Total records:              {len(records)}")
    print(f"  Execution records:        {len(execution_recs)}")
    print(f"  Scheduling latency recs:  {len(latency_recs)}")
    print(f"  Estimated IdType::MAX:    {idtype_max}")
    print()

    # ---- Slot classification ------------------------------------------------
    slots = group_by_slot(execution_recs)
    worker_slots, system_slots, receiver_slots = separate_worker_system_slots(
        slots, system_threads, worker_slots_count
    )

    print(f"Slot classification:")
    print(f"  Worker slots:   {sorted(worker_slots.keys())}")
    print(f"  System slots:   {sorted(system_slots.keys())}")
    print(f"  Receiver slots: {sorted(receiver_slots.keys())}")
    print()

    # ---- Global time span (from worker records only) ------------------------
    all_worker_recs = [r for recs in worker_slots.values() for r in recs]
    all_system_recs = [r for recs in system_slots.values() for r in recs]
    all_receiver_recs = [r for recs in receiver_slots.values() for r in recs]

    # Time zero: earliest receiver record if present, else earliest worker record
    if all_receiver_recs:
        global_start_ns = min(r[2] for r in all_receiver_recs)
    elif all_worker_recs:
        global_start_ns = min(r[2] for r in all_worker_recs)
    elif all_system_recs:
        global_start_ns = min(r[2] for r in all_system_recs)
    else:
        print("ERROR: No usable records found after classification")
        return

    global_end_ns = max(
        (max(r[3] for r in recs) for recs in [all_worker_recs, all_system_recs, all_receiver_recs] if recs),
        default=global_start_ns,
    )
    global_span_ns = global_end_ns - global_start_ns

    print(f"Global span: {global_span_ns / scale:.2f} {unit_label}  "
          f"(start={global_start_ns}ns  end={global_end_ns}ns)")
    print()

    # ---- Validation (worker execution records only) -------------------------
    if validate:
        print("Running validation (worker execution records only)...")
        workers = group_by_worker(all_worker_recs)
        validator = SchedulerValidator()
        validator.validate_all(workers)
        validator.print_report()
        print()
    else:
        workers = group_by_worker(all_worker_recs)

    # ---- Worker analysis ----------------------------------------------------
    print("=" * 80)
    print("WORKER ANALYSIS")
    print("=" * 80)
    print()

    worker_metrics = {}
    for worker_id in sorted(workers.keys()):
        m = calculate_idle_time(workers[worker_id], global_start_ns, global_end_ns)
        worker_metrics[worker_id] = m

        busy_pct = m["busy_time_ns"] / m["total_span_ns"] * 100 if m["total_span_ns"] else 0
        idle_pct = m["idle_time_ns"] / m["total_span_ns"] * 100 if m["total_span_ns"] else 0

        print(f"Worker {worker_id}:")
        print(f"  Tasks executed:        {m['num_tasks']}")
        print(f"  Global span:           {m['total_span_ns']     / scale:12.2f} {unit_label}")
        print(f"  Worker active span:    {m['worker_span_ns']    / scale:12.2f} {unit_label}")
        print(f"  Busy time:             {m['busy_time_ns']      / scale:12.2f} {unit_label}  [{busy_pct:6.2f}%]")
        print(f"  Idle time breakdown:")
        print(f"    Startup delay:       {m['startup_delay_ns']       / scale:12.2f} {unit_label}")
        print(f"    Inter-task idle:     {m['inter_task_idle_ns']     / scale:12.2f} {unit_label}"
              f"  ({len(m['large_gaps'])} large gaps >1ms)")
        print(f"    Scheduling overhead: {m['scheduling_overhead_ns'] / scale:12.2f} {unit_label}")
        print(f"    Tail idle:           {m['tail_idle_ns']           / scale:12.2f} {unit_label}")
        print(f"    TOTAL IDLE:          {m['idle_time_ns']           / scale:12.2f} {unit_label}  [{idle_pct:6.2f}%]")
        if m["num_gaps"] > 0:
            print(f"  Gap stats: min={m['min_gap_ns']/scale:.2f}  max={m['max_gap_ns']/scale:.2f} {unit_label}")
        if m["num_tasks"] > 0:
            print(f"  Avg task duration:     {m['avg_task_duration_ns'] / scale:12.2f} {unit_label}")
        print()

    # ---- System thread analysis ---------------------------------------------
    if system_slots:
        print("=" * 80)
        print("SYSTEM THREAD ANALYSIS")
        print("=" * 80)
        print()
        summarize_system_slots(system_slots, idtype_max, global_start_ns, global_end_ns, scale, unit_label)
        print()

    # ---- Receiver thread analysis -------------------------------------------
    if receiver_slots:
        print("=" * 80)
        print("RECEIVER THREAD ANALYSIS")
        print("=" * 80)
        print()
        rcv_workers = group_by_worker(all_receiver_recs)
        for worker_id in sorted(rcv_workers.keys()):
            m = calculate_idle_time(rcv_workers[worker_id], global_start_ns, global_end_ns)
            busy_pct = m["busy_time_ns"] / m["total_span_ns"] * 100 if m["total_span_ns"] else 0
            print(f"  Receiver worker {worker_id}: {m['num_tasks']} records  "
                  f"busy={m['busy_time_ns']/scale:.2f} {unit_label}  [{busy_pct:.2f}%]")
        print()

    # ---- Scheduling latency -------------------------------------------------
    if latency_recs:
        print("=" * 80)
        print("SCHEDULING LATENCY")
        print("=" * 80)
        print()
        print_latency_stats(latency_recs, scale, unit_label)
        print()

    # ---- Summary ------------------------------------------------------------
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total_tasks   = sum(m["num_tasks"]     for m in worker_metrics.values())
    total_busy_ns = sum(m["busy_time_ns"]  for m in worker_metrics.values())
    total_idle_ns = sum(m["idle_time_ns"]  for m in worker_metrics.values())
    n_workers     = len(worker_metrics)

    avg_busy_ns = total_busy_ns / n_workers if n_workers else 0
    avg_idle_ns = total_idle_ns / n_workers if n_workers else 0
    avg_busy_pct = avg_busy_ns / global_span_ns * 100 if global_span_ns else 0
    avg_idle_pct = avg_idle_ns / global_span_ns * 100 if global_span_ns else 0

    max_possible_work = global_span_ns * n_workers
    efficiency = total_busy_ns / max_possible_work * 100 if max_possible_work else 0

    print(f"Workers:                {n_workers}")
    print(f"Total tasks executed:   {total_tasks}")
    print(f"Global execution span:  {global_span_ns     / scale:12.2f} {unit_label}")
    print(f"Total busy (all):       {total_busy_ns      / scale:12.2f} {unit_label}")
    print(f"Total idle (all):       {total_idle_ns      / scale:12.2f} {unit_label}")
    print(f"Avg busy per worker:    {avg_busy_ns        / scale:12.2f} {unit_label}  [{avg_busy_pct:6.2f}%]")
    print(f"Avg idle per worker:    {avg_idle_ns        / scale:12.2f} {unit_label}  [{avg_idle_pct:6.2f}%]")
    print(f"Scheduler efficiency:   {efficiency:6.2f}%")
    print()

    # ---- Recommendations ----------------------------------------------------
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    if efficiency < 70:
        print("  Low efficiency (<70%). Possible causes:")
        print("   - Insufficient parallelism in task graph")
        print("   - Task granularity too coarse")
        print("   - Load imbalance between workers")
    elif efficiency < 85:
        print("  Moderate efficiency. Room for improvement:")
        print("   - Review task graph for more parallelism")
        print("   - Consider work-stealing scheduler tuning")
    else:
        print("  Good efficiency! Scheduler is well-utilized.")

    # Flag large inter-task gaps
    workers_with_large_gaps = [
        wid for wid, m in worker_metrics.items() if m["large_gaps"]
    ]
    if workers_with_large_gaps:
        print(f"  Workers with large idle gaps (>1ms): {workers_with_large_gaps}")
        print("   - Suggests pipeline stalls or dependency bottlenecks")

    if latency_recs:
        all_lats = [r["exec_ns"] - r["spawn_ns"] for r in latency_recs]
        p99 = sorted(all_lats)[int(len(all_lats) * 0.99)]
        if p99 / scale > 100:  # >100 units (us=100µs, ms=100ms)
            print(f"  High P99 scheduling latency: {p99/scale:.2f} {unit_label}")
            print("   - Consider tuning batch_timeout_us or batch_size")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Τομί scheduler CSV analyzer with per-type breakdown."
    )
    parser.add_argument("csv", help="Path to CSV file with schedule data")
    parser.add_argument(
        "--system-threads", type=int, default=0,
        help="Number of system threads (default: auto-detect)",
    )
    parser.add_argument(
        "--slots", type=int, default=0,
        help="Number of worker slots (default: auto-detect)",
    )
    parser.add_argument(
        "--units", choices=["ns", "us", "ms", "s"], default="us",
        help="Time units for display (default: us)",
    )
    parser.add_argument(
        "--no-validate", action="store_true",
        help="Skip validation checks",
    )

    args = parser.parse_args()

    try:
        analyze_schedule(
            args.csv,
            system_threads=args.system_threads,
            worker_slots_count=args.slots,
            units=args.units,
            validate=not args.no_validate,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
