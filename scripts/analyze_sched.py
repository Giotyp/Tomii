#!/usr/bin/env python3
"""
Enhanced scheduler CSV analyzer with validation and correctness checks.

Improvements over v1:
- Validates record consistency (no overlaps, time ordering)
- Detects data corruption
- Better idle time semantics (accounts for startup)
- Identifies scheduler inefficiencies

Usage:
    python3 scripts/analyze_sched_v2.py schedule_log.csv --system-threads 2
"""
import argparse
import sys
from utils import *


class SchedulerValidator:
    """Validates scheduler records for correctness."""

    def __init__(self):
        self.errors = []
        self.warnings = []

    def validate_worker_records(self, worker_id, records):
        """Validate records for a single worker."""
        if not records:
            return True

        # Sort by start time
        sorted_records = sorted(records, key=lambda r: r[2])

        # Check 1: Time ordering
        for rec in sorted_records:
            slot, job_id, start_ns, end_ns, worker, task_id, index = rec
            if start_ns >= end_ns:
                self.errors.append(
                    f"Worker {worker_id}: Invalid time range job_id={job_id} "
                    f"(start={start_ns} >= end={end_ns})"
                )

        # Check 2: No overlapping tasks
        for i in range(1, len(sorted_records)):
            prev_rec = sorted_records[i - 1]
            curr_rec = sorted_records[i]

            prev_end = prev_rec[3]
            curr_start = curr_rec[2]

            if curr_start < prev_end:
                overlap_ns = prev_end - curr_start
                self.errors.append(
                    f"Worker {worker_id}: Overlapping tasks! "
                    f"job_id={prev_rec[1]} ends at {prev_end}, "
                    f"job_id={curr_rec[1]} starts at {curr_start} "
                    f"(overlap={overlap_ns}ns)"
                )

        # Check 3: Detect suspiciously short tasks (< 100ns - likely measurement error)
        for rec in sorted_records:
            duration = rec[3] - rec[2]
            if duration < 100:
                self.warnings.append(
                    f"Worker {worker_id}: Suspiciously short task job_id={rec[1]} "
                    f"duration={duration}ns (possible timing artifact)"
                )

        return len(self.errors) == 0

    def validate_all(self, workers):
        """Validate all worker records."""
        all_valid = True
        for worker_id, records in workers.items():
            if not self.validate_worker_records(worker_id, records):
                all_valid = False
        return all_valid

    def print_report(self):
        """Print validation report."""
        if not self.errors and not self.warnings:
            print("  Validation passed: No issues detected")
            return True

        if self.errors:
            print(f"  Validation FAILED: {len(self.errors)} error(s) found")
            for err in self.errors[:10]:  # Limit output
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


def calculate_idle_time_v2(
    worker_records, global_start_ns, global_end_ns, gap_threshold_ns=1000000
):
    """
    Enhanced idle time calculation with proper semantics.

    Args:
        worker_records: List of records for this worker
        global_start_ns: Earliest start time across all workers
        global_end_ns: Latest end time across all workers
        gap_threshold_ns: Threshold for classifying gaps as "large idle" vs overhead

    Returns:
        Dictionary with detailed metrics
    """
    if not worker_records:
        # Worker never executed any tasks
        total_span = global_end_ns - global_start_ns
        return {
            "num_tasks": 0,
            "total_span_ns": total_span,
            "busy_time_ns": 0,
            "idle_time_ns": total_span,
            "startup_delay_ns": 0,
            "inter_task_idle_ns": 0,
            "tail_idle_ns": 0,
            "large_gaps": [],
            "scheduling_overhead_ns": 0,
            "num_gaps": 0,
            "max_gap_ns": 0,
            "min_gap_ns": 0,
            "avg_task_duration_ns": 0,
        }

    # Sort by start time
    intervals = [(rec[2], rec[3]) for rec in worker_records]
    intervals.sort(key=lambda x: x[0])

    # Metrics
    num_tasks = len(intervals)
    first_start = intervals[0][0]
    last_end = intervals[-1][1]

    # Busy time = sum of all task durations
    busy_time = sum(end - start for start, end in intervals)

    # Startup delay: time from global start to first task
    startup_delay = first_start - global_start_ns

    # Tail idle: time from last task to global end
    tail_idle = global_end_ns - last_end

    # Inter-task idle and overhead
    inter_task_idle = 0
    scheduling_overhead = 0
    large_gaps = []
    gaps = []

    for i in range(1, len(intervals)):
        prev_end = intervals[i - 1][1]
        curr_start = intervals[i][0]
        gap = curr_start - prev_end

        if gap > 0:
            gaps.append(gap)
            if gap > gap_threshold_ns:
                large_gaps.append((i, gap))
                inter_task_idle += gap
            else:
                scheduling_overhead += gap

    # Total idle time
    total_idle = startup_delay + inter_task_idle + scheduling_overhead + tail_idle

    # Total span (for this worker)
    worker_span = last_end - first_start

    # Global span
    total_span = global_end_ns - global_start_ns

    # Statistics
    num_gaps = len(gaps)
    max_gap = max(gaps) if gaps else 0
    min_gap = min(gaps) if gaps else 0
    avg_task = busy_time / num_tasks if num_tasks > 0 else 0

    return {
        "num_tasks": num_tasks,
        "total_span_ns": total_span,
        "worker_span_ns": worker_span,
        "busy_time_ns": busy_time,
        "idle_time_ns": total_idle,
        "startup_delay_ns": startup_delay,
        "inter_task_idle_ns": inter_task_idle,
        "tail_idle_ns": tail_idle,
        "large_gaps": large_gaps,
        "scheduling_overhead_ns": scheduling_overhead,
        "num_gaps": num_gaps,
        "max_gap_ns": max_gap,
        "min_gap_ns": min_gap,
        "avg_task_duration_ns": avg_task,
    }


def analyze_schedule_v2(csv_path, system_threads=0, units="us", validate=True):
    """
    Enhanced schedule analysis with validation.
    """
    # Determine scale factor
    if units == "us":
        scale = 1e3
        unit_label = "µs"
    elif units == "ms":
        scale = 1e6
        unit_label = "ms"
    elif units == "s":
        scale = 1e9
        unit_label = "s"
    else:
        scale = 1.0
        unit_label = "ns"

    print(f"Analyzing schedule: {csv_path}")
    print(f"System threads: {system_threads}, Units: {unit_label}")
    print("=" * 80)

    # Read data
    records = read_csv(csv_path)
    if not records:
        print("ERROR: No records found")
        return

    # Determine global time span
    global_start_ns = min(rec[2] for rec in records)
    global_end_ns = max(rec[3] for rec in records)
    global_span = global_end_ns - global_start_ns

    # Group by slot
    slots = group_by_slot(records)
    print(f"Total slots: {len(slots)}")

    # Separate workers from system threads
    worker_slots, system_slots = separate_worker_system_slots(slots, system_threads)
    print(f"Worker slots: {sorted(worker_slots.keys())}")
    print(f"System slots: {sorted(system_slots.keys())}")
    print()

    # Combine worker records
    all_worker_records = []
    for slot_records in worker_slots.values():
        all_worker_records.extend(slot_records)

    # Group by worker
    workers = group_by_worker(all_worker_records)
    print(f"Number of workers: {len(workers)}")
    print()

    # Validation
    if validate:
        print("Running validation...")
        validator = SchedulerValidator()
        if not validator.validate_all(workers):
            validator.print_report()
            print("\n   WARNING: Validation failed. Results may be unreliable.")
            print()
        else:
            validator.print_report()
        print()

    # Analyze each worker
    print("=" * 80)
    print("WORKER ANALYSIS")
    print("=" * 80)
    print()

    worker_metrics = {}
    for worker_id in sorted(workers.keys()):
        metrics = calculate_idle_time_v2(
            workers[worker_id], global_start_ns, global_end_ns
        )
        worker_metrics[worker_id] = metrics

        # Display
        m = metrics
        span_scaled = m["total_span_ns"] / scale
        worker_span_scaled = m["worker_span_ns"] / scale
        busy_scaled = m["busy_time_ns"] / scale
        idle_scaled = m["idle_time_ns"] / scale
        startup_scaled = m["startup_delay_ns"] / scale
        inter_idle_scaled = m["inter_task_idle_ns"] / scale
        tail_scaled = m["tail_idle_ns"] / scale
        overhead_scaled = m["scheduling_overhead_ns"] / scale
        avg_task_scaled = m["avg_task_duration_ns"] / scale

        busy_pct = (
            (m["busy_time_ns"] / m["total_span_ns"] * 100)
            if m["total_span_ns"] > 0
            else 0
        )
        idle_pct = (
            (m["idle_time_ns"] / m["total_span_ns"] * 100)
            if m["total_span_ns"] > 0
            else 0
        )

        print(f"Worker {worker_id}:")
        print(f"  Tasks executed:        {m['num_tasks']}")
        print(f"  Global span:           {span_scaled:12.2f} {unit_label}")
        print(f"  Worker active span:    {worker_span_scaled:12.2f} {unit_label}")
        print(
            f"  Busy time:             {busy_scaled:12.2f} {unit_label}  [{busy_pct:6.2f}%]"
        )
        print(f"  Idle time breakdown:")
        print(f"    Startup delay:       {startup_scaled:12.2f} {unit_label}")
        print(
            f"    Inter-task idle:     {inter_idle_scaled:12.2f} {unit_label}  ({len(m['large_gaps'])} large gaps)"
        )
        print(f"    Scheduling overhead: {overhead_scaled:12.2f} {unit_label}")
        print(f"    Tail idle:           {tail_scaled:12.2f} {unit_label}")
        print(
            f"    TOTAL IDLE:          {idle_scaled:12.2f} {unit_label}  [{idle_pct:6.2f}%]"
        )

        if m["num_gaps"] > 0:
            max_gap_scaled = m["max_gap_ns"] / scale
            min_gap_scaled = m["min_gap_ns"] / scale
            print(
                f"  Gap stats: min={min_gap_scaled:.2f} max={max_gap_scaled:.2f} {unit_label}"
            )

        if m["num_tasks"] > 0:
            print(f"  Avg task duration:     {avg_task_scaled:12.2f} {unit_label}")
        print()

    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)

    total_tasks = sum(m["num_tasks"] for m in worker_metrics.values())
    total_busy = sum(m["busy_time_ns"] for m in worker_metrics.values())
    total_idle = sum(m["idle_time_ns"] for m in worker_metrics.values())
    avg_busy = total_busy / len(workers) if workers else 0
    avg_idle = total_idle / len(workers) if workers else 0

    global_span_scaled = global_span / scale
    total_busy_scaled = total_busy / scale
    total_idle_scaled = total_idle / scale
    avg_busy_scaled = avg_busy / scale
    avg_idle_scaled = avg_idle / scale

    avg_busy_pct = (avg_busy / global_span * 100) if global_span > 0 else 0
    avg_idle_pct = (avg_idle / global_span * 100) if global_span > 0 else 0

    # Efficiency metric
    max_possible_work = global_span * len(workers)
    efficiency = (total_busy / max_possible_work * 100) if max_possible_work > 0 else 0

    print(f"Workers:                {len(workers)}")
    print(f"Total tasks executed:   {total_tasks}")
    print(f"Global execution span:  {global_span_scaled:12.2f} {unit_label}")
    print(f"Total busy time (all):  {total_busy_scaled:12.2f} {unit_label}")
    print(f"Total idle time (all):  {total_idle_scaled:12.2f} {unit_label}")
    print(
        f"Avg busy per worker:    {avg_busy_scaled:12.2f} {unit_label}  [{avg_busy_pct:6.2f}%]"
    )
    print(
        f"Avg idle per worker:    {avg_idle_scaled:12.2f} {unit_label}  [{avg_idle_pct:6.2f}%]"
    )
    print(f"Scheduler efficiency:   {efficiency:6.2f}%")
    print()

    # Recommendations
    print("=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)
    if efficiency < 70:
        print("   Low scheduler efficiency (<70%). Possible causes:")
        print("    - Insufficient parallelism in task graph")
        print("    - Task granularity too coarse (long-running tasks)")
        print("    - Load imbalance between workers")
    elif efficiency < 85:
        print("   Moderate efficiency. Room for improvement:")
        print("    - Review task graph for more parallelism")
        print("    - Consider work-stealing scheduler")
    else:
        print("  Good efficiency! Scheduler is well-utilized.")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced scheduler CSV analyzer with validation."
    )
    parser.add_argument("csv", help="Path to CSV file with schedule data")
    parser.add_argument(
        "--system-threads",
        type=int,
        default=0,
        help="Number of system threads (default: auto-detect)",
    )
    parser.add_argument(
        "--units",
        choices=["ns", "us", "ms", "s"],
        default="us",
        help="Time units for display (default: us)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation checks",
    )

    args = parser.parse_args()

    try:
        analyze_schedule_v2(
            args.csv,
            args.system_threads,
            args.units,
            validate=not args.no_validate,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
