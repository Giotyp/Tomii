#!/usr/bin/env python3
"""
Analyze scheduler CSV produced by SynStream to calculate idle time per worker.

Usage:
    python3 scripts/analyze_sched.py schedule_log.csv --system-threads 2

The CSV must have columns: slot,job_id,start_ns,end_ns,worker,task_id,index

Args:
    csv: Path to CSV file.
    --system-threads: Number of system threads (default: 0).
"""
import argparse
from utils import *


def calculate_idle_time(worker_records, gap_threshold_ns=1000000):
    """
    Calculate idle time for a worker.
    
    Idle time is the sum of gaps between consecutive task executions.
    
    Args:
        worker_records: List of records (slot, job_id, start_ns, end_ns, worker, task_id, index)
        gap_threshold_ns: Threshold in nanoseconds for "large" gaps (default: 1ms)
        
    Returns:
        Tuple of (total_idle_time_ns, total_busy_time_ns, total_span_ns, large_gaps, small_gaps_time)
    """
    if not worker_records:
        return 0, 0, 0, [], 0
    
    # Extract (start_ns, end_ns) tuples and sort by start time
    intervals = [(rec[2], rec[3]) for rec in worker_records]
    intervals.sort(key=lambda x: x[0])
    
    # Calculate total busy time
    total_busy_time = sum(end - start for start, end in intervals)
    
    # Calculate idle time as gaps between consecutive tasks
    total_idle_time = 0
    large_gaps = []  # List of (task_index, gap_ns) for gaps > threshold
    small_gaps_time = 0  # Total time in small gaps (overhead)
    
    for i in range(1, len(intervals)):
        prev_end = intervals[i-1][1]
        curr_start = intervals[i][0]
        gap = curr_start - prev_end
        if gap > 0:
            total_idle_time += gap
            if gap > gap_threshold_ns:
                large_gaps.append((i, gap))
            else:
                small_gaps_time += gap
    
    # Total time span from first start to last end
    first_start = intervals[0][0]
    last_end = intervals[-1][1]
    total_span = last_end - first_start
    
    return total_idle_time, total_busy_time, total_span, large_gaps, small_gaps_time


def analyze_schedule(csv_path, system_threads=0, units="us"):
    """
    Analyze schedule CSV and calculate idle time statistics.
    
    Args:
        csv_path: Path to the CSV file
        system_threads: Number of system threads to separate from worker threads
        units: Time units for display (ns, us, ms, s)
    """
    # Determine scale factor based on units
    if units == "us":
        scale = 1e3
        unit_label = "µs"
    elif units == "ms":
        scale = 1e6
        unit_label = "ms"
    elif units == "s":
        scale = 1e9
        unit_label = "s"
    else:  # ns
        scale = 1.0
        unit_label = "ns"
    
    print(f"Analyzing schedule from: {csv_path}")
    print(f"System threads: {system_threads}")
    print(f"Time units: {unit_label}")
    print()
    
    # Read and preprocess data
    records = read_csv(csv_path)
    if not records:
        print("No records found in CSV file.")
        return
    
    # Group by slot
    slots = group_by_slot(records)
    print(f"Found {len(slots)} slots")
    
    # Separate worker and system slots
    worker_slots, system_slots = separate_worker_system_slots(slots, system_threads)
    print(f"Worker slots: {sorted(worker_slots.keys())}")
    print(f"System slots: {sorted(system_slots.keys())}")
    print()
    
    # Combine all worker records
    all_worker_records = []
    for slot_records in worker_slots.values():
        all_worker_records.extend(slot_records)
    
    # Group by worker
    workers = group_by_worker(all_worker_records)
    print(f"Found {len(workers)} distinct workers")
    print()
    
    # Calculate and display statistics for each worker
    print("=" * 80)
    print("WORKER IDLE TIME ANALYSIS")
    print("=" * 80)
    print()
    
    total_idle_all = 0
    total_busy_all = 0
    total_span_all = 0
    total_large_gap_time = 0
    total_small_gap_time = 0
    
    for worker_id in sorted(workers.keys()):
        worker_records = workers[worker_id]
        idle_time, busy_time, span, large_gaps, small_gaps_time = calculate_idle_time(worker_records)
        
        large_gap_time = sum(gap for _, gap in large_gaps)
        
        total_idle_all += idle_time
        total_busy_all += busy_time
        total_span_all = max(total_span_all, span)
        total_large_gap_time += large_gap_time
        total_small_gap_time += small_gaps_time
        
        num_tasks = len(worker_records)
        idle_percent = (idle_time / span * 100) if span > 0 else 0
        busy_percent = (busy_time / span * 100) if span > 0 else 0
        large_gap_percent = (large_gap_time / span * 100) if span > 0 else 0
        small_gap_percent = (small_gaps_time / span * 100) if span > 0 else 0
        
        # Scale values
        span_scaled = span / scale
        busy_scaled = busy_time / scale
        idle_scaled = idle_time / scale
        large_gap_scaled = large_gap_time / scale
        small_gap_scaled = small_gaps_time / scale
        
        print(f"Worker {worker_id}:")
        print(f"  Tasks executed:    {num_tasks}")
        print(f"  Total span:        {span_scaled:15.2f} {unit_label}  [100.00%]")
        print(f"  Busy time:         {busy_scaled:15.2f} {unit_label}  [{busy_percent:6.2f}%]")
        print(f"  Idle time:         {idle_scaled:15.2f} {unit_label}  [{idle_percent:6.2f}%]")
        if large_gaps:
            print(f"    Large gaps (>1ms): {len(large_gaps):3d} gaps, {large_gap_scaled:10.2f} {unit_label}  [{large_gap_percent:6.2f}%]")
        print(f"    Scheduling overhead:              {small_gap_scaled:10.2f} {unit_label}  [{small_gap_percent:6.2f}%]")
        print()
    
    # Print summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    avg_idle = total_idle_all / len(workers) if workers else 0
    avg_busy = total_busy_all / len(workers) if workers else 0
    avg_idle_percent = (avg_idle / total_span_all * 100) if total_span_all > 0 else 0
    avg_busy_percent = (avg_busy / total_span_all * 100) if total_span_all > 0 else 0
    large_gap_percent = (total_large_gap_time / total_span_all * 100) if total_span_all > 0 else 0
    small_gap_percent = (total_small_gap_time / total_span_all * 100) if total_span_all > 0 else 0
    
    # Scale values
    span_scaled = total_span_all / scale
    busy_total_scaled = total_busy_all / scale
    idle_total_scaled = total_idle_all / scale
    avg_busy_scaled = avg_busy / scale
    avg_idle_scaled = avg_idle / scale
    large_gap_scaled = total_large_gap_time / scale
    small_gap_scaled = total_small_gap_time / scale
    
    print(f"Number of workers:                    {len(workers)}")
    print(f"Total execution span:                 {span_scaled:15.2f} {unit_label}")
    print(f"Total busy time (all):                {busy_total_scaled:15.2f} {unit_label}")
    print(f"Total idle time (all):                {idle_total_scaled:15.2f} {unit_label}")
    print(f"  Large gaps (>1ms):                  {large_gap_scaled:15.2f} {unit_label}  [{large_gap_percent:6.2f}%]")
    print(f"  Scheduling overhead:                {small_gap_scaled:15.2f} {unit_label}  [{small_gap_percent:6.2f}%]")
    print(f"Average busy time per worker:         {avg_busy_scaled:15.2f} {unit_label}  [{avg_busy_percent:6.2f}%]")
    print(f"Average idle time per worker:         {avg_idle_scaled:15.2f} {unit_label}  [{avg_idle_percent:6.2f}%]")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze scheduler CSV to calculate worker idle time."
    )
    parser.add_argument(
        "csv",
        help="Path to CSV file with schedule data"
    )
    parser.add_argument(
        "--system-threads",
        type=int,
        default=0,
        help="Number of system threads (default: 0)"
    )
    parser.add_argument(
        "--units",
        choices=["ns", "us", "ms", "s"],
        default="us",
        help="Time units for display (default: us)"
    )
    
    args = parser.parse_args()
    
    analyze_schedule(args.csv, args.system_threads, args.units)


if __name__ == "__main__":
    main()
