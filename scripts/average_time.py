#!/usr/bin/env python3
"""
Average timing metrics from multiple timing files produced by Τομί.

Usage:
    python3 scripts/average_time.py timing_1s_1b_1.txt timing_1s_1b_2.txt -o timing_1s_1b_avg.txt
    python3 scripts/average_time.py timings/timing_2s_1b_* -o timings/timing_2s_1b_avg.txt

The input files must have the same format and configuration (same number of
slots, workers, tasks, etc.). This script will parse all files, extract numeric
values, average them, and produce a new file in the same format.

For CSV files, it will use analyze_sched.py logic to calculate worker idle/busy
time metrics and average them, writing to a separate _sched.txt file.

Args:
    files: List of input timing files to average (.txt and .csv)
    -o, --output: Output file path (default: timing_avg.txt)
    --system-threads: Number of system threads for CSV analysis (default: auto-detect)
    --units: Time units for CSV analysis display (ns, us, ms, s) (default: us)
"""
import argparse
import os
from utils import *
from timingfile import TimingFile
from typing import List, Dict, Any
from analyze_sched import calculate_idle_time


def auto_convert_unit(value, unit):
    """
    Automatically convert value to a more appropriate unit if value >= 1000.
    
    Args:
        value: Numeric value
        unit: Current unit (ns, µs, ms, s)
    
    Returns:
        Tuple of (converted_value, new_unit)
    """
    if unit == 'ns' and value >= 1000:
        return value / 1000, 'µs'
    elif unit == 'µs' and value >= 1000:
        return value / 1000, 'ms'
    elif unit == 'ms' and value >= 1000:
        return value / 1000, 's'
    else:
        return value, unit


def analyze_csv_file(csv_path: str, system_threads: int = 0) -> Dict[str, Any]:
    """
    Analyze a CSV schedule file and return worker metrics.
    
    Args:
        csv_path: Path to the CSV file
        system_threads: Number of system threads to separate from worker threads
    
    Returns:
        Dictionary with worker metrics:
        {
            'workers': {worker_id: {'idle_time': ns, 'busy_time': ns, 'span': ns, 'num_tasks': int}},
            'total_span': ns
        }
    """
    # Read and preprocess data
    records = read_csv(csv_path)
    if not records:
        return None
    
    # Group by slot
    slots = group_by_slot(records)
    
    # Separate worker and system slots
    worker_slots, system_slots = separate_worker_system_slots(slots, system_threads)
    
    # Combine all worker records
    all_worker_records = []
    for slot_records in worker_slots.values():
        all_worker_records.extend(slot_records)
    
    # Group by worker
    workers = group_by_worker(all_worker_records)
    
    # Calculate statistics for each worker
    worker_metrics = {}
    total_span = 0
    
    for worker_id in sorted(workers.keys()):
        worker_records = workers[worker_id]
        idle_time, busy_time, span, large_gaps, small_gaps_time = calculate_idle_time(worker_records)
        
        worker_metrics[worker_id] = {
            'idle_time': idle_time,
            'busy_time': busy_time,
            'span': span,
            'num_tasks': len(worker_records),
            'large_gaps': large_gaps,
            'small_gaps_time': small_gaps_time
        }
        total_span = max(total_span, span)
    
    return {
        'workers': worker_metrics,
        'total_span': total_span,
        'num_workers': len(workers)
    }


def average_csv_analyses(csv_files: List[str], system_threads: int = 0) -> Dict[str, Any]:
    """
    Analyze multiple CSV files and average their worker metrics.
    
    Args:
        csv_files: List of CSV file paths
        system_threads: Number of system threads
    
    Returns:
        Averaged metrics dictionary
    """
    analyses = []
    for csv_file in csv_files:
        analysis = analyze_csv_file(csv_file, system_threads)
        if analysis:
            analyses.append(analysis)
    
    if not analyses:
        return None
    
    n = len(analyses)
    
    # Get worker IDs from first analysis
    template = analyses[0]
    worker_ids = sorted(template['workers'].keys())
    
    # Average metrics for each worker
    avg_metrics = {
        'workers': {},
        'total_span': sum(a['total_span'] for a in analyses) / n,
        'num_workers': template['num_workers']
    }
    
    for worker_id in worker_ids:
        idle_times = [a['workers'][worker_id]['idle_time'] for a in analyses]
        busy_times = [a['workers'][worker_id]['busy_time'] for a in analyses]
        spans = [a['workers'][worker_id]['span'] for a in analyses]
        num_tasks = [a['workers'][worker_id]['num_tasks'] for a in analyses]
        
        avg_metrics['workers'][worker_id] = {
            'idle_time': sum(idle_times) / n,
            'busy_time': sum(busy_times) / n,
            'span': sum(spans) / n,
            'num_tasks': int(sum(num_tasks) / n)
        }
    
    return avg_metrics


def write_csv_analysis(metrics: Dict[str, Any], output_path: str, units: str = "us"):
    """
    Write CSV analysis results to a file.
    
    Args:
        metrics: Averaged metrics dictionary
        output_path: Output file path
        units: Time units for display
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
    else:  # ns
        scale = 1.0
        unit_label = "ns"
    
    with open(output_path, 'w') as f:
        f.write("Schedule Analysis - Averaged Worker Metrics\n")
        f.write("=" * 80 + "\n")
        f.write(f"Number of workers: {metrics['num_workers']}\n")
        f.write(f"Time units: {unit_label}\n")
        f.write("\n")
        
        f.write("Per-Worker Metrics:\n")
        f.write("-" * 80 + "\n")
        
        total_idle_all = 0
        total_busy_all = 0
        
        for worker_id in sorted(metrics['workers'].keys()):
            w = metrics['workers'][worker_id]
            
            idle_time = w['idle_time']
            busy_time = w['busy_time']
            span = w['span']
            num_tasks = w['num_tasks']
            
            total_idle_all += idle_time
            total_busy_all += busy_time
            
            idle_percent = (idle_time / span * 100) if span > 0 else 0
            busy_percent = (busy_time / span * 100) if span > 0 else 0
            
            # Scale values
            span_scaled = span / scale
            busy_scaled = busy_time / scale
            idle_scaled = idle_time / scale
            
            f.write(f"Worker {worker_id}:\n")
            f.write(f"  Tasks executed:    {num_tasks}\n")
            f.write(f"  Total span:        {span_scaled:15.2f} {unit_label}  [100.00%]\n")
            f.write(f"  Busy time:         {busy_scaled:15.2f} {unit_label}  [{busy_percent:6.2f}%]\n")
            f.write(f"  Idle time:         {idle_scaled:15.2f} {unit_label}  [{idle_percent:6.2f}%]\n")
            f.write("\n")
        
        # Write summary
        f.write("=" * 80 + "\n")
        f.write("SUMMARY\n")
        f.write("=" * 80 + "\n")
        
        total_span_all = metrics['total_span']
        avg_idle = total_idle_all / metrics['num_workers'] if metrics['num_workers'] > 0 else 0
        avg_busy = total_busy_all / metrics['num_workers'] if metrics['num_workers'] > 0 else 0
        avg_idle_percent = (avg_idle / total_span_all * 100) if total_span_all > 0 else 0
        avg_busy_percent = (avg_busy / total_span_all * 100) if total_span_all > 0 else 0
        
        # Scale values
        span_scaled = total_span_all / scale
        busy_total_scaled = total_busy_all / scale
        idle_total_scaled = total_idle_all / scale
        avg_busy_scaled = avg_busy / scale
        avg_idle_scaled = avg_idle / scale
        
        f.write(f"Number of workers:           {metrics['num_workers']}\n")
        f.write(f"Total execution span:        {span_scaled:15.2f} {unit_label}\n")
        f.write(f"Total busy time (all):       {busy_total_scaled:15.2f} {unit_label}\n")
        f.write(f"Total idle time (all):       {idle_total_scaled:15.2f} {unit_label}\n")
        f.write(f"Average busy time per worker:{avg_busy_scaled:15.2f} {unit_label}  [{avg_busy_percent:6.2f}%]\n")
        f.write(f"Average idle time per worker:{avg_idle_scaled:15.2f} {unit_label}  [{avg_idle_percent:6.2f}%]\n")


def average_timing_files(files: List[str]) -> TimingFile:
    """
    Average timing metrics from multiple files.
    
    Args:
        files: List of file paths to average
    
    Returns:
        TimingFile with averaged values
    """
    parsed_files = []
    for filepath in files:
        if not filepath.endswith(".txt"):
            continue
        tf = TimingFile()
        tf.parse(filepath)
        parsed_files.append(tf)
    
    if not parsed_files:
        return None
    
    n = len(parsed_files)
    avg_file = TimingFile()
    
    # Use first file as template
    template = parsed_files[0]
    avg_file.filename = "averaged"
    avg_file.total_slots = template.total_slots
    avg_file.timing_method = template.timing_method
    avg_file.worker_slots_range = template.worker_slots_range
    avg_file.system_slots_range = template.system_slots_range
    avg_file.total_streams = template.total_streams
    avg_file.streams_per_slot = template.streams_per_slot.copy()
    
    # Average aggregated statistics with unit conversion
    total_runtime_ns = [convert_to_ns(f.total_runtime[0], f.total_runtime[1]) for f in parsed_files]
    avg_runtime_ns = sum(total_runtime_ns) / n
    avg_file.total_runtime = (convert_from_ns(avg_runtime_ns, template.total_runtime[1]), template.total_runtime[1])
    
    avg_time_ns = [convert_to_ns(f.avg_time_per_stream[0], f.avg_time_per_stream[1]) for f in parsed_files]
    avg_avg_ns = sum(avg_time_ns) / n
    avg_file.avg_time_per_stream = (convert_from_ns(avg_avg_ns, template.avg_time_per_stream[1]), template.avg_time_per_stream[1])
    
    min_ns = [convert_to_ns(f.min_max_per_stream[0][0], f.min_max_per_stream[0][1]) for f in parsed_files]
    max_ns = [convert_to_ns(f.min_max_per_stream[1][0], f.min_max_per_stream[1][1]) for f in parsed_files]
    avg_min_ns = sum(min_ns) / n
    avg_max_ns = sum(max_ns) / n
    avg_file.min_max_per_stream = (
        (convert_from_ns(avg_min_ns, template.min_max_per_stream[0][1]), template.min_max_per_stream[0][1]),
        (convert_from_ns(avg_max_ns, template.min_max_per_stream[1][1]), template.min_max_per_stream[1])
    )
    
    compute_time_ns = [convert_to_ns(f.total_compute_time[0], f.total_compute_time[1]) for f in parsed_files]
    avg_compute_ns = sum(compute_time_ns) / n
    avg_file.total_compute_time = (convert_from_ns(avg_compute_ns, template.total_compute_time[1]), template.total_compute_time[1])
    
    avg_compute_stream_ns = [convert_to_ns(f.avg_compute_time_per_stream[0], f.avg_compute_time_per_stream[1]) for f in parsed_files]
    avg_avg_compute_ns = sum(avg_compute_stream_ns) / n
    avg_file.avg_compute_time_per_stream = (convert_from_ns(avg_avg_compute_ns, template.avg_compute_time_per_stream[1]), template.avg_compute_time_per_stream[1])
    
    # Average per-task analysis
    for task_idx in range(len(template.tasks)):
        avg_task = {
            'name': template.tasks[task_idx]['name'],
            'workers': template.tasks[task_idx]['workers'],
            'executions': template.tasks[task_idx]['executions'],
            'timing': {},
            'worker_summary': {}
        }
        
        # Average timing metrics with unit conversion
        for metric in ['avg_stream', 'avg_task', 'min', 'max', 'total']:
            if metric in template.tasks[task_idx]['timing']:
                # Convert to nanoseconds before averaging
                values_ns = [convert_to_ns(
                    f.tasks[task_idx]['timing'][metric][0],
                    f.tasks[task_idx]['timing'][metric][1]
                ) for f in parsed_files]
                avg_ns = sum(values_ns) / n
                
                # Convert back to original unit
                unit = template.tasks[task_idx]['timing'][metric][1]
                avg_task['timing'][metric] = (convert_from_ns(avg_ns, unit), unit)
        
        # Average worker summaries with unit conversion
        # Collect all unique worker IDs across all files for this task
        all_worker_ids = set()
        for f in parsed_files:
            all_worker_ids.update(f.tasks[task_idx]['worker_summary'].keys())
        
        for worker_id in sorted(all_worker_ids):
            # Only include files that have this worker_id
            counts = []
            percents = []
            times_ns = []
            unit = None
            
            for f in parsed_files:
                if worker_id in f.tasks[task_idx]['worker_summary']:
                    ws = f.tasks[task_idx]['worker_summary'][worker_id]
                    counts.append(ws['count'])
                    percents.append(ws['percent'])
                    times_ns.append(convert_to_ns(ws['time'][0], ws['time'][1]))
                    if unit is None:
                        unit = ws['time'][1]
            
            if counts:  # Only add if at least one file has this worker
                avg_time_ns = sum(times_ns) / len(times_ns)
                
                avg_task['worker_summary'][worker_id] = {
                    'count': int(sum(counts) / len(counts)),
                    'percent': sum(percents) / len(percents),
                    'time': (convert_from_ns(avg_time_ns, unit), unit)
                }
        
        avg_file.tasks.append(avg_task)
    
    # Average system thread tasks
    for sys_idx in range(len(template.system_threads)):
        avg_sys = {
            'thread_id': template.system_threads[sys_idx]['thread_id'],
            'slot': template.system_threads[sys_idx]['slot'],
            'tasks': []
        }
        
        for task_idx in range(len(template.system_threads[sys_idx]['tasks'])):
            template_task = template.system_threads[sys_idx]['tasks'][task_idx]
            
            # Convert all values to nanoseconds before averaging
            execs = [f.system_threads[sys_idx]['tasks'][task_idx]['executions'] for f in parsed_files]
            avgs_ns = [convert_to_ns(
                f.system_threads[sys_idx]['tasks'][task_idx]['avg'][0],
                f.system_threads[sys_idx]['tasks'][task_idx]['avg'][1]
            ) for f in parsed_files]
            mins_ns = [convert_to_ns(
                f.system_threads[sys_idx]['tasks'][task_idx]['min'][0],
                f.system_threads[sys_idx]['tasks'][task_idx]['min'][1]
            ) for f in parsed_files]
            maxs_ns = [convert_to_ns(
                f.system_threads[sys_idx]['tasks'][task_idx]['max'][0],
                f.system_threads[sys_idx]['tasks'][task_idx]['max'][1]
            ) for f in parsed_files]
            totals_ns = [convert_to_ns(
                f.system_threads[sys_idx]['tasks'][task_idx]['total'][0],
                f.system_threads[sys_idx]['tasks'][task_idx]['total'][1]
            ) for f in parsed_files]
            
            # Calculate averages in nanoseconds
            avg_avg_ns = sum(avgs_ns) / n
            avg_min_ns = sum(mins_ns) / n
            avg_max_ns = sum(maxs_ns) / n
            avg_total_ns = sum(totals_ns) / n
            
            # Convert back to original units
            avg_sys_task = {
                'name': template_task['name'],
                'executions': int(sum(execs) / n),
                'avg': (convert_from_ns(avg_avg_ns, template_task['avg'][1]), template_task['avg'][1]),
                'min': (convert_from_ns(avg_min_ns, template_task['min'][1]), template_task['min'][1]),
                'max': (convert_from_ns(avg_max_ns, template_task['max'][1]), template_task['max'][1]),
                'total': (convert_from_ns(avg_total_ns, template_task['total'][1]), template_task['total'][1])
            }
            avg_sys['tasks'].append(avg_sys_task)
        
        avg_file.system_threads.append(avg_sys)
    
    return avg_file


def write_timing_file(timing_file: TimingFile, output_path: str):
    """Write a TimingFile to disk in the original format."""
    with open(output_path, 'w') as f:
        # Write header
        f.write(f"Time Statistics for {output_path}\n")
        f.write(f"Total Slots: {timing_file.total_slots}\n")
        f.write(f"Timing Method: {timing_file.timing_method}\n")
        f.write(f"Worker Slots: {timing_file.worker_slots_range}, System Thread Slots: {timing_file.system_slots_range}\n")
        f.write("****************\n")
        
        # Write aggregated statistics
        f.write("Aggregated Statistics (All Slots):\n")
        f.write(f"  Total Streams Processed: {timing_file.total_streams}\n")
        
        # Write streams per slot
        slot_str = ", ".join([f"Slot {k}: {v}" for k, v in sorted(timing_file.streams_per_slot.items())])
        f.write(f"  Streams per Slot: {slot_str}\n")
        
        # Auto-convert units for better readability
        total_runtime_val, total_runtime_unit = auto_convert_unit(*timing_file.total_runtime)
        avg_time_val, avg_time_unit = auto_convert_unit(*timing_file.avg_time_per_stream)
        min_val, min_unit = auto_convert_unit(*timing_file.min_max_per_stream[0])
        max_val, max_unit = auto_convert_unit(*timing_file.min_max_per_stream[1])
        total_compute_val, total_compute_unit = auto_convert_unit(*timing_file.total_compute_time)
        avg_compute_val, avg_compute_unit = auto_convert_unit(*timing_file.avg_compute_time_per_stream)
        
        f.write(f"  Total Runtime: {format_unit_value(total_runtime_val, total_runtime_unit)}\n")
        f.write(f"  Avg Time Per Stream: {format_unit_value(avg_time_val, avg_time_unit)}\n")
        f.write(f"  Min/Max Per Stream: {format_unit_value(min_val, min_unit)} / {format_unit_value(max_val, max_unit)}\n")
        f.write(f"  Total Compute Time: {format_unit_value(total_compute_val, total_compute_unit)}\n")
        f.write(f"  Avg Compute Time Per Stream: {format_unit_value(avg_compute_val, avg_compute_unit)}\n")
        f.write("****************\n")
        
        # Write per-task analysis
        f.write("Per-Task Analysis (Aggregated):\n")
        for task in timing_file.tasks:
            f.write("  ****************\n")
            f.write(f"  Task '{task['name']}' - Workers: {task['workers']}, Total Executions: {task['executions']}\n")
            
            # Write timing line
            timing_parts = []
            if 'avg_stream' in task['timing']:
                val, unit = auto_convert_unit(*task['timing']['avg_stream'])
                timing_parts.append(f"Avg/Stream: {format_unit_value(val, unit)}")
            if 'avg_task' in task['timing']:
                val, unit = auto_convert_unit(*task['timing']['avg_task'])
                timing_parts.append(f"Avg/Task: {format_unit_value(val, unit)}")
            if 'min' in task['timing']:
                val, unit = auto_convert_unit(*task['timing']['min'])
                timing_parts.append(f"Min: {format_unit_value(val, unit)}")
            if 'max' in task['timing']:
                val, unit = auto_convert_unit(*task['timing']['max'])
                timing_parts.append(f"Max: {format_unit_value(val, unit)}")
            if 'total' in task['timing']:
                val, unit = auto_convert_unit(*task['timing']['total'])
                timing_parts.append(f"Total: {format_unit_value(val, unit)}")
            
            f.write(f"    Timing - {', '.join(timing_parts)}\n")
            
            # Write worker summary
            worker_parts = []
            for worker_id in sorted(task['worker_summary'].keys()):
                ws = task['worker_summary'][worker_id]
                val, unit = auto_convert_unit(*ws['time'])
                worker_parts.append(
                    f"W-{worker_id}: {ws['count']} ({ws['percent']:.1f}%) - {format_unit_value(val, unit)}"
                )
            f.write(f"    Worker Summary: {', '.join(worker_parts)}\n")
        
        f.write("****************\n")
        
        # Write system thread tasks
        f.write("System Thread Tasks (Slots " + timing_file.system_slots_range + "):\n")
        for sys_thread in timing_file.system_threads:
            f.write(f"  Resolution Thread {sys_thread['thread_id']} (Slot {sys_thread['slot']}):\n")
            for task in sys_thread['tasks']:
                avg_val, avg_unit = auto_convert_unit(*task['avg'])
                min_val, min_unit = auto_convert_unit(*task['min'])
                max_val, max_unit = auto_convert_unit(*task['max'])
                total_val, total_unit = auto_convert_unit(*task['total'])
                
                f.write(
                    f"    Task '{task['name']}' - "
                    f"Executions: {task['executions']}, "
                    f"Avg: {format_unit_value(avg_val, avg_unit)}, "
                    f"Min: {format_unit_value(min_val, min_unit)}, "
                    f"Max: {format_unit_value(max_val, max_unit)}, "
                    f"Total: {format_unit_value(total_val, total_unit)}\n"
                )


def main():
    parser = argparse.ArgumentParser(
        description="Average timing metrics from multiple timing files (.txt and .csv)."
    )
    parser.add_argument(
        "files",
        nargs="+",
        help="List of timing files to average (.txt and/or .csv)"
    )
    parser.add_argument(
        "-o", "--output",
        default="timing_avg.txt",
        help="Output file path (default: timing_avg.txt)"
    )
    parser.add_argument(
        "--system-threads",
        type=int,
        default=0,
        help="Number of system threads for CSV analysis (default: 0, auto-detect)"
    )
    parser.add_argument(
        "--units",
        choices=["ns", "us", "ms", "s"],
        default="us",
        help="Time units for CSV analysis display (default: us)"
    )
    
    args = parser.parse_args()

    # Separate txt and csv files
    txt_files = []
    csv_files = []
    
    for filepath in args.files:
        if filepath.endswith(".txt"):
            txt_files.append(filepath)
        elif filepath.endswith(".csv"):
            csv_files.append(filepath)
    
    # Process .txt files
    if txt_files:
        if len(txt_files) < 2:
            print("Warning: Averaging with less than 2 .txt files. Proceeding anyway.")
        
        print(f"Averaging {len(txt_files)} .txt files:")
        for f in txt_files:
            print(f"  - {f}")
        print()
        
        avg_file = average_timing_files(txt_files)
        
        if avg_file:
            write_timing_file(avg_file, args.output)
            print(f"Averaged timing file written to: {args.output}")
        else:
            print("Error: Could not average .txt timing files.")
    
    # Process .csv files
    if csv_files:
        if len(csv_files) < 2:
            print("Warning: Averaging with less than 2 .csv files. Proceeding anyway.")
        
        print(f"\nAveraging {len(csv_files)} .csv schedule files:")
        for f in csv_files:
            print(f"  - {f}")
        print()
        
        # Determine system threads from first CSV if not specified
        system_threads = args.system_threads
        if system_threads == 0:
            # Auto-detect from first file
            records = read_csv(csv_files[0])
            if records:
                slots = group_by_slot(records)
                num_slots = len(slots)
                # Heuristic: assume 1 system thread for now
                system_threads = 1
                print(f"Auto-detected {system_threads} system thread(s) from {num_slots} slots")
        
        avg_metrics = average_csv_analyses(csv_files, system_threads)
        
        if avg_metrics:
            # Generate output path for schedule analysis
            # Replace .txt with _sched.txt
            base_name = os.path.splitext(args.output)[0]
            sched_output = f"{base_name}_sched.txt"
            
            write_csv_analysis(avg_metrics, sched_output, args.units)
            print(f"Averaged schedule analysis written to: {sched_output}")
        else:
            print("Error: Could not average .csv schedule files.")
    
    if not txt_files and not csv_files:
        print("Error: No valid .txt or .csv files provided.")
        # exit with error
        exit(1)


if __name__ == "__main__":
    main()
