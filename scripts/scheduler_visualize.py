#!/usr/bin/env python3
"""
Visualize scheduler CSV produced by SynStream.

Usage:
    python3 scripts/scheduler_visualize.py schedule_log.csv -o out.png --units us

The CSV must have columns: slot,job_id,start_ns,end_ns,worker,task_id,index

Creates one subplot per `slot` found in the CSV. Each bar is a task executed
on a worker; Time is shown in microseconds by default (use `--units ns/ms/us/s`
to change). The x-axis is time, and the y-axis is the worker number. The
color of the bar is determined by the `task_id`.

Args:
    csv: Path to CSV file.
    -o, --out: Output image path (default: schedule.png).
    --units: Time units for x-axis (ns, us, ms, s). Default is us.
    --exclude: Comma-separated list of task_id values to exclude from plotting.
"""
import argparse
import csv
from collections import defaultdict
import math
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

    
# hard-coded task color-map for consistent coloring across runs
csscolors = mcolors.CSS4_COLORS
cmap = [csscolors["red"], csscolors["blue"], csscolors["green"], csscolors["orange"], csscolors["violet"], csscolors["springgreen"], csscolors["teal"], csscolors["brown"], csscolors["magenta"], csscolors["olive"], csscolors["cyan"], csscolors["gold"]]

def read_csv(path):
    records = []
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            try:
                slot = int(r["slot"])
                job_id = int(r["job_id"])
                start_ns = int(r["start_ns"])
                end_ns = int(r["end_ns"])
                worker = int(r["worker"])
                task_id = int(r["task_id"])
                index = int(r["index"])
            except Exception as e:
                raise
            records.append((slot, job_id, start_ns, end_ns, worker, task_id, index))
    return records


def group_by_slot(records):
    slots = defaultdict(list)
    for rec in records:
        slot = rec[0]
        slots[slot].append(rec)
    return dict(sorted(slots.items()))


def visualize(csv_path, out_path, title, units="us", exclude=None, task_names=None, system_threads=0):
    records = read_csv(csv_path)
    # filter excluded task_ids if provided
    exclude_set = set()
    if exclude:
        try:
            exclude_set = set(int(x) for x in exclude.split(",") if x.strip() != "")
            print("Excluding task_ids:", exclude_set)
        except Exception:
            raise ValueError("--exclude expects a comma-separated list of integers")
        records = [r for r in records if r[5] not in exclude_set]
    if not records:
        print("No records found in", csv_path)
        return

    slots = group_by_slot(records)

    # Separate worker slots from system thread slots based on system_threads parameter
    all_slots = sorted(slots.keys())
    
    worker_slots = {}
    system_slots = {}
    
    if system_threads > 0 and len(all_slots) > system_threads:
        # System thread slots are the highest numbered slots
        system_slot_threshold = max(all_slots) - system_threads + 1
        
        for slot_id, slot_records in slots.items():
            if slot_id >= system_slot_threshold:
                system_slots[slot_id] = slot_records
            else:
                worker_slots[slot_id] = slot_records
    else:
        # Fallback: use heuristic if system_threads not specified
        for slot_id, slot_records in slots.items():
            task_ids = [r[5] for r in slot_records]
            if slot_id >= all_slots[-1] - 10:  # Assume last ~10 slots could be system
                is_system = True
                for r in slot_records:
                    if r[5] < 100:  # Arbitrary threshold
                        is_system = False
                        break
                if is_system:
                    system_slots[slot_id] = slot_records
                else:
                    worker_slots[slot_id] = slot_records
            else:
                worker_slots[slot_id] = slot_records
        
        if not system_slots and all_slots:
            max_slot = max(all_slots)
            system_slots[max_slot] = slots[max_slot]
            worker_slots = {k: v for k, v in slots.items() if k != max_slot}
    
    print(f"Worker slots: {sorted(worker_slots.keys())}")
    print(f"System slots: {sorted(system_slots.keys())}")
    
    # Identify resolution task_id from system slots
    resolution_task_id = -1
    all_system_records = []
    for sys_slot_records in system_slots.values():
        all_system_records.extend(sys_slot_records)
    
    if all_system_records:
        sys_task_ids = [r[5] for r in all_system_records]
        resolution_task_id = max(sys_task_ids)
        print(f"Resolution task_id identified as: {resolution_task_id}")
    
    # choose global baseline so all subplots share same time origin
    # Use the earliest start time from worker records only
    worker_records = [r for slot_recs in worker_slots.values() for r in slot_recs]
    if worker_records:
        global_min = min(r[2] for r in worker_records)
        global_max = max(r[3] for r in worker_records)
    else:
        # Fallback if no worker records
        global_min = min(r[2] for r in all_system_records)
        global_max = max(r[3] for r in all_system_records)
    
    # Filter system records: exclude by task_id and for resolution tasks, exclude anything before global_min
    filtered_system_slots = {}
    for slot_id, sys_slot_records in system_slots.items():
        filtered_records = []
        for r in sys_slot_records:
            task_id = r[5]
            start_ns = r[2]
            
            # Skip if task_id is excluded
            if task_id in exclude_set:
                continue
            
            # For resolution tasks, skip if they start before the earliest remaining task
            if task_id == resolution_task_id and start_ns < global_min:
                continue
            
            filtered_records.append(r)
        if filtered_records:
            filtered_system_slots[slot_id] = filtered_records
    
    system_slots = filtered_system_slots
    slots = worker_slots  # Continue with worker slots for main visualization

    if units == "us":
        scale = 1e3
        xlabel = "Time (µs)"
    elif units == "ms":
        scale = 1e6
        xlabel = "Time (ms)"
    elif units == "s":
        scale = 1e9
        xlabel = "Time (s)"
    else:
        scale = 1.0
        xlabel = "Time (ns)"

    # build color map and task name mapping first
    # Collect all unique keys for legend.
    unique_keys = set()
    # Normal records
    worker_task_ids = set()
    for slot, recs in slots.items():
        for r in recs:
            worker_task_ids.add(r[5])  # task_id

    sorted_worker_ids = sorted(list(worker_task_ids))
    id_map = {}
    if task_names:
        if len(task_names) == len(sorted_worker_ids):
            for tid, name in zip(sorted_worker_ids, task_names):
                id_map[tid] = name
        else:
            print(
                f"Warning: Provided {len(task_names)} task names but found {len(sorted_worker_ids)} unique worker tasks. Ignoring names."
            )
            for tid in sorted_worker_ids:
                id_map[tid] = tid
    else:
        for tid in sorted_worker_ids:
            id_map[tid] = tid

    # Calculate per-task statistics
    print("\n" + "=" * 80)
    print("Per-Task Timing Statistics")
    print("=" * 80)
    
    # Print global minimum timestamp
    global_min_scaled = global_min / scale
    global_max_scaled = global_max / scale
    
    print(f"Global Minimum Timestamp: {global_min_scaled:.4f} {units}\n")
    print(f"Global Maximum Timestamp: {global_max_scaled:.4f} {units}\n")
    print(f"Total Duration: {global_max_scaled - global_min_scaled:.4f} {units}\n")

    task_stats = defaultdict(
        lambda: {"count": 0, "total_duration": 0, "min": float("inf"), "max": 0, "min_start": float("inf")}
    )

    for rec in records:
        slot, job_id, start_ns, end_ns, worker, task_id, index = rec
        # Skip system slots
        if slot in system_slots:
            continue
        duration_ns = end_ns - start_ns
        task_stats[task_id]["count"] += 1
        task_stats[task_id]["total_duration"] += duration_ns
        task_stats[task_id]["min"] = min(task_stats[task_id]["min"], duration_ns)
        task_stats[task_id]["max"] = max(task_stats[task_id]["max"], duration_ns)
        task_stats[task_id]["min_start"] = min(task_stats[task_id]["min_start"], start_ns)

    # Print statistics for each task_id
    for task_id in sorted(task_stats.keys()):
        stats = task_stats[task_id]
        count = stats["count"]
        total_duration = stats["total_duration"]
        avg_duration = total_duration / count
        min_duration = stats["min"]
        max_duration = stats["max"]
        min_start_time = stats["min_start"]

        total_scaled = total_duration / scale
        avg_scaled = avg_duration / scale
        min_scaled = min_duration / scale
        max_scaled = max_duration / scale
        min_start_scaled = min_start_time / scale

        task_label = id_map.get(task_id, task_id)
        print(f"Task {task_label}:")
        print(f"  Executions: {count}")
        print(f"  First Start: {min_start_scaled:.4f} {units}")
        print(f"  Total Time: {total_scaled:.4f} {units}")
        print(f"  Avg/Task: {avg_scaled:.4f} {units}")
        print(f"  Min: {min_scaled:.4f} {units}")
        print(f"  Max: {max_scaled:.4f} {units}")
        print()

    print("=" * 80 + "\n")

    n_worker_slots = len(slots)
    
    if n_worker_slots == 0:
        print("No worker slots found to plot.")
        return

    # dynamic figure size: height depends on number of worker slots only
    fig_height = max(2.5 * n_worker_slots, 4)
    fig, axs = plt.subplots(n_worker_slots, 1, sharex=True, figsize=(12, fig_height))
    if n_worker_slots == 1:
        axs = [axs]

    # overall x limits - add small margin to show tasks clearly
    margin = (global_max - global_min) * 0.02  # 2% margin on each side
    x_min = (global_min - margin) / scale
    x_max = (global_max + margin) / scale

    # id_map was already built earlier for statistics
    for label in id_map.values():
        unique_keys.add(label)

    # System records - now grouped by slot
    system_workers = set()
    all_system_records_flat = []
    for sys_slot_id, sys_slot_records in system_slots.items():
        for r in sys_slot_records:
            tid = r[5]
            idx = r[6]
            system_workers.add(r[4])
            all_system_records_flat.append(r)
            if tid == resolution_task_id:
                unique_keys.add("system-res")
            else:
                unique_keys.add(f"system-prep")

    # Define order for legend/colors
    list_keys = []

    # 1. Worker tasks
    if task_names and len(task_names) == len(sorted_worker_ids):
        list_keys.extend(task_names)
    else:
        list_keys.extend(sorted_worker_ids)

    # 2. System tasks (append if present in unique_keys)
    if "system-res" in unique_keys:
        list_keys.append("system-res")
    if "system-prep" in unique_keys:
        list_keys.append("system-prep")

    color_map = {}
    for i, k in enumerate(list_keys):
        color_map[k] = cmap[i % len(cmap)]
    # Force system-thread entries (resolution and prep) to use a consistent color
    system_res_color = csscolors['black']
    system_prep_color = csscolors['peru']
    if "system-res" in color_map:
        color_map["system-res"] = system_res_color
    if "system-prep" in color_map:
        color_map["system-prep"] = system_prep_color
    
    # Plot worker slots with system thread activities side-by-side
    plot_idx = 0
    
    # Collect all system records grouped by worker
    prep_tasks_by_worker = defaultdict(list)
    res_tasks_by_worker = defaultdict(list)
    for sys_slot_id, sys_slot_records in system_slots.items():
        for rec in sys_slot_records:
            worker = rec[4]
            if rec[5] == resolution_task_id:
                res_tasks_by_worker[worker].append(rec)
            else:
                prep_tasks_by_worker[worker].append(rec)
    
    # Plot each worker slot
    for slot, recs in sorted(slots.items()):
        ax = axs[plot_idx]
        plot_idx += 1
        workers = set()
        system_workers_in_slot = set()

        # First, plot worker records
        for rec in recs:
            _, job_id, start_ns, end_ns, worker, task_id, index = rec
            workers.add(worker)
            start = start_ns / scale
            end = end_ns / scale
            width = max(end - start, 1e-6)

            label = id_map.get(task_id, task_id)
            col = color_map.get(label, "gray")
            ax.barh(worker, width, left=start, height=0.6, align="center", color=col, zorder=2)
        
        # Collect all system thread workers
        all_system_workers = set(prep_tasks_by_worker.keys()) | set(res_tasks_by_worker.keys())
        system_workers_in_slot.update(all_system_workers)
        workers.update(all_system_workers)
        
        # Plot system threads with half-height bars side by side
        bar_height = 0.4  # Half of normal height for each type
        
        # Plot resolution tasks (bottom half)
        for worker in all_system_workers:
            for rec in res_tasks_by_worker[worker]:
                _, job_id, start_ns, end_ns, _, task_id, index = rec
                start = start_ns / scale
                end = end_ns / scale
                width = max(end - start, 1e-6)
                
                label = "system-res"
                col = color_map.get(label, system_res_color)
                # Position at worker - bar_height/2 (bottom half)
                ax.barh(worker - bar_height/2, width, left=start, height=bar_height, 
                       align="center", color=col, alpha=0.8, zorder=1)
        
        # Plot preparation tasks (top half)
        for worker in all_system_workers:
            for rec in prep_tasks_by_worker[worker]:
                _, job_id, start_ns, end_ns, _, task_id, index = rec
                start = start_ns / scale
                end = end_ns / scale
                width = max(end - start, 1e-6)
                
                label = "system-prep"
                col = color_map.get(label, system_prep_color)
                # Position at worker + bar_height/2 (top half)
                ax.barh(worker + bar_height/2, width, left=start, height=bar_height, 
                       align="center", color=col, alpha=0.8, zorder=1)
        
        # Draw separator lines for system workers
        for sys_worker in system_workers_in_slot:
            ax.axhline(
                y=sys_worker + 0.5,
                color="black",
                linestyle="--",
                linewidth=0.8,
                alpha=0.7,
            )

        ax.set_ylabel(f"slot {slot}")
        if workers:
            ax.set_yticks(sorted(list(workers)))
            ax.set_ylim(min(workers) - 1, max(workers) + 1)
        ax.grid(axis="x", linestyle=":", alpha=0.6)

    axs[-1].set_xlabel(xlabel)
    axs[-1].set_xlim(x_min, x_max)

    # add title on top of the whole graph
    # Count unique worker cores from worker slots (excluding system workers)
    all_worker_cores = set()
    system_workers_set = set()
    
    for slot_recs in slots.values():
        for rec in slot_recs:
            all_worker_cores.add(rec[4])
    
    for sys_recs in system_slots.values():
        for rec in sys_recs:
            system_workers_set.add(rec[4])
    
    # Remove system workers from worker core count
    pure_worker_cores = all_worker_cores - system_workers_set
    
    num_worker_cores = len(pure_worker_cores)
    num_system_threads = len(system_slots)
    num_worker_slots = len(slots)
    plt.suptitle(
        f"{title}\n"
        f"Worker Cores: {num_worker_cores} / System Threads: {num_system_threads} / "
        f"Worker Slots: {num_worker_slots}"
    )

    # add legend
    import matplotlib.patches as mpatches

    legend_handles = []
    # limit legend size to avoid huge legends
    max_legend = 10
    for key in list_keys[:max_legend]:
        patch = mpatches.Patch(color=color_map.get(key, "gray"), label=str(key))
        legend_handles.append(patch)
    if legend_handles:
        # place legend above the top subplot
        axs[0].legend(
            handles=legend_handles,
            title="Task / System",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
        )
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    print("Saved visualization to", out_path)


def task_name_list(arg_string):
    """Parse a comma-separated bracketed list of task names"""
    if not arg_string:
        return []

    arg_string = arg_string.strip("[]")
    return [item.strip() for item in arg_string.split(",")]


def main():
    p = argparse.ArgumentParser(description="Visualize scheduler CSV")
    p.add_argument("csv", help="Path to CSV produced by scheduler")
    p.add_argument("-o", "--out", help="Output image path", default="schedule.png")
    p.add_argument("--title", help="Title for the plot", default="Scheduler Visualization")
    p.add_argument(
        "--units",
        choices=["ns", "us", "ms", "s"],
        default="us",
        help="Time units for x-axis",
    )
    p.add_argument(
        "--exclude",
        help="Comma-separated list of task_id values to exclude from plotting",
        default="",
    )
    p.add_argument(
        "--tasks",
        type=task_name_list,
        help="Comma-separated list of task names (e.g. [gen,fft,mult]) to label task_ids. Must match count of unique worker tasks.",
        default=None,
    )
    p.add_argument(
        "--system-threads",
        type=int,
        help="Number of system threads (slots at the end are system thread slots)",
        default=1,
    )
    args = p.parse_args()
    task_names = args.tasks
    print("Task names:", task_names)

    visualize(
        args.csv,
        args.out,
        title=args.title,
        units=args.units,
        exclude=args.exclude,
        task_names=task_names,
        system_threads=args.system_threads,
    )


if __name__ == "__main__":
    main()
