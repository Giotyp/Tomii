#!/usr/bin/env python3
"""
Visualize scheduler CSV produced by SynStream.

Usage:
    python3 scripts/scheduler_visualize.py schedule_log.csv -o out.png --units us

The CSV must have columns: slot,job_id,start_ns,end_ns,worker,task_id,index

Creates one subplot per `slot` found in the CSV. Each bar is a task executed
on a worker; Time is shown in microseconds by default (use `--units ns/ms/us`
to change). The x-axis is time, and the y-axis is the worker number. The
color of the bar is determined by the `task_id`.

Args:
    csv: Path to CSV file.
    -o, --out: Output image path (default: schedule.png).
    --units: Time units for x-axis (ns, us, ms). Default is us.
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


def visualize(csv_path, out_path, units="us", exclude=None, task_names=None):
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

    # Find systems thread which corresponds to maximum slot value
    system_slot = max(slots.keys())
    print("System Thread executed in slot:", system_slot)

    # Separate system records and filter by excluded task_ids
    all_system_records = slots.pop(system_slot)
    
    # Identify resolution task_id
    resolution_task_id = -1
    if all_system_records:
        sys_task_ids = [r[5] for r in all_system_records]
        resolution_task_id = max(sys_task_ids)
    
    # choose global baseline so all subplots share same time origin
    # Use the earliest start time from remaining (non-excluded) worker records only (exclude system records)
    worker_records = [r for slot_recs in slots.values() for r in slot_recs]
    global_min = min(r[2] for r in worker_records)
    global_max = max(r[3] for r in worker_records)
    
    # Filter system records: exclude by task_id and for resolution tasks, exclude anything before global_min
    system_records = []
    for r in all_system_records:
        task_id = r[5]
        start_ns = r[2]
        
        # Skip if task_id is excluded
        if task_id in exclude_set:
            continue
        
        # For resolution tasks, skip if they start before the earliest remaining task
        if task_id == resolution_task_id and start_ns < global_min:
            continue
        
        system_records.append(r)

    if units == "us":
        scale = 1e3
        xlabel = "Time (µs)"
    elif units == "ms":
        scale = 1e6
        xlabel = "Time (ms)"
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
    if units == "us":
        global_min_scaled = global_min / 1e3
        unit_str = "µs"
    elif units == "ms":
        global_min_scaled = global_min / 1e6
        unit_str = "ms"
    else:
        global_min_scaled = global_min
        unit_str = "ns"
    
    print(f"Global Minimum Timestamp: {global_min_scaled:.4f} {unit_str}\n")

    task_stats = defaultdict(
        lambda: {"count": 0, "total_duration": 0, "min": float("inf"), "max": 0, "min_start": float("inf")}
    )

    for rec in records:
        slot, job_id, start_ns, end_ns, worker, task_id, index = rec
        if slot == system_slot:
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

        if units == "us":
            total_scaled = total_duration / 1e3
            avg_scaled = avg_duration / 1e3
            min_scaled = min_duration / 1e3
            max_scaled = max_duration / 1e3
            min_start_scaled = min_start_time / 1e3
            unit_str = "µs"
        elif units == "ms":
            total_scaled = total_duration / 1e6
            avg_scaled = avg_duration / 1e6
            min_scaled = min_duration / 1e6
            max_scaled = max_duration / 1e6
            min_start_scaled = min_start_time / 1e6
            unit_str = "ms"
        else:
            total_scaled = total_duration
            avg_scaled = avg_duration
            min_scaled = min_duration
            max_scaled = max_duration
            min_start_scaled = min_start_time
            unit_str = "ns"

        task_label = id_map.get(task_id, task_id)
        print(f"Task {task_label}:")
        print(f"  Executions: {count}")
        print(f"  First Start: {min_start_scaled:.4f} {unit_str}")
        print(f"  Total Time: {total_scaled:.4f} {unit_str}")
        print(f"  Avg/Task: {avg_scaled:.4f} {unit_str}")
        print(f"  Min: {min_scaled:.4f} {unit_str}")
        print(f"  Max: {max_scaled:.4f} {unit_str}")
        print()

    print("=" * 80 + "\n")

    n_slots = len(slots)
    if n_slots == 0:
        print("No worker slots found to plot.")
        return

    # dynamic figure size: height depends on number of slots and workers
    fig_height = max(2.5 * n_slots, 4)
    fig, axs = plt.subplots(n_slots, 1, sharex=True, figsize=(12, fig_height))
    if n_slots == 1:
        axs = [axs]

    # overall x limits - add small margin to show tasks clearly
    margin = (global_max - global_min) * 0.02  # 2% margin on each side
    x_min = (global_min - margin) / scale
    x_max = (global_max + margin) / scale

    # id_map was already built earlier for statistics
    for label in id_map.values():
        unique_keys.add(label)

    # System records
    system_workers = set()
    for r in system_records:
        tid = r[5]
        idx = r[6]
        system_workers.add(r[4])
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
    # Force system-thread entries (resolution and prep) to use a consistent gray
    system_res_color = csscolors['black']
    system_prep_color = csscolors['peru']
    if "system-res" in color_map:
        color_map["system-res"] = system_res_color
    if "system-prep" in color_map:
        color_map["system-prep"] = system_prep_color
    for ax, (slot, recs) in zip(axs, slots.items()):
        # collect unique workers for y ticks
        workers = set()

        # Plot worker records
        for rec in recs:
            _, job_id, start_ns, end_ns, worker, task_id, index = rec
            workers.add(worker)
            start = start_ns / scale
            end = end_ns / scale
            width = max(end - start, 1e-6)

            label = id_map.get(task_id, task_id)
            col = color_map.get(label, "gray")
            ax.barh(worker, width, left=start, height=0.6, align="center", color=col)

        # Plot system records
        for rec in system_records:
            _, job_id, start_ns, end_ns, worker, task_id, index = rec
            workers.add(worker)
            start = start_ns / scale
            end = end_ns / scale
            width = max(end - start, 1e-6)

            if task_id == resolution_task_id:
                label = "system-res"
            else:
                label = "system-prep"

            col = color_map.get(label, "gray")
            ax.barh(worker, width, left=start, height=0.6, align="center", color=col)

        # Draw separator line for system workers
        for sys_worker in system_workers:
            ax.axhline(
                y=sys_worker + 0.5,
                color="black",
                linestyle="--",
                linewidth=0.8,
                alpha=0.7,
            )

        ax.set_ylabel(f"slot {slot}")
        ax.set_yticks(sorted(list(workers)))
        ax.set_ylim(min(workers) - 1, max(workers) + 1)
        ax.grid(axis="x", linestyle=":", alpha=0.6)

    axs[-1].set_xlabel(xlabel)
    axs[-1].set_xlim(x_min, x_max)

    # add title on top of the whole graph
    num_workers = len(workers)
    num_system_workers = len(system_workers)
    num_slots = len(slots)
    plt.suptitle(
        f"Task Scheduler Visualization\nWorker Cores: {num_workers-num_system_workers} / System Workers: {num_system_workers} / Slots: {num_slots}"
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
    p.add_argument(
        "--units",
        choices=["ns", "us", "ms"],
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
    args = p.parse_args()
    task_names = args.tasks
    print("Task names:", task_names)

    visualize(
        args.csv,
        args.out,
        units=args.units,
        exclude=args.exclude,
        task_names=task_names,
    )


if __name__ == "__main__":
    main()
