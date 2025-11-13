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


def visualize(csv_path, out_path, units="us", exclude=None):
    records = read_csv(csv_path)
    # filter excluded task_ids if provided
    if exclude:
        try:
            exclude_set = set(int(x) for x in exclude.split(",") if x.strip() != "")
        except Exception:
            raise ValueError("--exclude expects a comma-separated list of integers")
        records = [r for r in records if r[5] not in exclude_set]
    if not records:
        print("No records found in", csv_path)
        return

    slots = group_by_slot(records)

    # choose global baseline so all subplots share same time origin
    global_min = min(r[2] for r in records)
    global_max = max(r[3] for r in records)

    if units == "us":
        scale = 1e3
        xlabel = "Time (µs)"
    elif units == "ms":
        scale = 1e6
        xlabel = "Time (ms)"
    else:
        scale = 1.0
        xlabel = "Time (ns)"

    # Calculate per-task statistics
    print("\n" + "=" * 80)
    print("Per-Task Timing Statistics")
    print("=" * 80)

    task_stats = defaultdict(
        lambda: {"count": 0, "total_duration": 0, "min": float("inf"), "max": 0}
    )

    for rec in records:
        _, job_id, start_ns, end_ns, worker, task_id, index = rec
        duration_ns = end_ns - start_ns
        task_stats[task_id]["count"] += 1
        task_stats[task_id]["total_duration"] += duration_ns
        task_stats[task_id]["min"] = min(task_stats[task_id]["min"], duration_ns)
        task_stats[task_id]["max"] = max(task_stats[task_id]["max"], duration_ns)

    # Print statistics for each task_id
    for task_id in sorted(task_stats.keys()):
        stats = task_stats[task_id]
        count = stats["count"]
        total_duration = stats["total_duration"]
        avg_duration = total_duration / count
        min_duration = stats["min"]
        max_duration = stats["max"]

        if units == "us":
            total_scaled = total_duration / 1e3
            avg_scaled = avg_duration / 1e3
            min_scaled = min_duration / 1e3
            max_scaled = max_duration / 1e3
            unit_str = "µs"
        elif units == "ms":
            total_scaled = total_duration / 1e6
            avg_scaled = avg_duration / 1e6
            min_scaled = min_duration / 1e6
            max_scaled = max_duration / 1e6
            unit_str = "ms"
        else:
            total_scaled = total_duration
            avg_scaled = avg_duration
            min_scaled = min_duration
            max_scaled = max_duration
            unit_str = "ns"

        print(f"Task ID {task_id}:")
        print(f"  Executions: {count}")
        print(f"  Total Time: {total_scaled:.4f} {unit_str}")
        print(f"  Avg/Task: {avg_scaled:.4f} {unit_str}")
        print(f"  Min: {min_scaled:.4f} {unit_str}")
        print(f"  Max: {max_scaled:.4f} {unit_str}")
        print()

    print("=" * 80 + "\n")

    n_slots = len(slots)
    # dynamic figure size: height depends on number of slots and workers
    fig_height = max(2.5 * n_slots, 4)
    fig, axs = plt.subplots(n_slots, 1, sharex=True, figsize=(12, fig_height))
    if n_slots == 1:
        axs = [axs]

    # overall x limits
    x_min = (global_min - global_min) / scale
    x_max = (global_max - global_min) / scale

    # build color map per task_id
    task_ids = sorted({r[5] for r in records})
    cmap = plt.get_cmap("tab20")
    color_map = {}
    for i, tid in enumerate(task_ids):
        color_map[tid] = cmap(i % cmap.N)

    for ax, (slot, recs) in zip(axs, slots.items()):
        # collect unique workers for y ticks
        workers = sorted({r[4] for r in recs})
        # plotting
        for rec in recs:
            _, job_id, start_ns, end_ns, worker, task_id, index = rec
            start = (start_ns - global_min) / scale
            end = (end_ns - global_min) / scale
            width = max(end - start, 1e-6)
            # bar centered at worker id (use numeric worker value)
            col = color_map.get(task_id, "gray")
            ax.barh(worker, width, left=start, height=0.6, align="center", color=col)
            # labels removed — task_id is shown via color + legend

        ax.set_ylabel(f"slot {slot}")
        ax.set_yticks(sorted({r[4] for r in recs}))
        ax.set_ylim(min(workers) - 1, max(workers) + 1)
        ax.grid(axis="x", linestyle=":", alpha=0.6)

    axs[-1].set_xlabel(xlabel)
    axs[-1].set_xlim(x_min, x_max)
    # add legend for task_id colors
    # create legend handles in ascending task_id order
    import matplotlib.patches as mpatches

    legend_handles = []
    # limit legend size to avoid huge legends
    max_legend = 100
    for tid in task_ids[:max_legend]:
        patch = mpatches.Patch(color=color_map.get(tid, "gray"), label=str(tid))
        legend_handles.append(patch)
    if legend_handles:
        # place legend above the top subplot
        axs[0].legend(
            handles=legend_handles,
            title="task_id",
            bbox_to_anchor=(1.02, 1),
            loc="upper left",
        )
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    print("Saved visualization to", out_path)


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
    args = p.parse_args()
    visualize(args.csv, args.out, units=args.units, exclude=args.exclude)


if __name__ == "__main__":
    main()
