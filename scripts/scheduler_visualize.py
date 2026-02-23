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

Scheduling Latency Visualization:
- Automatically detects scheduling latency records (task_id = IdType::MAX - 3*original_task_id)
- Displays spawn time as a colored circle marker (○)
- Shows scheduling delay as a dashed line connecting spawn to execution start
- Provides detailed latency statistics (avg, median, min, max, P95, P99)

Slots are categorized as:
- Worker slots: [0 ... slots-1] - Main computational tasks
- System slots: [slots ... slots+system_threads-1] - Resolution/preparation threads
- Receiver slots: [slots+system_threads ... max] - Network packet reception threads

Time baseline (time zero):
- By default: Earliest received packet timestamp (shows full end-to-end latency)
- With --no-rcv: Earliest worker task timestamp (excludes packet reception time)

Args:
    csv: Path to CSV file.
    -o, --out: Output image path (default: schedule.png).
    --units: Time units for x-axis (ns, us, ms, s). Default is us.
    --exclude: Comma-separated list of task_id values to exclude from plotting.
    --slots: Number of worker slots (default: 0, auto-detect).
    --system-threads: Number of system threads (default: 0, auto-detect).
    --no-rcv: Use earliest worker task as time zero instead of earliest packet.
    --slot: Plot only the specified slot (useful when using --record-stream).
"""
import argparse
from collections import defaultdict
from utils import read_csv, group_by_slot, separate_worker_system_slots
import math
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors


# hard-coded task color-map for consistent coloring across runs
csscolors = mcolors.CSS4_COLORS
cmap = [
    csscolors["red"],
    csscolors["blue"],
    csscolors["green"],
    csscolors["orange"],
    csscolors["violet"],
    csscolors["springgreen"],
    csscolors["teal"],
    csscolors["brown"],
    csscolors["magenta"],
    csscolors["olive"],
    csscolors["cyan"],
    csscolors["gold"],
]


def visualize(
    csv_path,
    out_path,
    title,
    units="us",
    exclude=None,
    task_names=None,
    system_threads=0,
    worker_slots_count=0,
    no_rcv=False,
    plot_slot=None,
    plot_latency=False,
):
    records = read_csv(csv_path)
    # filter by specific slot if provided
    if plot_slot is not None:
        records = [r for r in records if r[0] == plot_slot]
        if not records:
            print(f"No records found for slot {plot_slot} in {csv_path}")
            return
        print(f"Filtering to only slot {plot_slot}: {len(records)} records")

    # ========================================================================
    # SCHEDULING LATENCY DETECTION: Separate execution and scheduling records
    # ========================================================================
    # Encoding: task_id = IdType::MAX - 3 * original_task_id for scheduling latency
    # Detect IdType::MAX as the maximum task_id in the dataset
    all_task_ids = [r[5] for r in records]
    max_task_id = max(all_task_ids)
    IDTYPE_MAX = max_task_id  # Assume max is at or near IdType::MAX

    print(f"\nScheduling Latency Detection:")
    print(f"  Estimated IdType::MAX: {IDTYPE_MAX}")

    scheduling_latency_recs = []
    execution_recs = []

    for rec in records:
        slot, job_id, start_ns, end_ns, worker, task_id, index = rec

        offset = IDTYPE_MAX - task_id

        # Heuristic: scheduling latency records have offset = 3*k for small k
        # Also check that it's not a system task (which use MAX-1, MAX-2)
        if offset > 0 and offset % 3 == 0:
            original_task_id = offset // 3
            # Sanity check: original task_id should be reasonable (< 1000)
            # This avoids false positives from regular tasks
            if 0 < original_task_id < 1000:
                scheduling_latency_recs.append(
                    {
                        "slot": slot,
                        "spawn_ns": start_ns,  # When task was spawned to scheduler
                        "exec_ns": end_ns,  # When worker started executing
                        "worker": worker,
                        "original_task_id": original_task_id,
                        "index": index,
                        "job_id": job_id,
                    }
                )
                continue

        execution_recs.append(rec)

    print(f"  Found {len(scheduling_latency_recs)} scheduling latency records")
    print(f"  Found {len(execution_recs)} execution records")

    # Use execution_recs instead of records from here on
    records = execution_recs

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

    # Separate worker slots from system thread slots and receiver thread slots
    worker_slots, system_slots, receiver_slots = separate_worker_system_slots(
        slots, system_threads, worker_slots_count
    )

    print(f"Worker slots: {sorted(worker_slots.keys())}")
    print(f"System slots: {sorted(system_slots.keys())}")
    print(f"Receiver slots: {sorted(receiver_slots.keys())}")

    # Identify resolution task_id from system slots
    resolution_task_id = -1
    preparation_task_id = -1
    packet_rx_task_id = -1
    all_system_records = []
    for sys_slot_records in system_slots.values():
        all_system_records.extend(sys_slot_records)

    if all_system_records:
        sys_task_ids = [r[5] for r in all_system_records]
        resolution_task_id = max(sys_task_ids)
        preparation_task_id = resolution_task_id - 1
        # Idle/wait task is typically one below preparation (IdType::MAX - 2)
        wait_task_id = resolution_task_id - 2
        print(f"Resolution task_id identified as: {resolution_task_id}")
        print(f"Preparation task_id identified as: {preparation_task_id}")
        print(f"Idle-wait task_id identified as: {wait_task_id}")
    else:
        wait_task_id = None

    # Identify packet reception task_id from receiver slots
    all_receiver_records = []
    for rcv_slot_records in receiver_slots.values():
        all_receiver_records.extend(rcv_slot_records)

    if all_receiver_records:
        rcv_task_ids = set(r[5] for r in all_receiver_records)
        # Packet reception typically uses task_id 0 or IdType::MAX - 2
        if len(rcv_task_ids) == 1:
            packet_rx_task_id = list(rcv_task_ids)[0]
            print(f"Packet RX task_id identified as: {packet_rx_task_id}")

    # choose global baseline so all subplots share same time origin
    # Use the earliest packet reception time as baseline (unless --no-rcv is set)
    worker_records = [r for slot_recs in worker_slots.values() for r in slot_recs]

    if not no_rcv and all_receiver_records:
        # Use earliest received packet as time zero
        global_min = min(r[2] for r in all_receiver_records)
        print(f"Using earliest packet reception as time zero (offset: {global_min} ns)")
    elif worker_records:
        # Use earliest worker task as time zero
        global_min = min(r[2] for r in worker_records)
        print(f"Using earliest worker task as time zero (offset: {global_min} ns)")
    else:
        # Fallback if no worker records
        global_min = min(r[2] for r in all_system_records)
        print(f"Using earliest system task as time zero (offset: {global_min} ns)")

    # Global max is always from worker records (end of computation)
    if worker_records:
        global_max = max(r[3] for r in worker_records)
    else:
        global_max = max(r[3] for r in all_system_records)

    # Filter system records: only filter out tasks that start before global_min for resolution tasks
    # Do NOT exclude based on exclude_set - system threads should show all their work
    filtered_system_slots = {}
    for slot_id, sys_slot_records in system_slots.items():
        filtered_records = []
        for r in sys_slot_records:
            task_id = r[5]
            start_ns = r[2]

            # For resolution tasks, skip if they start before the earliest worker task
            # For preparation tasks, also skip if they start before the earliest worker task
            if (
                task_id == resolution_task_id or task_id == preparation_task_id
            ) and start_ns < global_min:
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

    # Print global minimum timestamp (before offsetting)
    global_min_scaled = global_min / scale
    global_max_scaled = global_max / scale

    print(f"Global Minimum Timestamp (raw): {global_min_scaled:.4f} {units}")
    print(f"Global Maximum Timestamp (raw): {global_max_scaled:.4f} {units}")
    print(f"Total Duration: {global_max_scaled - global_min_scaled:.4f} {units}\n")
    print(f"Offsetting all timestamps by {global_min_scaled:.4f} {units} (time zero)\n")

    task_stats = defaultdict(
        lambda: {
            "count": 0,
            "total_duration": 0,
            "min": float("inf"),
            "max": 0,
            "min_start": float("inf"),
            "max_end": 0,
        }
    )

    for rec in records:
        slot, job_id, start_ns, end_ns, worker, task_id, index = rec
        # Skip system slots and receiver slots
        if slot in system_slots or slot in receiver_slots:
            continue
        duration_ns = end_ns - start_ns
        task_stats[task_id]["count"] += 1
        task_stats[task_id]["total_duration"] += duration_ns
        task_stats[task_id]["min"] = min(task_stats[task_id]["min"], duration_ns)
        task_stats[task_id]["max"] = max(task_stats[task_id]["max"], duration_ns)
        # Apply offset to start/end times for statistics
        task_stats[task_id]["min_start"] = min(
            task_stats[task_id]["min_start"], start_ns - global_min
        )
        task_stats[task_id]["max_end"] = max(
            task_stats[task_id]["max_end"], end_ns - global_min
        )

    # Print statistics for each task_id
    for task_id in sorted(task_stats.keys()):
        stats = task_stats[task_id]
        count = stats["count"]
        total_duration = stats["total_duration"]
        avg_duration = total_duration / count
        min_duration = stats["min"]
        max_duration = stats["max"]
        min_start_time = stats["min_start"]
        max_end_time = stats["max_end"]

        total_scaled = total_duration / scale
        avg_scaled = avg_duration / scale
        min_scaled = min_duration / scale
        max_scaled = max_duration / scale
        min_start_scaled = min_start_time / scale
        max_end_scaled = max_end_time / scale

        task_label = id_map.get(task_id, task_id)
        print(f"Task {task_label}:")
        print(f"  Executions: {count}")
        print(f"  First Start: {min_start_scaled:.4f} {units}")
        print(f"  Last End: {max_end_scaled:.4f} {units}")
        print(f"  Total Time: {total_scaled:.4f} {units}")
        print(f"  Avg/Task: {avg_scaled:.4f} {units}")
        print(f"  Min: {min_scaled:.4f} {units}")
        print(f"  Max: {max_scaled:.4f} {units}")
        print()

    print("=" * 80 + "\n")

    # ========================================================================
    # SCHEDULING LATENCY STATISTICS
    # ========================================================================
    if scheduling_latency_recs:
        print("=" * 80)
        print("Scheduling Latency Statistics")
        print("=" * 80)
        print(f"Total scheduling latency records: {len(scheduling_latency_recs)}\n")

        # Group by original task_id
        latency_stats = defaultdict(
            lambda: {
                "count": 0,
                "total_latency": 0,
                "min": float("inf"),
                "max": 0,
                "latencies": [],
            }
        )

        for sched_rec in scheduling_latency_recs:
            orig_task_id = sched_rec["original_task_id"]
            latency_ns = sched_rec["exec_ns"] - sched_rec["spawn_ns"]

            latency_stats[orig_task_id]["count"] += 1
            latency_stats[orig_task_id]["total_latency"] += latency_ns
            latency_stats[orig_task_id]["min"] = min(
                latency_stats[orig_task_id]["min"], latency_ns
            )
            latency_stats[orig_task_id]["max"] = max(
                latency_stats[orig_task_id]["max"], latency_ns
            )
            latency_stats[orig_task_id]["latencies"].append(latency_ns)

        # Print statistics for each task
        for task_id in sorted(latency_stats.keys()):
            stats = latency_stats[task_id]
            count = stats["count"]
            total_latency = stats["total_latency"]
            avg_latency = total_latency / count
            min_latency = stats["min"]
            max_latency = stats["max"]

            # Calculate median and percentiles
            latencies = sorted(stats["latencies"])
            median_latency = latencies[len(latencies) // 2]
            p95_latency = latencies[int(len(latencies) * 0.95)]
            p99_latency = latencies[int(len(latencies) * 0.99)]

            total_scaled = total_latency / scale
            avg_scaled = avg_latency / scale
            min_scaled = min_latency / scale
            max_scaled = max_latency / scale
            median_scaled = median_latency / scale
            p95_scaled = p95_latency / scale
            p99_scaled = p99_latency / scale

            task_label = id_map.get(task_id, task_id)
            print(f"Task {task_label}:")
            print(f"  Measurements: {count}")
            print(f"  Avg Latency: {avg_scaled:.4f} {units}")
            print(f"  Median: {median_scaled:.4f} {units}")
            print(f"  Min: {min_scaled:.4f} {units}")
            print(f"  Max: {max_scaled:.4f} {units}")
            print(f"  P95: {p95_scaled:.4f} {units}")
            print(f"  P99: {p99_scaled:.4f} {units}")
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

    # overall x limits - start from 0 (since we offset everything) with small margins
    duration = global_max - global_min
    left_margin = duration * 0.01  # 1% left margin
    right_margin = duration * 0.02  # 2% right margin
    x_min = -left_margin / scale  # Slightly negative to give breathing room
    x_max = (duration + right_margin) / scale

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
            elif tid == preparation_task_id:
                unique_keys.add("system-prep")
            elif wait_task_id is not None and tid == wait_task_id:
                unique_keys.add("system-wait")

    # Receiver records
    receiver_workers = set()
    all_receiver_records_flat = []
    for rcv_slot_id, rcv_slot_records in receiver_slots.items():
        for r in rcv_slot_records:
            tid = r[5]
            receiver_workers.add(r[4])
            all_receiver_records_flat.append(r)
            if tid == packet_rx_task_id:
                unique_keys.add("packet-rx")

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
    if "system-wait" in unique_keys:
        list_keys.append("system-wait")

    # 3. Receiver tasks (append if present in unique_keys)
    if "packet-rx" in unique_keys:
        list_keys.append("packet-rx")

    color_map = {}
    for i, k in enumerate(list_keys):
        color_map[k] = cmap[i % len(cmap)]
    # Force system-thread entries (resolution and prep) to use a consistent color
    system_res_color = csscolors["black"]
    system_prep_color = csscolors["peru"]
    packet_rx_color = csscolors["royalblue"]
    system_wait_color = csscolors["red"]
    if "system-res" in color_map:
        color_map["system-res"] = system_res_color
    if "system-prep" in color_map:
        color_map["system-prep"] = system_prep_color
    if "packet-rx" in color_map:
        color_map["packet-rx"] = packet_rx_color
    if "system-wait" in color_map:
        color_map["system-wait"] = system_wait_color

    # Plot worker slots with system thread activities side-by-side
    plot_idx = 0

    # Collect all system records grouped by worker
    prep_tasks_by_worker = defaultdict(list)
    res_tasks_by_worker = defaultdict(list)
    wait_tasks_by_worker = defaultdict(list)
    for sys_slot_id, sys_slot_records in system_slots.items():
        for rec in sys_slot_records:
            worker = rec[4]
            if rec[5] == resolution_task_id:
                res_tasks_by_worker[worker].append(rec)
            elif rec[5] == preparation_task_id:
                prep_tasks_by_worker[worker].append(rec)
            elif wait_task_id is not None and rec[5] == wait_task_id:
                wait_tasks_by_worker[worker].append(rec)

    # Collect all receiver records grouped by worker
    packet_rx_tasks_by_worker = defaultdict(list)
    for rcv_slot_id, rcv_slot_records in receiver_slots.items():
        for rec in rcv_slot_records:
            worker = rec[4]
            if rec[5] == packet_rx_task_id:
                packet_rx_tasks_by_worker[worker].append(rec)

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
            # Apply offset to start/end times
            start = (start_ns - global_min) / scale
            end = (end_ns - global_min) / scale
            width = max(end - start, 1e-6)

            label = id_map.get(task_id, task_id)
            col = color_map.get(label, "gray")
            ax.barh(
                worker,
                width,
                left=start,
                height=0.6,
                align="center",
                color=col,
                zorder=2,
            )

        # Collect all system thread workers and receiver workers
        all_system_workers = set(prep_tasks_by_worker.keys()) | set(
            res_tasks_by_worker.keys()
        )
        all_receiver_workers = set(packet_rx_tasks_by_worker.keys())
        system_workers_in_slot.update(all_system_workers)
        system_workers_in_slot.update(all_receiver_workers)
        workers.update(all_system_workers)
        workers.update(all_receiver_workers)

        # Plot system/receiver threads with overlay bars side by side
        overlay_keys = []
        # Order: resolution (bottom), preparation, idle-wait, packet-rx (top)
        if "system-res" in unique_keys:
            overlay_keys.append("system-res")
        if "system-prep" in unique_keys:
            overlay_keys.append("system-prep")
        if "system-wait" in unique_keys:
            overlay_keys.append("system-wait")
        if "packet-rx" in unique_keys:
            overlay_keys.append("packet-rx")

        num_overlay_types = max(1, len(overlay_keys))
        bar_height = 0.6 / num_overlay_types

        # Helper to plot a given overlay key at an index
        for i, key in enumerate(overlay_keys):
            offset = (i - (num_overlay_types - 1) / 2.0) * bar_height
            if key == "system-res":
                tasks_by_worker = res_tasks_by_worker
                col_key = "system-res"
            elif key == "system-prep":
                tasks_by_worker = prep_tasks_by_worker
                col_key = "system-prep"
            elif key == "system-wait":
                tasks_by_worker = wait_tasks_by_worker
                col_key = "system-wait"
            else:  # packet-rx
                tasks_by_worker = packet_rx_tasks_by_worker
                col_key = "packet-rx"

            for worker in tasks_by_worker.keys():
                for rec in tasks_by_worker[worker]:
                    _, job_id, start_ns, end_ns, _, task_id, index = rec
                    start = (start_ns - global_min) / scale
                    end = (end_ns - global_min) / scale
                    width = max(end - start, 1e-6)
                    col = color_map.get(col_key, "gray")
                    ax.barh(
                        worker + offset,
                        width,
                        left=start,
                        height=bar_height,
                        align="center",
                        color=col,
                        alpha=0.8,
                        zorder=1,
                    )

        # Draw separator lines for system workers
        for sys_worker in system_workers_in_slot:
            ax.axhline(
                y=sys_worker + 0.5,
                color="black",
                linestyle="--",
                linewidth=0.8,
                alpha=0.7,
            )

        # ====================================================================
        # SCHEDULING LATENCY VISUALIZATION: Plot spawn markers with lines
        # ====================================================================
        # Filter scheduling latency records for this slot
        if plot_latency:
            slot_sched_recs = [r for r in scheduling_latency_recs if r["slot"] == slot]

            for sched_rec in slot_sched_recs:
                spawn_time = (sched_rec["spawn_ns"] - global_min) / scale
                exec_time = (sched_rec["exec_ns"] - global_min) / scale
                worker = sched_rec["worker"]
                orig_task_id = sched_rec["original_task_id"]

                # Get color from original task
                label = id_map.get(orig_task_id, orig_task_id)
                col = color_map.get(label, "gray")

                # Draw dashed line from spawn to execution start (scheduling latency gap)
                ax.plot(
                    [spawn_time, exec_time],
                    [worker, worker],
                    color="black",
                    linewidth=1.5,
                    alpha=0.6,
                    zorder=2.5,
                    linestyle="--",
                )

                # Draw marker at spawn time (when task was sent to scheduler)
                ax.plot(
                    spawn_time,
                    worker,
                    marker="o",
                    markersize=5,
                    color=col,
                    markeredgecolor="black",
                    markeredgewidth=0.8,
                    zorder=3,
                )  # Higher zorder to appear on top

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
    num_receiver_threads = len(receiver_workers)
    plt.suptitle(
        f"{title}\n"
        f"Worker Cores: {num_worker_cores} / System Threads: {num_system_threads} / "
        f"Receiver Threads: {num_receiver_threads} / Worker Slots: {num_worker_slots}"
    )

    # add legend
    import matplotlib.patches as mpatches

    legend_handles = []
    # limit legend size to avoid huge legends
    max_legend = 10
    for key in list_keys[:max_legend]:
        patch = mpatches.Patch(color=color_map.get(key, "gray"), label=str(key))
        legend_handles.append(patch)

    # Add scheduling latency marker explanation if we have latency records
    if scheduling_latency_recs and plot_latency:
        import matplotlib.lines as mlines

        # Create a custom legend entry for spawn markers
        spawn_marker = mlines.Line2D(
            [],
            [],
            color="gray",
            marker="o",
            markersize=5,
            markeredgecolor="black",
            markeredgewidth=0.8,
            linestyle="--",
            linewidth=1.5,
            label="Spawn → Exec (sched latency)",
        )
        legend_handles.append(spawn_marker)

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
        "--title", help="Title for the plot", default="Scheduler Visualization"
    )
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
        help="Number of system threads (slots at the end are system thread slots). Use 0 for auto-detect.",
        default=0,
    )
    p.add_argument(
        "--slots",
        type=int,
        help="Number of worker slots (slots 0 to slots-1 are worker slots). Use 0 for auto-detect.",
        default=0,
    )
    p.add_argument(
        "--no-rcv",
        action="store_true",
        help="Use earliest worker task as time zero instead of earliest received packet.",
        default=False,
    )
    p.add_argument(
        "--slot",
        type=int,
        help="Plot only the specified slot (useful when using --record-stream).",
        default=None,
    )

    p.add_argument(
        "--plot-latency",
        action="store_true",
        help="Enable scheduling latency visualization (spawn markers and lines).",
        default=False,
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
        worker_slots_count=args.slots,
        no_rcv=args.no_rcv,
        plot_slot=args.slot,
        plot_latency=args.plot_latency,
    )


if __name__ == "__main__":
    main()
