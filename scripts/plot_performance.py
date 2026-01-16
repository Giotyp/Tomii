#!/usr/bin/env python3
"""
Generate performance visualization plots from timing files.

Usage:
    # Compare multiple configurations
    python3 scripts/plot_performance.py timings/res_flat/timing_*_avg.txt -o plots/comparison.png

    # Single configuration analysis
    python3 scripts/plot_performance.py timings/res_flat/timing_1s_1b_avg.txt -o plots/single.png --single

    # Both modes with sched files
    python3 scripts/plot_performance.py timings/res_flat/timing_*_avg.txt --sched -o plots/combined.png

Args:
    files: Path to timing files (supports wildcards).
    -o, --output: Output image path (default: performance_plots.png).
    --sched: Also process corresponding _sched.txt files for worker metrics.
    --single: Generate single-configuration detailed analysis (only works with one file).
    --baseline: Configuration name to use as baseline for speedup calculation (default: first file).
"""

import argparse
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import numpy as np
from pathlib import Path
from timingfile import TimingFile
from collections import defaultdict
import re


def extract_config_name(filepath):
    """Extract configuration name from filepath (e.g., '1s_1b' from 'timing_1s_1b_avg.txt')."""
    match = re.search(r"timing_(.+?)_avg", str(filepath))
    if match:
        return match.group(1)
    return Path(filepath).stem


def parse_config_name(config_name):
    """Parse config name to extract system threads and batch size.

    Args:
        config_name: String like '1s_1b' or '2s_10b'

    Returns:
        Tuple of (system_threads, batch_size) or (None, None) if parsing fails
    """
    match = re.match(r"(\d+)s_(\d+)b", config_name)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def parse_sched_file(filepath):
    """Parse _sched.txt file to extract worker metrics."""
    workers = {}
    summary = {}

    with open(filepath, "r") as f:
        lines = f.readlines()

    current_worker = None
    in_summary = False

    for line in lines:
        line = line.strip()

        if line.startswith("Worker "):
            worker_id = line.split(":")[0].replace("Worker ", "").strip()
            current_worker = worker_id
            workers[current_worker] = {}
        elif line.startswith("Tasks executed:"):
            if current_worker:
                workers[current_worker]["total_tasks"] = int(line.split(":")[1].strip())
        elif line.startswith("Busy time:"):
            if current_worker:
                # Format: "Busy time:                 2003.80 µs  [ 62.97%]"
                match = re.search(r"([\d.]+)\s+(\S+)\s+\[\s*([\d.]+)%\]", line)
                if match:
                    workers[current_worker]["busy_time"] = float(match.group(1))
                    workers[current_worker]["busy_unit"] = match.group(2)
                    workers[current_worker]["utilization"] = float(match.group(3))
        elif line.startswith("Idle time:"):
            if current_worker:
                # Format: "Idle time:                 1178.39 µs  [ 37.03%]"
                match = re.search(r"([\d.]+)\s+(\S+)", line)
                if match:
                    workers[current_worker]["idle_time"] = float(match.group(1))
                    workers[current_worker]["idle_unit"] = match.group(2)
        elif "SUMMARY" in line:
            in_summary = True
            current_worker = None
        elif in_summary and ":" in line:
            parts = line.split(":", 1)
            key = parts[0].strip()
            value_str = parts[1].strip()

            if "Number of workers" in key:
                summary["num_workers"] = int(value_str)
            elif "Total execution span" in key:
                match = re.search(r"([\d.]+)\s+(\S+)", value_str)
                if match:
                    summary["execution_span"] = (float(match.group(1)), match.group(2))
            elif "Total busy time (all)" in key:
                match = re.search(r"([\d.]+)\s+(\S+)", value_str)
                if match:
                    summary["total_busy"] = (float(match.group(1)), match.group(2))
            elif "Total idle time (all)" in key:
                match = re.search(r"([\d.]+)\s+(\S+)", value_str)
                if match:
                    summary["total_idle"] = (float(match.group(1)), match.group(2))
            elif "Average busy time per worker" in key:
                match = re.search(r"([\d.]+)\s+(\S+)\s+\[\s*([\d.]+)%\]", value_str)
                if match:
                    summary["avg_busy"] = (float(match.group(1)), match.group(2))
                    summary["avg_utilization"] = float(match.group(3))

    return workers, summary


def plot_comparison_graphs(timing_files, sched_files, output_path, baseline_name):
    """Generate comparison plots across multiple configurations."""

    # Parse all timing files
    configs = {}
    for filepath in timing_files:
        config_name = extract_config_name(filepath)
        tf = TimingFile()
        tf.parse(filepath)
        configs[config_name] = {"timing": tf}

    # Parse sched files if available
    for filepath in sched_files:
        config_name = extract_config_name(filepath)
        if config_name in configs:
            workers, summary = parse_sched_file(filepath)
            configs[config_name]["sched_workers"] = workers
            configs[config_name]["sched_summary"] = summary

    config_names = sorted(configs.keys())
    n_configs = len(config_names)

    # Determine baseline for speedup calculation
    if baseline_name and baseline_name in configs:
        baseline = configs[baseline_name]["timing"]
    else:
        baseline = configs[config_names[0]]["timing"]

    baseline_runtime = baseline.total_runtime[0]  # in original units
    baseline_unit = baseline.total_runtime[1]

    # Create figure with subplots
    fig = plt.figure(figsize=(16, 12))
    gs = GridSpec(3, 3, figure=fig, hspace=0.3, wspace=0.3)

    # Plot 1: Runtime Comparison (Total vs Compute)
    ax1 = fig.add_subplot(gs[0, 0])
    x = np.arange(n_configs)
    width = 0.35

    total_runtimes = []
    compute_times = []

    for name in config_names:
        tf = configs[name]["timing"]
        # Convert to ms for consistency
        total_ms = (
            tf.total_runtime[0]
            if tf.total_runtime[1] == "ms"
            else tf.total_runtime[0] / 1000
        )
        compute_ms = (
            tf.total_compute_time[0]
            if tf.total_compute_time[1] == "ms"
            else tf.total_compute_time[0] / 1000
        )
        total_runtimes.append(total_ms)
        compute_times.append(compute_ms)

    ax1.bar(
        x - width / 2, total_runtimes, width, label="Total Runtime", color="steelblue"
    )
    ax1.bar(x + width / 2, compute_times, width, label="Compute Time", color="coral")
    ax1.set_xlabel("Configuration")
    ax1.set_ylabel("Time (ms)")
    ax1.set_title("Total Runtime vs Compute Time")
    ax1.set_xticks(x)
    ax1.set_xticklabels(config_names, rotation=45, ha="right")
    ax1.legend()
    ax1.grid(axis="y", alpha=0.3)

    # Plot 2: Speedup Factor
    ax2 = fig.add_subplot(gs[0, 1])
    speedups = [baseline_runtime / total_runtimes[i] for i in range(n_configs)]
    colors = ["green" if s >= 1.0 else "red" for s in speedups]

    ax2.bar(x, speedups, color=colors, alpha=0.7)
    ax2.axhline(y=1.0, color="black", linestyle="--", linewidth=1, label="Baseline")
    ax2.set_xlabel("Configuration")
    ax2.set_ylabel("Speedup Factor")
    ax2.set_title(f"Speedup vs Baseline ({config_names[0]})")
    ax2.set_xticks(x)
    ax2.set_xticklabels(config_names, rotation=45, ha="right")
    ax2.legend()
    ax2.grid(axis="y", alpha=0.3)

    # Plot 3: Worker Efficiency (from sched files)
    if any("sched_summary" in configs[name] for name in config_names):
        ax3 = fig.add_subplot(gs[0, 2])

        utilizations = []
        for name in config_names:
            if "sched_summary" in configs[name]:
                utilizations.append(
                    configs[name]["sched_summary"].get("avg_utilization", 0)
                )
            else:
                utilizations.append(0)

        bars = ax3.bar(x, utilizations, color="mediumseagreen", alpha=0.7)
        ax3.set_xlabel("Configuration")
        ax3.set_ylabel("Utilization (%)")
        ax3.set_title("Average Worker Utilization")
        ax3.set_xticks(x)
        ax3.set_xticklabels(config_names, rotation=45, ha="right")
        ax3.set_ylim(0, 100)
        ax3.grid(axis="y", alpha=0.3)

        # Add percentage labels on bars
        for bar, util in zip(bars, utilizations):
            height = bar.get_height()
            ax3.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"{util:.1f}%",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    # Plot 4: Task Execution Time Breakdown
    ax4 = fig.add_subplot(gs[1, :])

    # Collect all task names and their avg times across configs
    all_tasks = set()
    for name in config_names:
        tf = configs[name]["timing"]
        all_tasks.update([task["name"] for task in tf.tasks])

    # Filter out argbuild tasks for cleaner visualization
    main_tasks = sorted([t for t in all_tasks if "-argbuild" not in t])

    if main_tasks:
        x_pos = np.arange(len(main_tasks))
        width = 0.8 / n_configs

        for i, name in enumerate(config_names):
            tf = configs[name]["timing"]
            task_times = []
            for task_name in main_tasks:
                # Find task by name
                task = next((t for t in tf.tasks if t["name"] == task_name), None)
                if task:
                    # Convert avg_per_task to microseconds
                    avg_val = task["timing"]["avg_task"][0]
                    avg_unit = task["timing"]["avg_task"][1]
                    if avg_unit == "ns":
                        avg_us = avg_val / 1000
                    elif avg_unit == "ms":
                        avg_us = avg_val * 1000
                    elif avg_unit == "s":
                        avg_us = avg_val * 1000000
                    else:  # us
                        avg_us = avg_val
                    task_times.append(avg_us)
                else:
                    task_times.append(0)

            offset = (i - n_configs / 2 + 0.5) * width
            ax4.bar(x_pos + offset, task_times, width, label=name, alpha=0.8)

        ax4.set_xlabel("Task")
        ax4.set_ylabel("Avg Time per Task (µs)")
        ax4.set_title("Task Execution Time Comparison (Most Important Metric)")
        ax4.set_xticks(x_pos)
        ax4.set_xticklabels(main_tasks, rotation=45, ha="right")
        ax4.legend(loc="upper left")
        ax4.grid(axis="y", alpha=0.3)

    # Plot 5: Worker Load Balance (from timing files)
    ax5 = fig.add_subplot(gs[2, 0])

    # For the first config, show worker load distribution
    if config_names:
        first_config = config_names[0]
        tf = configs[first_config]["timing"]

        worker_loads = defaultdict(int)
        for task in tf.tasks:
            if "worker_summary" in task:
                for worker_id, summary in task["worker_summary"].items():
                    worker_loads[worker_id] += summary["count"]

        if worker_loads:
            workers = sorted(worker_loads.keys())
            counts = [worker_loads[w] for w in workers]

            ax5.bar(range(len(workers)), counts, color="skyblue", alpha=0.7)
            ax5.set_xlabel("Worker")
            ax5.set_ylabel("Total Tasks Executed")
            ax5.set_title(f"Worker Load Balance ({first_config})")
            ax5.set_xticks(range(len(workers)))
            ax5.set_xticklabels(workers, rotation=45, ha="right")
            ax5.grid(axis="y", alpha=0.3)

            # Add count labels
            for i, count in enumerate(counts):
                ax5.text(i, count, str(count), ha="center", va="bottom", fontsize=6)

    # Plot 6: Busy/Idle Time Distribution (from sched files)
    if any("sched_workers" in configs[name] for name in config_names):
        ax6 = fig.add_subplot(gs[2, 1:])

        # Show stacked bar for first config with sched data
        for name in config_names:
            if "sched_workers" in configs[name]:
                workers_data = configs[name]["sched_workers"]
                worker_ids = sorted(workers_data.keys())

                busy_times = [workers_data[w]["busy_time"] for w in worker_ids]
                idle_times = [workers_data[w]["idle_time"] for w in worker_ids]

                x_pos = np.arange(len(worker_ids))

                ax6.bar(x_pos, busy_times, label="Busy Time", color="green", alpha=0.7)
                ax6.bar(
                    x_pos,
                    idle_times,
                    bottom=busy_times,
                    label="Idle Time",
                    color="red",
                    alpha=0.7,
                )

                ax6.set_xlabel("Worker")
                ax6.set_ylabel("Time (µs)")
                ax6.set_title(f"Worker Busy/Idle Time Distribution ({name})")
                ax6.set_xticks(x_pos)
                ax6.set_xticklabels(worker_ids, rotation=45, ha="right")
                ax6.legend()
                ax6.grid(axis="y", alpha=0.3)

                # Add utilization percentage on bars
                for i, (worker, busy, idle) in enumerate(
                    zip(worker_ids, busy_times, idle_times)
                ):
                    total = busy + idle
                    util = (busy / total * 100) if total > 0 else 0
                    ax6.text(
                        i,
                        busy / 2,
                        f"{util:.1f}%",
                        ha="center",
                        va="center",
                        fontsize=9,
                        fontweight="bold",
                        color="white",
                    )

                break  # Only show first config with sched data

    plt.suptitle(
        "Performance Comparison Across Configurations", fontsize=16, fontweight="bold"
    )
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Comparison plots saved to: {output_path}")


def plot_single_configuration(timing_file, sched_file, output_path):
    """Generate detailed analysis plots for a single configuration."""

    # Parse timing file
    tf = TimingFile()
    tf.parse(timing_file)
    config_name = extract_config_name(timing_file)

    # Parse config name for title
    sys_threads, batch_size = parse_config_name(config_name)

    # Parse sched file if available
    workers_data = None
    sched_summary = None
    if sched_file and Path(sched_file).exists():
        workers_data, sched_summary = parse_sched_file(sched_file)
        if sched_summary and "num_workers" in sched_summary:
            num_workers = sched_summary["num_workers"]

    # Create figure with more space for labels
    fig = plt.figure(figsize=(30, 11))
    gs = GridSpec(3, 5, figure=fig, hspace=0.40, wspace=0.35)

    # Plot 1: Task Average Execution Time (MOST IMPORTANT)
    ax1 = fig.add_subplot(gs[0, :])

    main_tasks = sorted([t["name"] for t in tf.tasks if "-argbuild" not in t["name"]])

    # Collect system thread data
    system_thread_labels = []
    system_thread_prep_times = []
    system_thread_res_times = []

    if hasattr(tf, "system_threads") and tf.system_threads:
        for sys_thread in tf.system_threads:
            thread_id = sys_thread["thread_id"]
            prep_time = 0
            res_time = 0

            # Find preparation and resolution tasks
            for task in sys_thread["tasks"]:
                avg_val = task["avg"][0]
                unit = task["avg"][1]

                # Convert to microseconds
                if unit == "ns":
                    factor = 1 / 1000
                elif unit == "ms":
                    factor = 1000
                elif unit == "s":
                    factor = 1000000
                else:
                    factor = 1

                avg_us = avg_val * factor

                if "Preparation" in task["name"]:
                    prep_time = avg_us
                elif "Resolution" in task["name"]:
                    res_time = avg_us

            system_thread_labels.append(f"Sys-{thread_id}")
            system_thread_prep_times.append(prep_time)
            system_thread_res_times.append(res_time)

    # Combine main tasks and system threads
    all_labels = main_tasks + system_thread_labels
    num_main_tasks = len(main_tasks)
    num_total = len(all_labels)

    if main_tasks or system_thread_labels:
        task_times = []
        task_mins = []
        task_maxs = []
        task_totals = []  # Store total times for labels

        # Process main tasks
        for task_name in main_tasks:
            task = next((t for t in tf.tasks if t["name"] == task_name), None)
            if not task:
                continue
            # Convert to microseconds
            avg_val = task["timing"]["avg_task"][0]
            min_val = task["timing"]["min"][0]
            max_val = task["timing"]["max"][0]
            total_val = task["timing"]["total"][0]
            unit = task["timing"]["avg_task"][1]
            total_unit = task["timing"]["total"][1]

            if unit == "ns":
                factor = 1 / 1000
            elif unit == "ms":
                factor = 1000
            elif unit == "s":
                factor = 1000000
            else:
                factor = 1

            # Convert total to microseconds for consistent display
            if total_unit == "ns":
                total_factor = 1 / 1000
            elif total_unit == "ms":
                total_factor = 1000
            elif total_unit == "s":
                total_factor = 1000000
            else:
                total_factor = 1

            task_times.append(avg_val * factor)
            task_mins.append(min_val * factor)
            task_maxs.append(max_val * factor)
            task_totals.append((total_val * total_factor, total_unit))

        x = np.arange(num_total)

        # Plot main task bars
        if main_tasks:
            bars = ax1.bar(
                x[:num_main_tasks],
                task_times,
                color="steelblue",
                alpha=0.8,
                label="Worker Tasks",
            )

            # Add error bars showing min/max range (ensure non-negative)
            yerr_lower = [
                max(0, avg - min_val) for avg, min_val in zip(task_times, task_mins)
            ]
            yerr_upper = [
                max(0, max_val - avg) for avg, max_val in zip(task_times, task_maxs)
            ]

            # Only add error bars if there are valid values
            if any(yerr_lower) or any(yerr_upper):
                ax1.errorbar(
                    x[:num_main_tasks],
                    task_times,
                    yerr=[yerr_lower, yerr_upper],
                    fmt="none",
                    ecolor="red",
                    capsize=5,
                    alpha=0.5,
                )

            # Add value labels on main task bars
            for bar, time, (total_val_us, _) in zip(bars, task_times, task_totals):
                height = bar.get_height()
                # total_val_us is already in microseconds, format appropriately
                if total_val_us >= 1000000:
                    total_str = f"{total_val_us/1000000:.2f}s"
                elif total_val_us >= 1000:
                    total_str = f"{total_val_us/1000:.2f}ms"
                else:
                    total_str = f"{total_val_us:.2f}µs"

                # Use dynamic offset based on max height for better visibility
                y_offset = max(height * 0.02, height * 0.02) if height > 0 else 0
                ax1.text(
                    bar.get_x() + bar.get_width() / 2.0,
                    height + y_offset,
                    f"{time:.2f}µs ({total_str})",
                    ha="center",
                    va="bottom",
                    fontsize=7,
                )

        # Plot system thread bars (side-by-side: preparation and resolution)
        if system_thread_labels:
            x_sys = x[num_main_tasks:]

            # Collect total times with units for system threads (convert to µs)
            system_thread_totals = []
            if hasattr(tf, "system_threads") and tf.system_threads:
                for sys_thread in tf.system_threads:
                    prep_total_us = None
                    res_total_us = None

                    for task in sys_thread["tasks"]:
                        if "total" in task:
                            total_val = task["total"][0]
                            total_unit = task["total"][1]

                            # Convert to microseconds
                            if total_unit == "ns":
                                total_us = total_val / 1000
                            elif total_unit == "ms":
                                total_us = total_val * 1000
                            elif total_unit == "s":
                                total_us = total_val * 1000000
                            else:
                                total_us = total_val

                            if "Preparation" in task["name"]:
                                prep_total_us = total_us
                            elif "Resolution" in task["name"]:
                                res_total_us = total_us

                    system_thread_totals.append((prep_total_us, res_total_us))

            # Create side-by-side bars with narrow width
            bar_width = 0.35

            # Preparation bars (left)
            prep_bars = ax1.bar(
                x_sys - bar_width / 2,
                system_thread_prep_times,
                bar_width,
                color="gold",
                alpha=0.9,
                label="Preparation Thread",
            )

            # Resolution bars (right)
            res_bars = ax1.bar(
                x_sys + bar_width / 2,
                system_thread_res_times,
                bar_width,
                color="coral",
                alpha=0.8,
                label="Resolution Thread (bold)",
            )

            # Add value labels on system thread bars
            for i, (prep_time, res_time) in enumerate(
                zip(system_thread_prep_times, system_thread_res_times)
            ):
                x_pos = x_sys[i]

                if i < len(system_thread_totals):
                    prep_total_us, res_total_us = system_thread_totals[i]

                    # Label for preparation bar
                    if prep_time > 0 and prep_total_us is not None:
                        # Format total appropriately
                        if prep_total_us >= 1000:
                            total_str = f"{prep_total_us/1000:.2f}ms"
                        else:
                            total_str = f"{prep_total_us:.2f}µs"

                        prep_label = f"{prep_time:.2f}µs ({total_str})"
                        y_offset = max(prep_time * 0.02, 10)
                        ax1.text(
                            x_pos - bar_width / 2,
                            prep_time + y_offset,
                            prep_label,
                            ha="center",
                            va="bottom",
                            fontsize=6,
                        )
                    elif prep_time > 0:
                        y_offset = max(prep_time * 0.02, 10)
                        ax1.text(
                            x_pos - bar_width / 2,
                            prep_time + y_offset,
                            f"{prep_time:.2f}µs",
                            ha="center",
                            va="bottom",
                            fontsize=6,
                        )

                    # Label for resolution bar
                    if res_time > 0 and res_total_us is not None:
                        # Format total appropriately
                        if res_total_us >= 1000:
                            total_str = f"{res_total_us/1000:.2f}ms"
                        else:
                            total_str = f"{res_total_us:.2f}µs"

                        res_label = f"{res_time:.2f}µs ({total_str})"
                        y_offset = max(res_time * 0.02, 10) * 3
                        ax1.text(
                            x_pos + bar_width / 2,
                            res_time + y_offset,
                            res_label,
                            ha="center",
                            va="bottom",
                            fontsize=6,
                            fontweight="bold",
                        )
                    elif res_time > 0:
                        y_offset = max(res_time * 0.02, 10) * 3
                        ax1.text(
                            x_pos + bar_width / 2,
                            res_time + y_offset,
                            f"{res_time:.2f}µs",
                            ha="center",
                            va="bottom",
                            fontsize=6,
                            fontweight="bold",
                        )

        ax1.set_xlabel("Task / System Thread", fontsize=11)
        ax1.set_ylabel("Average Time per Task (µs)", fontsize=11)
        ax1.set_title("Task Execution Time", fontsize=13, fontweight="bold")
        ax1.set_xticks(x)
        ax1.set_xticklabels(all_labels, rotation=0, ha="center")
        ax1.legend(loc="upper left", fontsize=9)
        ax1.grid(axis="y", alpha=0.3)
        # Add some space at the top for labels
        ymin, ymax = ax1.get_ylim()
        ax1.set_ylim(ymin, ymax * 1.15)

    # Plot 2: Worker Load Balance
    ax2 = fig.add_subplot(gs[1, :2])

    worker_loads = defaultdict(int)
    for task in tf.tasks:
        if "worker_summary" in task:
            for worker_id, summary in task["worker_summary"].items():
                worker_loads[worker_id] += summary["count"]

    if worker_loads:
        workers = sorted(worker_loads.keys())
        counts = [worker_loads[w] for w in workers]
        total_tasks = sum(counts)

        colors = plt.cm.viridis(np.linspace(0, 0.8, len(workers)))
        bars = ax2.bar(range(len(workers)), counts, color=colors, alpha=0.8)

        ax2.set_xlabel("Worker", fontsize=10)
        ax2.set_ylabel("Tasks Executed", fontsize=10)
        ax2.set_title("Worker Load Balance", fontsize=12, fontweight="bold")
        ax2.set_xticks(range(len(workers)))
        ax2.set_xticklabels([f"W{w}" for w in workers], rotation=0)
        ax2.grid(axis="y", alpha=0.3)

        # Add percentage labels with alternating positions
        for i, (bar, count) in enumerate(zip(bars, counts)):
            pct = (count / total_tasks * 100) if total_tasks > 0 else 0
            # Alternate between center and slightly off-center higher positioning
            if i % 2 == 0:
                y_pos = count / 2  # center
            else:
                y_pos = count * 0.7  # slightly above center

            ax2.text(
                bar.get_x() + bar.get_width() / 2.0,
                y_pos,
                f"{count}\n({pct:.1f}%)",
                ha="center",
                va="center",
                fontsize=9,
            )

    # Plot 3: Worker Busy/Idle Time (from sched)
    if workers_data and num_workers > 0:
        ax3 = fig.add_subplot(gs[1, 2:])

        # Filter to only include actual worker threads (not system threads)
        # System threads are cores 0, 1, 2, ... (sys_threads - 1)
        # Worker threads start at core sys_threads and onwards
        all_worker_ids = sorted([int(w) for w in workers_data.keys()])

        # Determine number of system threads from config
        if sys_threads is not None and sys_threads > 0:
            # Filter out system thread IDs (0 to sys_threads-1)
            worker_ids = [w for w in all_worker_ids if w >= sys_threads]
            # Only keep the first num_workers after filtering
            worker_ids = worker_ids[:num_workers]
        else:
            # Fallback: assume last num_workers entries are workers
            if len(all_worker_ids) > num_workers:
                worker_ids = all_worker_ids[-num_workers:]
            else:
                worker_ids = all_worker_ids

        # Convert back to strings for dictionary lookup
        worker_ids_str = [str(w) for w in worker_ids]

        busy_times = [workers_data[w]["busy_time"] for w in worker_ids_str]
        idle_times = [workers_data[w]["idle_time"] for w in worker_ids_str]

        x = np.arange(len(worker_ids))
        width = 0.6

        bars1 = ax3.bar(
            x, busy_times, width, label="Busy Time", color="green", alpha=0.7
        )
        bars2 = ax3.bar(
            x,
            idle_times,
            width,
            bottom=busy_times,
            label="Idle Time",
            color="red",
            alpha=0.7,
        )

        ax3.set_xlabel("Worker", fontsize=10)
        ax3.set_ylabel("Time (µs)", fontsize=10)
        ax3.set_title(
            "Worker Time Distribution (Scheduler Efficiency)",
            fontsize=12,
            fontweight="bold",
        )
        ax3.set_xticks(x)
        # Create labels with format W-0(core_id), W-1(core_id), etc.
        # Worker index is 0, 1, ... and core_id is the actual core
        worker_labels = [f"W{i}({core_id})" for i, core_id in enumerate(worker_ids)]
        ax3.set_xticklabels(worker_labels, rotation=0)
        ax3.legend(fontsize=9)
        ax3.grid(axis="y", alpha=0.3)

        # Add utilization percentage with alternating positions
        for i, (core_id, busy, idle) in enumerate(
            zip(worker_ids, busy_times, idle_times)
        ):
            total = busy + idle
            util = (busy / total * 100) if total > 0 else 0
            # Alternate between center and slightly off-center higher positioning
            if i % 2 == 0:
                y_pos = busy / 2  # center
            else:
                y_pos = busy * 0.7  # slightly above center

            ax3.text(
                i,
                y_pos,
                f"{util:.1f}%",
                ha="center",
                va="center",
                fontsize=10,
                fontweight="bold",
                color="black",
            )

    # Plot 4: Task Distribution Per Worker
    ax4 = fig.add_subplot(gs[2, :3])

    if main_tasks:
        worker_task_counts = defaultdict(lambda: defaultdict(int))

        for task_name in main_tasks:
            task = next((t for t in tf.tasks if t["name"] == task_name), None)
            if task and "worker_summary" in task:
                for worker_id, summary in task["worker_summary"].items():
                    worker_task_counts[worker_id][task_name] = summary["count"]

        workers = sorted(worker_task_counts.keys())

        # Create stacked bar chart
        x = np.arange(len(workers))
        width = 0.6
        bottom = np.zeros(len(workers))

        colors = plt.cm.Set3(np.linspace(0, 1, len(main_tasks)))

        for task_idx, task_name in enumerate(main_tasks):
            counts = [worker_task_counts[w][task_name] for w in workers]
            ax4.bar(
                x,
                counts,
                width,
                bottom=bottom,
                label=task_name,
                color=colors[task_idx],
                alpha=0.8,
            )
            bottom += counts

        ax4.set_xlabel("Worker", fontsize=10)
        ax4.set_ylabel("Task Count", fontsize=10)
        ax4.set_title("Task Distribution Per Worker", fontsize=12, fontweight="bold")
        ax4.set_xticks(x)
        ax4.set_xticklabels([f"W-{w}" for w in workers], rotation=0)
        ax4.legend(loc="upper left", bbox_to_anchor=(1, 1), fontsize=8)
        ax4.grid(axis="y", alpha=0.3)

    # Plot 5: Summary Statistics
    ax5 = fig.add_subplot(gs[2, 3:])
    ax5.axis("off")

    summary_text = f"Configuration: {config_name}\n"
    summary_text += "=" * 40 + "\n\n"
    summary_text += f"Total Streams: {tf.total_streams}\n"
    summary_text += f"Total Runtime: {tf.total_runtime[0]:.4f} {tf.total_runtime[1]}\n"
    summary_text += (
        f"Compute Time: {tf.total_compute_time[0]:.4f} {tf.total_compute_time[1]}\n"
    )
    summary_text += (
        f"Avg/Stream: {tf.avg_time_per_stream[0]:.4f} {tf.avg_time_per_stream[1]}\n\n"
    )

    if sched_summary:
        summary_text += "Scheduler Metrics:\n"
        summary_text += "-" * 40 + "\n"
        summary_text += f"Workers: {sched_summary['num_workers']}\n"
        if "execution_span" in sched_summary:
            summary_text += f"Exec Span: {sched_summary['execution_span'][0]:.2f} {sched_summary['execution_span'][1]}\n"
        if "avg_utilization" in sched_summary:
            summary_text += (
                f"Avg Utilization: {sched_summary['avg_utilization']:.2f}%\n"
            )
        if "total_busy" in sched_summary:
            summary_text += f"Total Busy: {sched_summary['total_busy'][0]:.2f} {sched_summary['total_busy'][1]}\n"
        if "total_idle" in sched_summary:
            summary_text += f"Total Idle: {sched_summary['total_idle'][0]:.2f} {sched_summary['total_idle'][1]}\n"

    ax5.text(
        0.1,
        0.95,
        summary_text,
        transform=ax5.transAxes,
        fontsize=10,
        verticalalignment="top",
        family="monospace",
        bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.3),
    )

    # Create descriptive title
    if sys_threads is not None and batch_size is not None:
        title = f"Detailed Performance Analysis for System Thread(s): {sys_threads} and Receive Batch Size: {batch_size}"
    else:
        title = f"Detailed Performance Analysis: {config_name}"

    plt.suptitle(title, fontsize=16, fontweight="bold")
    plt.savefig(output_path, dpi=300, bbox_inches="tight")
    print(f"Single configuration plots saved to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate performance visualization plots from timing files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare multiple configurations
  python3 scripts/plot_performance.py timings/res_flat/timing_*_avg.txt -o plots/comparison.png
  
  # Single configuration detailed analysis
  python3 scripts/plot_performance.py timings/res_flat/timing_1s_1b_avg.txt -o plots/single.png --single
  
  # Both modes with scheduler data
  python3 scripts/plot_performance.py timings/res_flat/timing_*_avg.txt --sched -o plots/full.png
        """,
    )
    parser.add_argument(
        "files", nargs="+", help="Timing files to process (supports wildcards)"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="performance_plots.png",
        help="Output image path (default: performance_plots.png)",
    )
    parser.add_argument(
        "--sched",
        action="store_true",
        help="Also process corresponding _sched.txt files",
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Generate single-configuration detailed analysis (requires one file)",
    )
    parser.add_argument(
        "--baseline", help="Configuration name to use as baseline for speedup"
    )

    args = parser.parse_args()

    # Separate timing and sched files
    timing_files = [f for f in args.files if "_sched.txt" not in f]
    sched_files = []

    if args.sched:
        # Find corresponding sched files
        for tf in timing_files:
            sched_path = str(tf).replace("_avg.txt", "_avg_sched.txt")
            if Path(sched_path).exists():
                sched_files.append(sched_path)

    if not timing_files:
        print("Error: No timing files found")
        return 1

    if args.single:
        if len(timing_files) > 1:
            print(
                "Warning: --single mode requires exactly one file. Using first file only."
            )

        sched_file = None
        if sched_files:
            sched_file = sched_files[0]

        plot_single_configuration(timing_files[0], sched_file, args.output)
    else:
        plot_comparison_graphs(timing_files, sched_files, args.output, args.baseline)

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
