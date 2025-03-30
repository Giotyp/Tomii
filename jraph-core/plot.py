import re
import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from collections import defaultdict

def violin_worker_times(results_directory, file_name, keep_runs, bench_name, save_file, num_tasks=4):

    # Regular expression patterns for different time measurements
    task_pattern = re.compile(r"Task:\s+(.+)")
    run_pattern = re.compile(r"Run:\s+(\d+)")
    res_pat = r"Worker-(\d+) -> \[([\d.msµn, ]+)\]"
    worker_pattern = re.compile(res_pat)

    units = {'ms': 1e+3, 'µs': 1, 'ns': 1e-3} 

    results = defaultdict(lambda: defaultdict(list))

    with open(f"{results_directory}/{file_name}") as f:
        lines = f.readlines()

        current_task = None
        current_run = None

        for line in lines:
            task_match = task_pattern.match(line)
            run_match = run_pattern.match(line)
            worker_match = worker_pattern.match(line)
            
            if task_match:
                current_task = task_match.group(1).strip()
                current_run = None
                continue
            elif run_match:
                current_run = int(run_match.group(1))
                continue
            
            elif worker_match:

                if current_run not in keep_runs:
                    continue

                worker_id = int(worker_match.group(1))

                times = worker_match.group(2).split(',')

                # convert all times to microseconds
                times = [float(time[:-2]) * units[time[-2:]] for time in times]
                # round to 6 digits
                times = [round(time, 6) for time in times]
                
                # Store average times for each task and run
                for time in times:
                    results[current_task][current_run].append(time)
            else:
                continue
        
    if num_tasks == 4:
        # 4 tasks total - 2 x 2 grid
        fig, axs = plt.subplots(nrows=2, ncols=2, figsize=(8, 8))
        fig.subplots_adjust(hspace=0.3, wspace=0.5)
    else: # num_tasks = 5
        # 5 tasks total - 3 x 2 grid
        fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(8, 12))
        fig.subplots_adjust(hspace=0.3, wspace=0.5)
        # Turn off the unused subplot at position (2, 1)
        axs[2, 1].axis("off")

    # Set fig title
    fig.suptitle(f"Per Task Worker Times for {bench_name}")

    for i, task in enumerate(results):

        # get run results in list of lists
        task_res = [results[task][run] for run in results[task]]
        runs = len(task_res)

        positions = np.arange(1, runs + 1)

        row, col = divmod(i, 2)
        # create a violin plot for data in results[current_taks]
        axs[row, col].violinplot(task_res, showmeans=True, showmedians=False, positions=positions)

        # set title
        axs[row, col].set_title(f"Task: {task}")
        axs[row, col].set_xlabel("Run")
        axs[row, col].set_ylabel("Time (μs)")
        # set xtixks
        axs[row, col].set_xticks(positions)
        # set xtick labels
        axs[row, col].set_xticklabels([str(run+1) for run in keep_runs])

    plt.savefig(save_file)

    named_res = {bench_name: results}
    return named_res


def add_label(violin, label):
    color = violin["bodies"][0].get_facecolor().flatten()
    return (mpatches.Patch(color=color), label)

def combine_violins(manual_res, jraph_res, keep_runs, save_file):

    for idx, (manual_dict, jraph_dict) in enumerate(zip(manual_res, jraph_res)):

        name_man = list(manual_dict.keys())[0]
        manual = manual_dict[name_man]
        name_jr = list(jraph_dict.keys())[0]
        jraph = jraph_dict[name_jr]

        if name_man != name_jr:
            print("Benchmarks do not match!")
            continue
       
       # 5 tasks total - 3 x 2 grid
        fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(8, 12))
        fig.subplots_adjust(hspace=0.3, wspace=0.5)
        axs[2, 1].axis("off")

        # Set fig title
        fig.suptitle(f"Per Task Worker Times for Bench-{idx+1}")

        for i, task in enumerate(jraph):

            labels = []
            # get run results in list of lists
            jr_task_res = [jraph[task][run] for run in jraph[task]]

            if task in manual:
                man_task_res = [manual[task][run] for run in manual[task]]
            else: 
                man_task_res = None
            
            runs = len(jr_task_res)

            positions = np.arange(1, runs + 1)

            row, col = divmod(i, 2)
            # create a violin plot for data in jraph results
            meanprops = {'color': 'blue', 'linewidth': 2.5} # set mean line properties
            jr_violin = axs[row, col].violinplot(jr_task_res, showmeans=True, showmedians=False, positions=positions)
            jr_violin['cmeans'].set_color(meanprops['color'])
            jr_violin['cmeans'].set_linewidth(meanprops['linewidth'])
            labels.append(add_label(jr_violin, "JRaph"))

            if man_task_res is not None:
                meanprops = {'color': 'orange', 'linewidth': 2.5} # set mean line properties
                man_violin = axs[row, col].violinplot(man_task_res, showmeans=True, showmedians=False, positions=positions)
                man_violin['cmeans'].set_color(meanprops['color'])
                man_violin['cmeans'].set_linewidth(meanprops['linewidth'])
                labels.append(add_label(man_violin, "Manual"))

            # set title
            axs[row, col].set_title(f"Task: {task}")
            axs[row, col].set_xlabel("Run")
            axs[row, col].set_ylabel("Time (μs)")
            # set xtixks
            axs[row, col].set_xticks(positions)
            # set xtick labels
            axs[row, col].set_xticklabels([str(run+1) for run in keep_runs])
            # set legend
            axs[row, col].legend(*zip(*labels))

        # save figure
        save = f"{save_file}_{name_man}.png"
        plt.savefig(save)



if __name__ == "__main__":

    results_directory = "examples/manualRayon/results"
    file_names = ["worker_raw_man_Bench-1.txt", "worker_raw_man_Bench-2.txt", "worker_raw_man_Bench-3.txt"]

    manual_res = []

    for filename in file_names:
        bench_name = filename.split("_")[3].split(".")[0]
        savefile = f"{results_directory}/{bench_name}_man_wviolin.png"
        keep_runs = [1, 14, 24, 34, 49, 64, 74, 99]

        res = violin_worker_times(results_directory, filename, keep_runs, bench_name, savefile)
        manual_res.append(res)

    results_directory = "examples/benchmarks/results"
    file_names = ["worker_raw_JRaph_Bench-1.txt", "worker_raw_JRaph_Bench-2.txt", "worker_raw_JRaph_Bench-3.txt"]

    jraph_res = []
    for filename in file_names:
        bench_name = filename.split("_")[3].split(".")[0]
        savefile = f"{results_directory}/{bench_name}_jr_wviolin.png"
        keep_runs = [1, 14, 24, 34, 49, 64, 74, 99]

        res = violin_worker_times(results_directory, filename, keep_runs, bench_name, savefile, num_tasks=5)
        jraph_res.append(res)

    combine_violins(manual_res, jraph_res, keep_runs, "examples/benchmarks/results/combined")