import numpy as np
import argparse
import matplotlib.pyplot as plt
from timingfile import TimingFile
from utils import convert_to_ns, convert_from_ns


def parse_runtime(file_paths):
    """
    Parses a list of timing files in the format:
    'timing_xs_yb_z.txt' where x is the system threads, y is the batching size, and z is the repeat number.
    Uses TimingFile to parse timing files and extract total runtime.
    Returns a dictionary of the form:
    {
        (x, y): [f1, f2, ...]
    }
    where x is the system threads, y is the batching size, and [f1, f2, ...] are runtime values.
    Also returns a dictionary of units for each configuration.
    """
    import os
    import re

    pattern = re.compile(r"timing_(?P<threads>\d+)s_(?P<batch>\d+)b.*\.txt$")
    results = {}
    units = {}
    system_threads = set()
    batches = set()

    for path in file_paths:
        # Extract filename from path
        name = os.path.basename(path)
        m = pattern.search(name)
        if not m:
            print(f"Warning: Filename '{name}' doesn't match expected pattern")
            continue

        t = int(m.group("threads"))
        b = int(m.group("batch"))
        system_threads.add(t)
        batches.add(b)

        try:
            # Use TimingFile to parse the file
            tf = TimingFile()
            tf.parse(path)

            # Extract total runtime and convert to nanoseconds for consistent storage
            val, unit = tf.total_runtime
            val_ns = convert_to_ns(val, unit)
            results.setdefault((t, b), []).append(val_ns)

            # Store unit for this configuration
            if (t, b) not in units:
                units[(t, b)] = unit
        except Exception as e:
            print(f"Warning: Failed to parse {path}: {e}")
            continue

    return results, units, system_threads, batches


if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Create heatmap from runtime data",
        epilog="""Example usage:
  python scripts/heatmap.py timings/timing_*s_*b_*.txt -o heatmap.png
        """,
    )
    p.add_argument("files", nargs="+", help="List of timing files to process")
    p.add_argument("-o", "--output", help="Output file name", default="heatmap.png")
    args = p.parse_args()

    times_ns, units, system_threads, batches = parse_runtime(args.files)

    # Get a representative unit for display (from first config)
    if not units:
        print("Error: No timing files found")
        exit(1)

    # Use the first configuration's unit as reference
    reference_unit = next(iter(units.values()))

    # Ensure axes are sorted ascending for consistent visualization
    system_threads = sorted(system_threads)
    batches = sorted(batches)

    # Create 2D array for heatmap
    time_array = np.zeros((len(system_threads), len(batches)))
    for i, s in enumerate(system_threads):
        for j, b in enumerate(batches):
            if (s, b) not in times_ns:
                print(
                    f"Warning: No data found for System Threads: {s}, Batching Size: {b}"
                )
                continue

            # Get times in nanoseconds
            res_ns = times_ns[(s, b)]

            # Convert back to reference unit for display
            res = [convert_from_ns(val_ns, reference_unit) for val_ns in res_ns]

            mean_time = np.mean(res)
            perc_95 = np.percentile(res, 95)
            max_time = np.max(res)
            min_time = np.min(res)

            print(f"System Threads: {s}, Batching Size: {b}")
            print(
                f"  Mean: {mean_time:.4f}{reference_unit}, 95p: {perc_95:.4f}{reference_unit}, "
                f"Max: {max_time:.4f}{reference_unit}, Min: {min_time:.4f}{reference_unit}\n"
            )
            time_array[i, j] = mean_time
    # Create the heatmap
    plt.figure(figsize=(8, 6))
    plt.imshow(time_array, cmap="RdYlGn_r", origin="lower", aspect="auto")

    # Add labels and title
    plt.xticks(range(len(batches)), batches)
    plt.yticks(range(len(system_threads)), system_threads)
    plt.xlabel("Batching Size")
    plt.ylabel("System Threads")
    plt.title("End to End Time Heatmap")

    # Add colorbar
    cbar = plt.colorbar()
    cbar.set_label(f"Total Time ({reference_unit})")

    # Save the plot
    plt.savefig(args.output, dpi=300, bbox_inches="tight")
    print(f"Heatmap saved as '{args.output}'")
