import csv
import re
from collections import defaultdict


def best_unit(filepath, threshold=10):
    """
    Determine the best time unit for displaying timing data based on total runtime.

    Args:
        filepath: Path to the timing file
        threshold: Threshold value for unit selection (default: 10)

    Returns:
        String representing the best unit: 'ns', 'µs', 'ms', or 's'
    """
    from timingfile import TimingFile

    tf = TimingFile()
    tf.parse(filepath)

    # Get total runtime and convert to nanoseconds
    total_value, total_unit = tf.total_runtime
    total_ns = convert_to_ns(total_value, total_unit)

    # Determine best unit based on thresholds
    threshold_us = threshold * 1e3  # 10µs in ns
    threshold_ms = threshold * 1e6  # 10ms in ns
    threshold_s = threshold * 1e9  # 10s in ns

    if total_ns < threshold_us:
        return "ns"
    elif total_ns < threshold_ms:
        return "µs"
    elif total_ns < threshold_s:
        return "ms"
    else:
        return "s"


def best_runtime(timing_files):
    """
    Parse multiple timing files and return the corresponding sched file
    of the txt with the best (minimum) total runtime found.
    """
    from timingfile import TimingFile

    tf_files = {}
    for filepath in timing_files:
        tf = TimingFile()
        tf.parse(filepath)
        tf_files[filepath] = tf
    best = min(tf.total_runtime for tf in tf_files.values())
    best_file = [f for f, tf in tf_files.items() if tf.total_runtime == best][0]
    best_file = best_file.replace(".txt", "_sched.csv")
    return best_file


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
    """Group records by slot number."""
    slots = defaultdict(list)
    for rec in records:
        slot = rec[0]
        slots[slot].append(rec)
    return dict(sorted(slots.items()))


def separate_worker_system_slots(slots, system_threads=0, worker_slots_count=0):
    """
    Separate worker slots from system thread slots and receiver thread slots.

    Uses task_id-based detection to identify slot types:
    - Worker slots: contain regular task_ids (typically 0-100)
    - System slots: contain high task_ids (65534=prep, 65535=resolution)
    - Receiver slots: contain packet reception task_ids (typically 0 or very low)

    Args:
        slots: Dictionary of slot_id -> records
        system_threads: Number of system threads (default: 0, auto-detect)
        worker_slots_count: Number of worker slots (default: 0, auto-detect)

    Returns:
        Tuple of (worker_slots, system_slots, receiver_slots) dictionaries
    """
    all_slots = sorted(slots.keys())

    worker_slots = {}
    system_slots = {}
    receiver_slots = {}

    # Find max task_id to identify system thread task_ids
    max_task_id = 0
    for slot_records in slots.values():
        for rec in slot_records:
            task_id = rec[5]
            max_task_id = max(max_task_id, task_id)

    # System thread task_ids are very high (65534, 65535)
    # Only treat as system thread if max_task_id is above a threshold
    SYSTEM_TASK_THRESHOLD = 10000  # System tasks are IdType::MAX - 1 and IdType::MAX

    if max_task_id >= SYSTEM_TASK_THRESHOLD:
        # We have system thread tasks in the data
        resolution_task_id = max_task_id
        preparation_task_id = max_task_id - 1
    else:
        # No system thread tasks detected - all slots are worker or receiver slots
        resolution_task_id = -1
        preparation_task_id = -1

    packet_rx_task_id = 0  # Packet reception uses task_id 0

    # Classify each slot based on the task_ids it contains
    for slot_id, slot_records in slots.items():
        slot_task_ids = set(rec[5] for rec in slot_records)

        # Check if this slot contains system thread tasks
        if resolution_task_id in slot_task_ids or preparation_task_id in slot_task_ids:
            system_slots[slot_id] = slot_records
        # Check if this slot contains ONLY packet reception tasks
        elif slot_task_ids == {packet_rx_task_id}:
            receiver_slots[slot_id] = slot_records
        else:
            # Regular worker slot
            worker_slots[slot_id] = slot_records

    return worker_slots, system_slots, receiver_slots


def group_by_worker(records):
    """Group records by worker number."""
    workers = defaultdict(list)
    for rec in records:
        worker_id = rec[4]  # worker field is at index 4
        workers[worker_id].append(rec)
    return dict(sorted(workers.items()))


def parse_unit_value(value_str):
    """
    Parse a value with unit (e.g., '2.1605ms', '925.4000µs') and return (value, unit).
    """
    match = re.match(r"([\d,.]+)(ms|µs|ns|s)?", value_str.strip())
    if match:
        value = float(match.group(1).replace(",", ""))
        unit = match.group(2) if match.group(2) else ""
        return value, unit
    return None, None


def convert_to_ns(value, unit):
    """
    Convert a time value to nanoseconds.
    """
    if unit == "s":
        return value * 1e9
    elif unit == "ms":
        return value * 1e6
    elif unit == "µs":
        return value * 1e3
    else:  # ns or no unit
        return value


def convert_from_ns(value_ns, target_unit):
    """
    Convert a time value from nanoseconds to the target unit.
    """
    if target_unit == "s":
        return value_ns / 1e9
    elif target_unit == "ms":
        return value_ns / 1e6
    elif target_unit == "µs":
        return value_ns / 1e3
    else:  # ns or no unit
        return value_ns


def format_unit_value(value, unit, decimals=4):
    """
    Format a value with unit.
    """
    if unit:
        return f"{value:.{decimals}f}{unit}"
    else:
        return f"{value:.{decimals}f}"
