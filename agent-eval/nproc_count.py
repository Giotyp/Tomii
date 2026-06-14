"""Utility: detect physical CPU core count."""

import os


def physical_cores() -> int:
    try:
        # Linux: read from /sys/devices/system/cpu/
        online = set()
        sibling_sets = []
        cpu_dir = "/sys/devices/system/cpu"
        for entry in os.listdir(cpu_dir):
            if not entry.startswith("cpu") or not entry[3:].isdigit():
                continue
            sib_path = f"{cpu_dir}/{entry}/topology/thread_siblings_list"
            if os.path.exists(sib_path):
                sib = open(sib_path).read().strip()
                sibling_sets.append(sib)
        cores = len(set(sibling_sets)) if sibling_sets else 0
        if cores > 0:
            return cores
    except Exception:
        pass
    # Fallback: logical CPUs
    try:
        import multiprocessing

        return multiprocessing.cpu_count()
    except Exception:
        return 4
