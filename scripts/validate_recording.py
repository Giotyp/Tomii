#!/usr/bin/env python3
"""
Validate Τομί scheduler recordings for core allocation correctness.
Checks for worker ID conflicts and system/worker core overlaps.

Usage:
    python validate_recording.py --csv timing.txt --system-threads 2 --workers 4 --network 2
"""

import argparse
import csv
from collections import defaultdict


def parse_csv(filepath):
    """Parse scheduler CSV: slot,job_id,start_ns,end_ns,worker,task_id,index"""
    records = []
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        # skip header row
        next(reader)
        for row in reader:
            if len(row) >= 7:
                records.append(
                    {
                        "slot": int(row[0]),
                        "job_id": int(row[1]),
                        "start_ns": int(row[2]),
                        "end_ns": int(row[3]),
                        "worker": int(row[4]),
                        "task_id": int(row[5]),
                        "index": int(row[6]),
                    }
                )
    return records


def analyze_worker_ids(records, system_threads, workers, network_workers):
    """Check for worker ID conflicts and proper allocation."""
    worker_ids = set()
    system_slots = set()
    worker_slots = set()

    max_worker_slot = None

    for r in records:
        worker_id = r["worker"]
        slot = r["slot"]

        # Identify slot types (system slots are typically highest numbered)
        if max_worker_slot is None:
            # First pass: find max slot to identify system slots
            pass

        worker_ids.add(worker_id)

    # Determine expected ranges
    expected_total = workers + (network_workers if network_workers else 0)
    expected_ids = set(range(expected_total))

    # Current (buggy) system would have conflicts
    # Network workers would reuse IDs 0..(network_workers-1)
    buggy_main_ids = set(range(workers))
    buggy_network_ids = set(range(network_workers)) if network_workers else set()
    has_conflict = len(buggy_main_ids & buggy_network_ids) > 0

    print("=" * 60)
    print("WORKER ID ANALYSIS")
    print("=" * 60)
    print(f"Expected total workers: {expected_total}")
    print(f"  Main pool:    0..{workers-1}")
    if network_workers:
        print(f"  Network pool: {workers}..{workers+network_workers-1}")
    print()
    print(f"Observed worker IDs: {sorted(worker_ids)}")
    print(f"  Count: {len(worker_ids)}")
    print()

    # Check if observed IDs match expected sequential allocation
    if worker_ids == expected_ids:
        print("✅ Worker IDs correctly allocated (sequential, no conflicts)")
        print("   This indicates UNIFIED scheduler architecture")
        return True
    elif max(worker_ids) < workers and network_workers:
        print("❌ Worker IDs show CONFLICT pattern!")
        print(f"   Max ID {max(worker_ids)} < workers {workers}")
        print("   This indicates DUAL scheduler architecture with overlapping IDs")
        print()
        print("   Example conflict:")
        print(f"     Main scheduler:    Worker IDs {sorted(buggy_main_ids)}")
        print(f"     Network scheduler: Worker IDs {sorted(buggy_network_ids)}")
        print(f"     Overlap:           {sorted(buggy_main_ids & buggy_network_ids)}")
        return False
    else:
        print("⚠️  Worker ID pattern unclear, manual inspection needed")
        return None


def analyze_core_allocation(records, system_threads, workers, network_workers):
    """Infer core allocation from worker IDs and check for overlaps."""
    # Group records by worker to infer which workers are on which cores
    # In the CSV, 'worker' field for scheduler tasks is the worker thread ID
    # For system threads (resolution), 'worker' is the core they're pinned to

    worker_tasks = defaultdict(list)
    system_tasks = defaultdict(list)

    # Heuristic: System threads recorded with task_id == MAX or MAX-1
    SYSTEM_TASK_ID_PREP = 2**16 - 2  # IdType::MAX - 1 from runtime.rs
    SYSTEM_TASK_ID_RES = 2**16 - 1  # IdType::MAX from runtime.rs

    for r in records:
        task_id = r["task_id"]
        worker = r["worker"]

        if task_id in (SYSTEM_TASK_ID_PREP, SYSTEM_TASK_ID_RES):
            # This is a system thread record
            system_tasks[worker].append(r)
        else:
            # This is a worker task
            worker_tasks[worker].append(r)

    print()
    print("=" * 60)
    print("CORE ALLOCATION ANALYSIS")
    print("=" * 60)

    # System threads
    system_cores = sorted(system_tasks.keys())
    print(f"System thread cores: {system_cores}")
    print(f"  Expected: {system_threads} system threads")
    if len(system_cores) == system_threads:
        print(f"  ✅ Correct count")
    else:
        print(f"  ⚠️  Found {len(system_cores)} instead of {system_threads}")

    # Worker threads
    worker_ids = sorted(worker_tasks.keys())
    print(f"\nWorker thread IDs: {worker_ids}")
    print(f"  Expected: {workers + (network_workers or 0)} workers")
    if len(worker_ids) == workers + (network_workers or 0):
        print(f"  ✅ Correct count")
    else:
        print(f"  ⚠️  Found {len(worker_ids)} workers")

    # Check for overlap (CRITICAL BUG)
    # System threads' 'worker' field is their core ID
    # Worker tasks' 'worker' field is worker thread ID, not core
    # So we can't directly detect core overlap from CSV alone
    # But we can detect ID conflicts

    print()
    print("=" * 60)
    print("OVERLAP CHECK")
    print("=" * 60)

    # The bug manifests as: system threads appear with low core IDs (0, 1, ...)
    # AND worker IDs also start from 0
    # In the buggy line 99: (system_core=0, workers=1, worker_core=0)
    # So system thread on core 0, and workers start at core 0

    # Check if system cores overlap with expected worker core range
    # Expected layout: system cores first, then worker cores
    # E.g., 2 system threads → cores 0-1, workers start at core 2+

    # If we see system threads on cores that should be worker cores, that's the bug
    if system_cores and worker_ids:
        expected_worker_start_core = system_threads

        # In recordings, system threads' 'worker' field is their core
        # Workers' 'worker' field is their thread ID (which may not equal core)
        # But in start_handler, workers are pinned to sequential cores

        # The bug is: create_threadpool returns (0, 1, 0)
        # meaning worker_core_offset=0, same as system_core_offset=0
        # So workers would be pinned to cores starting at 0

        # Check: are any system cores in the range [0, workers)?
        potential_overlap = [c for c in system_cores if c < workers]

        if potential_overlap:
            print("⚠️  POTENTIAL OVERLAP DETECTED!")
            print(f"   System threads on cores: {system_cores}")
            print(
                f"   These cores {potential_overlap} are in worker range [0, {workers})"
            )
            print()
            print("   This suggests the fallback bug (line 99 in scheduler.rs):")
            print("   return (0, 1, 0)  ← system_core=0, worker_core=0")
            print()
            print("❌ CORE OVERLAP: System and workers may be on SAME cores!")
            return False
        else:
            print("✅ No obvious overlap detected")
            print(f"   System cores {system_cores} outside worker range")
            return True
    else:
        print("⚠️  Insufficient data to determine overlap")
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Validate Τομί scheduler recordings"
    )
    parser.add_argument("--csv", required=True, help="Path to timing CSV file")
    parser.add_argument("--system-threads", type=int, required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--network", type=int, default=0, help="Network workers (nrx)")
    args = parser.parse_args()

    print(f"Analyzing: {args.csv}")
    print(
        f"Configuration: {args.system_threads} system + {args.workers} workers + {args.network} network"
    )
    print()

    records = parse_csv(args.csv)
    print(f"Loaded {len(records)} records")
    print()

    # Analyze worker IDs
    ids_ok = analyze_worker_ids(
        records, args.system_threads, args.workers, args.network
    )

    # Analyze core allocation
    cores_ok = analyze_core_allocation(
        records, args.system_threads, args.workers, args.network
    )

    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)

    if ids_ok and cores_ok:
        print("✅ ALL CHECKS PASSED")
        print("   Recording shows correct unified scheduler behavior")
        return 0
    elif ids_ok is False or cores_ok is False:
        print("❌ ISSUES DETECTED")
        print("   Recording shows buggy dual scheduler behavior")
        print()
        print("Recommended action:")
        print("  1. Apply core allocation fix (Phase 1)")
        print("  2. Implement unified scheduler (Phase 2)")
        print("  3. Re-run benchmark and validate again")
        return 1
    else:
        print("⚠️  INCONCLUSIVE")
        print("   Manual inspection required")
        return 2


if __name__ == "__main__":
    exit(main())
