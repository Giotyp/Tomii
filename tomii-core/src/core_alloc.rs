//! Core allocation logic for thread pools
//!
//! This module provides a pure, deterministic core allocation algorithm
//! that determines where to pin system threads, receiver threads, and worker threads.
//! The algorithm handles over-subscription gracefully with fallback strategies.

use core_affinity::CoreId;

/// Result of core allocation strategy selection
pub struct CoreAllocation {
    /// Offset where system threads start
    pub system_core_offset: usize,
    /// Number of system threads allocated
    pub system_threads: usize,
    /// Offset where receiver threads start
    pub receiver_offset: usize,
    /// Number of receiver threads allocated
    pub receiver_threads: usize,
    /// Offset where worker threads start
    pub worker_offset: usize,
    /// Number of worker threads allocated
    pub worker_count: usize,
    /// Optional reserved core for main/orchestrator thread
    pub main_core: Option<CoreId>,
    /// All available physical cores, sorted
    pub all_core_ids: Vec<CoreId>,
}

/// Allocate cores for thread pool with fallback logic
///
/// Determines optimal core offsets for system threads, receiver threads, and workers.
/// Implements a 5-branch decision tree:
///
/// 1. **Insufficient cores** (< 2): panic
/// 2. **Ideal case**: Spare core for main + requested layout
/// 3. **Offset valid but no spare**: Honor requested offset, no main core
/// 4. **Layout valid at offset 0 only**: Reset offset, warn
/// 5. **Over-subscription**: Reduce proportionally, warn
///
/// # Arguments
/// * `core_offset` - Requested starting core offset
/// * `system_threads` - Number of system threads needed
/// * `receiver_threads` - Number of receiver threads needed
/// * `worker_count` - Number of worker threads needed
///
/// # Returns
/// `CoreAllocation` struct with computed offsets and actual thread counts
///
/// # Panics
/// If fewer than 2 cores are available (system + 1 worker minimum)
pub fn allocate_cores(
    core_offset: usize,
    system_threads: usize,
    receiver_threads: usize,
    worker_count: usize,
) -> CoreAllocation {
    // Get and sort available cores
    let mut core_ids = core_affinity::get_core_ids().unwrap_or_default();
    core_ids.sort();
    let available_cores = core_ids.len();

    let total_needed = system_threads + receiver_threads + worker_count;

    // 5-branch decision tree for core allocation
    let (
        system_core_offset,
        receiver_offset,
        worker_offset,
        actual_workers,
        actual_receivers,
        actual_system_threads,
        main_core_opt,
    ) = if available_cores < 2 {
        // Branch 1: Insufficient cores (panic)
        panic!(
            "Insufficient cores: need minimum 2 cores (1 system + 1 worker), found {}",
            available_cores
        );
    } else if core_offset + total_needed < available_cores {
        // Branch 2: Ideal case - we can reserve an extra core for the main thread at `core_offset`
        let main_idx = core_offset;
        let sys_start = core_offset + 1;
        let recv_start = sys_start + system_threads;
        let worker_start = recv_start + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            worker_count,
            receiver_threads,
            system_threads,
            Some(core_ids[main_idx]),
        )
    } else if core_offset + total_needed <= available_cores {
        // Branch 3: Can honor requested offset but no spare core for main
        let sys_start = core_offset;
        let recv_start = core_offset + system_threads;
        let worker_start = recv_start + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            worker_count,
            receiver_threads,
            system_threads,
            None,
        )
    } else if total_needed <= available_cores {
        // Branch 4: Fit all threads but not with requested offset - use offset 0
        tracing::warn!(
            core_offset,
            "cannot honor core_offset, using offset 0 instead"
        );
        let sys_start = 0;
        let recv_start = system_threads;
        let worker_start = recv_start + receiver_threads;
        (
            sys_start,
            recv_start,
            worker_start,
            worker_count,
            receiver_threads,
            system_threads,
            None,
        )
    } else {
        // Branch 5: Over-subscription - scale down proportionally
        let max_system = 1; // at least one system thread
        let remaining = available_cores.saturating_sub(max_system);
        let max_receivers = receiver_threads.min(remaining / 2).max(0);
        let max_workers = remaining.saturating_sub(max_receivers).max(1);
        tracing::warn!(
            requested = total_needed,
            available = available_cores,
            actual_system = max_system,
            actual_receivers = max_receivers,
            actual_workers = max_workers,
            "over-subscription: scaling down thread counts"
        );
        (
            0,
            max_system,
            max_system + max_receivers,
            max_workers,
            max_receivers,
            max_system,
            None,
        )
    };

    // VERIFICATION: Ensure proper sequential allocation with no overlaps
    assert!(
        system_core_offset + actual_system_threads <= receiver_offset,
        "Core allocation bug: system cores [{}..{}) overlap with receiver cores [{}..{})",
        system_core_offset,
        system_core_offset + actual_system_threads,
        receiver_offset,
        receiver_offset + actual_receivers
    );
    assert!(
        receiver_offset + actual_receivers <= worker_offset,
        "Core allocation bug: receiver cores [{}..{}) overlap with worker cores [{}..{})",
        receiver_offset,
        receiver_offset + actual_receivers,
        worker_offset,
        worker_offset + actual_workers
    );

    CoreAllocation {
        system_core_offset,
        system_threads: actual_system_threads,
        receiver_offset,
        receiver_threads: actual_receivers,
        worker_offset,
        worker_count: actual_workers,
        main_core: main_core_opt,
        all_core_ids: core_ids,
    }
}
