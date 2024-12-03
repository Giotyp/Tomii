use core_affinity;
use rayon::prelude::*;
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::graph_struct::*;
use cst_macros::*;

pub struct Executor {
    workers: usize,
    threadpool: ThreadPool,
}

// Public API
impl Executor {
    pub fn new(core_offset: usize, workers: usize) -> Executor {
        // Create threadpool and pin workers to cores
        let mut core_ids = core_affinity::get_core_ids().unwrap();
        core_ids.sort();
        let cores_to_use: Vec<core_affinity::CoreId> =
            core_ids[core_offset..core_offset + workers].to_vec();

        let threadpool = ThreadPoolBuilder::new()
            .num_threads(workers)
            .start_handler(move |thread_index| {
                // Pin each thread to a specific core
                let core_id = cores_to_use[thread_index];
                core_affinity::set_for_current(core_id);
            })
            .build()
            .unwrap();

        Executor {
            workers,
            threadpool,
        }
    }
}
