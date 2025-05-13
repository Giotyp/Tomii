#![allow(unused_imports)]
#![allow(dead_code)]
use core_affinity;
use rayon::{prelude::*, vec};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crossbeam_channel::{bounded, Receiver, Sender};

pub struct Scheduler {
    threadpool: ThreadPool,
}

// Public API
impl Scheduler {
    pub fn new(core_offset: usize, workers: usize) -> Scheduler {
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

        Scheduler { threadpool }
    }

    pub fn spawn_task<F>(&self, task_clos: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Spawn Rayon tasks to process the given closure
        self.threadpool.spawn(move || {
            task_clos();
        });
    }

    pub fn get_thread_idx() -> usize {
        rayon::current_thread_index().unwrap_or(0)
    }
}
