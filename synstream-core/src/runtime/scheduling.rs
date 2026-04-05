use super::reporting::should_record_slot;
use super::shared_data::SharedData;
use super::task_execution::{execute_task, INLINE_CONTINUATION};
use crate::async_recorder::submit_record;
use crate::buffers::*;
use crate::time_buffer::TimingMethod;
use crate::Record;
use crate::IdType;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use synstream_types::*;

thread_local! {
    // Reusable buffer for preparation() — eliminates vec![None; N] heap allocation
    // on every incremental flush (~77 flushes/stream).
    static PREP_ARGS_BUF: RefCell<Vec<Option<Vec<synstream_types::CmTypes>>>> =
        RefCell::new(Vec::with_capacity(64));
}

#[inline]
pub(super) fn send_to_scheduler(
    shared: &Arc<SharedData>,
    nodes_to_schedule: &Vec<NodeInfo>,
    pre_built_args_vec: &[Option<Vec<CmTypes>>],
    custom_func_vec: Option<&[Option<CmPtr>]>,
) {
    for (i, node_info) in nodes_to_schedule.iter().enumerate() {
        // Look up func_ptr, priority, and affinity from pre-computed cache.
        // Post-nodes use the cold path since they're rare (end-of-run only).
        let custom_func = custom_func_vec.and_then(|v| v[i]);
        let (func_ptr, task_priority, affinity_group) = if node_info.post_node {
            let nodes = &shared
                .graph
                .post_nodes
                .as_ref()
                .expect("Post nodes not initialized");
            let node = &nodes[node_info.id as usize];

            let func = custom_func
                .unwrap_or_else(|| node.func_ptr.expect("Post node function pointer is None"));

            use crate::custom_scheduler::Priority;
            use crate::graph_struct::NodePriority;
            let priority = match node.priority {
                NodePriority::High => Priority::High,
                NodePriority::Normal => Priority::Normal,
                NodePriority::Low => Priority::Low,
            };
            let group = shared
                .exec.scheduler
                .get_affinity_group(node.use_workers.as_ref());
            (func, priority, group)
        } else {
            let cache = &shared.graph_cache.node_cache[node_info.id as usize];
            let func = custom_func.unwrap_or(cache.func_ptr);
            (func, cache.priority, cache.affinity_group)
        };

        let shared_clone = Arc::clone(shared);
        let should_record = should_record_slot(shared, node_info.slot);
        let meta_data = (node_info.id, node_info.slot, node_info.index, should_record);
        let mut node_info = node_info.clone();
        // Stamp the current slot generation so execute_task can detect stale tasks.
        // Post-nodes are exempt: they run after all streams complete and have no generation risk.
        if !node_info.post_node {
            node_info.gen =
                shared.slot_data.generation[node_info.slot].load(Ordering::Acquire) as u32;
        }
        let pre_built_args = pre_built_args_vec[i].clone();

        // Per-task spawn timestamp for accurate scheduling latency measurement.
        let spawn_ns = shared.telemetry.base_instant.elapsed().as_nanos();
        let task = move || {
            let mut current = node_info;
            let mut current_func = func_ptr;
            let mut first = true;
            loop {
                let args = if first { pre_built_args.clone() } else { None };
                first = false;
                execute_task(&shared_clone, current_func, &current, args, spawn_ns);
                match INLINE_CONTINUATION.with(|c| c.borrow_mut().take()) {
                    Some(next) => {
                        current_func = shared_clone.graph_cache.node_cache[next.id as usize].func_ptr;
                        current = next;
                    }
                    None => break,
                }
            }
        };

        if affinity_group > 0 {
            shared.exec.scheduler.spawn_to_group_with_meta(
                affinity_group,
                task_priority,
                Some(meta_data),
                task,
            );
        } else {
            shared
                .exec.scheduler
                .spawn_task_with_meta_priority(task_priority, Some(meta_data), task);
        }
    }
}

impl super::SynRt {
    pub(super) fn preparation(
        shared: &Arc<SharedData>,
        nodes_to_schedule: &Vec<NodeInfo>,
        thread_core: usize,
        thread_slot: usize,
    ) {
        let start_time = if let Some(tb) = &shared.telemetry.time_buffer {
            tb.measure_time()
        } else {
            TimingMethod::Instant(Instant::now())
        };
        let start_ns = shared.telemetry.base_instant.elapsed().as_nanos();

        // Schedule Task - args will be built in the worker thread.
        // Reuse thread-local buffer to avoid vec![None; N] heap allocation per flush.
        PREP_ARGS_BUF.with(|abuf| {
            let mut args_buf = abuf.borrow_mut();
            let n = nodes_to_schedule.len();
            args_buf.clear();
            args_buf.resize(n, None);
            send_to_scheduler(shared, nodes_to_schedule, &*args_buf, None);
        });

        if let Some(tb) = &shared.telemetry.time_buffer {
            let end_time = tb.measure_time();
            let duration = tb.measure_duration(start_time, end_time);
            tb.add_task_time(thread_slot, "Preparation", usize::MAX, duration);
        }

        // Lock-free recording via per-worker channel
        let should_record = shared.telemetry.async_recorder.is_some()
            && nodes_to_schedule
                .iter()
                .any(|n| should_record_slot(&shared, n.slot));
        if should_record {
            let end_ns = shared.telemetry.base_instant.elapsed().as_nanos();
            let job_id = shared.telemetry.job_counter.fetch_add(1, Ordering::SeqCst);
            submit_record(Record {
                slot: thread_slot,
                job_id,
                start_ns,
                end_ns,
                worker: thread_core,
                task_id: IdType::MAX - 1,
                index: 0,
            });
        }
    }

    pub(super) fn schedule_post_nodes(&mut self) {
        use std::thread::sleep;
        use std::time::Duration;
        let nodes = &self.shared.graph.post_nodes;
        if let Some(post_nodes) = nodes {
            let stream_use = self.shared.config.slots + self.shared.config.system_threads; // Use last available slot for post-nodes
            for post_node in post_nodes {
                let mut post_schedule: Vec<NodeInfo> = Vec::new();
                let mut pre_build_args: Vec<Option<Vec<CmTypes>>> = Vec::new();
                let mut functions: Vec<Option<CmPtr>> = Vec::new();
                for index in 0..post_node.factor {
                    let mut node_info = NodeInfo::new(post_node.id, stream_use, index, 0);
                    node_info.set_post_node(true);

                    let arg_vec =
                        super::arg_resolution::parse_args(&self.shared, &post_node.args, index, stream_use, 0, None);

                    let func: Option<CmPtr> = post_node.func_ptr;
                    pre_build_args.push(Some(arg_vec));
                    functions.push(func);
                    post_schedule.push(node_info);
                }
                send_to_scheduler(&self.shared, &post_schedule, &pre_build_args, Some(&functions));
                crate::debug::print_debug(|| format!("Added post node: {}", post_node.name));
                // Wait until all are completed by checking node_results
                let mut completed_count = 0;
                while completed_count < post_node.factor {
                    sleep(Duration::from_millis(10));
                    completed_count = 0;
                    // Lock-free check - no RwLock needed
                    for i in 0..post_node.factor {
                        let node_info = NodeInfo::new(post_node.id, stream_use, i, 0);
                        if self.shared.exec.node_results.result_exists(&node_info) {
                            completed_count += 1;
                        }
                    }
                }
            }
            crate::debug::print_debug(|| "All post-nodes completed".to_string());
        }
    }
}
