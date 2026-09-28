use super::Priority;
use crossbeam_channel::{Receiver, Sender};
use std::sync::Arc;

/// Recording metadata carried alongside task.
/// Eliminates Arc::clone per spawn - worker loop handles metrics directly.
pub(super) struct RecordMeta {
    pub(super) job_id: usize,
    pub(super) task_id: crate::IdType,
    pub(super) slot: usize,
    pub(super) index: usize,
}

/// Task + optional recording metadata. The boxed-closure channel item.
pub(super) struct ScheduledTask {
    pub(super) task: super::BoxedTask,
    pub(super) meta: Option<RecordMeta>,
}

/// The item type for all channels.
///
/// `Node` is the zero-allocation typed spawn path: a POD [`super::NodeTaskDesc`]
/// travels through the channel by value and is executed via the node-executor
/// hook installed once at runtime init — no per-task `Box`, no closure capture.
/// `Boxed` remains for post-nodes, custom-func nodes, and external callers.
pub(super) enum Job {
    Boxed(ScheduledTask),
    Node(super::NodeTaskDesc),
}

/// 3 priority-level MPMC channels (High/Normal/Low).
/// Used for both global and per-group task distribution.
/// crossbeam_channel provides efficient MPMC with built-in park/wake.
pub(super) struct ChannelSet {
    pub(super) high_tx: Sender<Job>,
    pub(super) high_rx: Receiver<Job>,
    pub(super) normal_tx: Sender<Job>,
    pub(super) normal_rx: Receiver<Job>,
    pub(super) low_tx: Sender<Job>,
    pub(super) low_rx: Receiver<Job>,
}

impl ChannelSet {
    pub(super) fn new() -> Self {
        let (high_tx, high_rx) = crossbeam_channel::unbounded();
        let (normal_tx, normal_rx) = crossbeam_channel::unbounded();
        let (low_tx, low_rx) = crossbeam_channel::unbounded();
        Self {
            high_tx,
            high_rx,
            normal_tx,
            normal_rx,
            low_tx,
            low_rx,
        }
    }

    #[inline]
    pub(super) fn send(&self, priority: Priority, job: Job) {
        let _ = match priority {
            Priority::High => self.high_tx.send(job),
            Priority::Normal => self.normal_tx.send(job),
            Priority::Low => self.low_tx.send(job),
        };
    }

    /// Non-blocking priority-ordered receive.
    /// Checks High first, then Normal, then Low.
    #[allow(dead_code)] // used by future work-stealing / load-balancing path
    #[inline]
    pub(super) fn try_recv_prioritized(&self) -> Option<Job> {
        self.high_rx
            .try_recv()
            .ok()
            .or_else(|| self.normal_rx.try_recv().ok())
            .or_else(|| self.low_rx.try_recv().ok())
    }

    #[allow(dead_code)] // used by future load-balancing / backpressure path
    #[inline]
    pub(super) fn is_empty(&self) -> bool {
        self.high_rx.is_empty() && self.normal_rx.is_empty() && self.low_rx.is_empty()
    }

    /// Total queued jobs across all priority levels. crossbeam updates these lengths
    /// synchronously on `send`, so least-loaded dispatch reading them self-balances a
    /// burst across shards (unlike an idle-worker count, which lags until a worker
    /// wakes and can herd a burst onto one shard).
    #[inline]
    pub(super) fn load(&self) -> usize {
        self.high_rx.len() + self.normal_rx.len() + self.low_rx.len()
    }
}

/// Non-blocking priority-ordered receive across group and global channels.
/// Order: group.high -> group.normal -> global.high -> global.normal -> group.low -> global.low
#[inline]
pub(super) fn try_recv_all(
    group: &ChannelSet,
    global: &ChannelSet,
    allow_global: bool,
) -> Option<Job> {
    // Group high priority
    if let Ok(t) = group.high_rx.try_recv() {
        return Some(t);
    }
    // Group normal priority
    if let Ok(t) = group.normal_rx.try_recv() {
        return Some(t);
    }
    // Global high/normal (if allowed)
    if allow_global {
        if let Ok(t) = global.high_rx.try_recv() {
            return Some(t);
        }
        if let Ok(t) = global.normal_rx.try_recv() {
            return Some(t);
        }
    }
    // Group low priority
    if let Ok(t) = group.low_rx.try_recv() {
        return Some(t);
    }
    // Global low (if allowed)
    if allow_global {
        if let Ok(t) = global.low_rx.try_recv() {
            return Some(t);
        }
    }
    None
}

/// Steal a job (priority-ordered) from any shard other than `own`, scanning from
/// `start` so different workers probe shards in different orders. Used only in the
/// sharded (default) config: an idle worker whose own shard is empty pulls work that
/// another shard's workers have not reached yet, so a load imbalance across shards
/// still drains without waiting on the park timeout.
#[inline]
pub(super) fn try_steal_shards(
    shards: &[Arc<ChannelSet>],
    own: usize,
    start: usize,
) -> Option<Job> {
    let n = shards.len();
    for k in 0..n {
        let i = (start + k) % n;
        if i == own {
            continue;
        }
        let s = &shards[i];
        if let Ok(t) = s.high_rx.try_recv() {
            return Some(t);
        }
        if let Ok(t) = s.normal_rx.try_recv() {
            return Some(t);
        }
        if let Ok(t) = s.low_rx.try_recv() {
            return Some(t);
        }
    }
    None
}
