use crossbeam_channel::{Receiver, Sender};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::thread;

/// Control message sent from main thread to the coordinator thread.
pub enum CtrlMsg {
    /// Reset all dep counters for the next sweep; reply on the provided channel.
    Reset(Sender<()>),
    /// Shut down the coordinator thread.
    Stop,
}

/// Centralised resolution: all dependency tracking happens in a single coordinator thread.
///
/// Workers execute tasks and then push their completion to `completion_tx`. They do NO
/// inline resolution — that work happens in the coordinator. This models the "separate
/// readiness-management stage" that the Resolve principle claims to eliminate.
///
/// Hot path difference vs DistributedResolution:
///   Worker:      execute task → send(completion_tx)     ← cheap send
///   Coordinator: recv → decrement dep → maybe send(ready_tx)  ← extra thread hop
pub struct CentralizedResolution {
    pub completion_tx: Sender<u32>,
    ctrl_tx: Sender<CtrlMsg>,
}

impl CentralizedResolution {
    /// Spawn the coordinator thread and return the resolution handle.
    ///
    /// `successors` is a flat topology snapshot (indexed by node ID).
    /// `remaining` is shared with the main thread; coordinator decrements it per task.
    /// `done_tx` is signalled when `remaining` hits zero.
    pub fn start(
        initial_deps: Vec<u32>,
        successors: Vec<Vec<u32>>,
        ready_tx: Sender<u32>,
        remaining: Arc<AtomicUsize>,
        done_tx: Sender<()>,
    ) -> Self {
        let (completion_tx, completion_rx) = crossbeam_channel::unbounded::<u32>();
        let (ctrl_tx, ctrl_rx) = crossbeam_channel::unbounded::<CtrlMsg>();

        thread::spawn(move || {
            coordinator_loop(
                initial_deps,
                successors,
                completion_rx,
                ctrl_rx,
                ready_tx,
                remaining,
                done_tx,
            )
        });

        Self { completion_tx, ctrl_tx }
    }

    /// Send this node's completion to the coordinator. Called by worker threads.
    #[allow(dead_code)]
    #[inline(always)]
    pub fn submit(&self, node_id: u32) {
        self.completion_tx.send(node_id).unwrap();
    }

    /// Reset dep counters for the next sweep. Synchronous: returns once coordinator confirms.
    /// Called by main thread between sweeps (outside timing window).
    pub fn reset(&self) {
        let (ack_tx, ack_rx) = crossbeam_channel::bounded(1);
        self.ctrl_tx.send(CtrlMsg::Reset(ack_tx)).unwrap();
        ack_rx.recv().unwrap();
    }

    pub fn stop(&self) {
        self.ctrl_tx.send(CtrlMsg::Stop).unwrap();
    }
}

fn coordinator_loop(
    initial_deps: Vec<u32>,
    successors: Vec<Vec<u32>>,
    completion_rx: Receiver<u32>,
    ctrl_rx: Receiver<CtrlMsg>,
    ready_tx: Sender<u32>,
    remaining: Arc<AtomicUsize>,
    done_tx: Sender<()>,
) {
    // Coordinator owns dep_remaining exclusively (no locks needed — single writer).
    let mut dep_remaining = initial_deps.clone();

    loop {
        crossbeam_channel::select! {
            recv(completion_rx) -> msg => {
                let completed_id = msg.unwrap() as usize;
                for &succ in &successors[completed_id] {
                    dep_remaining[succ as usize] -= 1;
                    if dep_remaining[succ as usize] == 0 {
                        dep_remaining[succ as usize] = initial_deps[succ as usize];
                        ready_tx.send(succ).unwrap();
                    }
                }
                // Signal done when this was the last task in the sweep.
                if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                    done_tx.send(()).unwrap();
                }
            }
            recv(ctrl_rx) -> msg => {
                match msg.unwrap() {
                    CtrlMsg::Reset(ack) => {
                        // Sweep is over; reset dep counters for next sweep.
                        for (d, &init) in dep_remaining.iter_mut().zip(initial_deps.iter()) {
                            *d = init;
                        }
                        ack.send(()).unwrap();
                    }
                    CtrlMsg::Stop => return,
                }
            }
        }
    }
}
