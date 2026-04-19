//! Thread spawning and shutdown helpers for receiver and resolution threads.
//! Holds the `TomiiRt::spawn_receiver_threads`, `spawn_resolution_threads`, and
//! `shutdown_receiver_threads` impl blocks; no runtime logic lives here.
use super::TomiiRt;
use crate::async_recorder::set_worker_recorder;
use crate::debug::print_debug;
#[cfg(feature = "network")]
use crate::network::multi_socket_receiver_loop;
use core_affinity;
#[cfg(feature = "network")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Spawn one network-receiver thread and return its handle.
///
/// Encapsulates the `thread::Builder` boilerplate shared by both the 1:1
/// (one thread per socket) and round-robin (one thread per socket range) paths.
#[cfg(feature = "network")]
#[allow(clippy::too_many_arguments)]
fn spawn_receiver_thread(
    thread_name: String,
    packet_length: usize,
    recv_pool_size: usize,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    tx: flume::Sender<crate::network::PacketMessage>,
    sockets: Arc<Vec<crate::network::NetworkSocket>>,
    drop_counters: Arc<Vec<std::sync::atomic::AtomicUsize>>,
    thread_id: usize,
    socket_range: std::ops::Range<usize>,
    core_id: usize,
    return_rxs: Vec<flume::Receiver<Vec<u8>>>,
) -> Result<JoinHandle<()>, crate::RuntimeError> {
    thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            multi_socket_receiver_loop(
                packet_length,
                recv_pool_size,
                shutdown,
                tx,
                sockets,
                drop_counters,
                thread_id,
                socket_range,
                core_id,
                return_rxs,
            );
        })
        .map_err(crate::RuntimeError::SpawnFailed)
}

// ---------------------------------------------------------------------------
// impl TomiiRt
// ---------------------------------------------------------------------------

impl TomiiRt {
    /// Spawn dedicated network receiver threads (one per socket, or round-robin if fewer threads).
    #[cfg(feature = "network")]
    pub(super) fn spawn_receiver_threads(
        &self,
    ) -> Result<Vec<JoinHandle<()>>, crate::RuntimeError> {
        let Some(ref network_config) = self.shared.graph.network_config() else {
            tracing::debug!("No network_config present - skipping network receiver setup");
            return Ok(Vec::new());
        };

        let num_sockets = network_config.num_sockets;
        let buffer_depth = network_config.buffer_depth;

        tracing::info!(
            num_sockets,
            buffer_depth,
            "initializing network receiver infrastructure"
        );

        assert_eq!(
            self.shared.net.receiver_sockets.len(),
            num_sockets,
            "Network config expected {} sockets but {} were allocated",
            num_sockets,
            self.shared.net.receiver_sockets.len()
        );

        let receiver_threads = self.shared.config.receiver_threads;
        let receiver_offset = self.shared.config.receiver_core_offset;
        let dylib_path =
            std::env::var("DYLIB_PATH").unwrap_or_else(|_| "./libmimolib.so".to_string());

        tracing::info!(
            receiver_threads,
            receiver_offset,
            dylib = %dylib_path,
            "spawning receiver threads"
        );

        let packet_length = self
            .shared
            .graph
            .network_config()
            .expect("Network config must be present for receiver threads")
            .packet_length;
        let recv_pool_size = self.shared.config.recv_pool_size;
        let shutdown = Arc::clone(&self.shared.shutdown_flag);
        let tx = self.shared.net.packet_sender.clone();
        let sockets = Arc::clone(&self.shared.net.receiver_sockets);
        let drop_counters = Arc::clone(&self.shared.net.packet_drop_counters);

        let mut handles = Vec::with_capacity(receiver_threads);

        if receiver_threads >= num_sockets {
            tracing::debug!("using 1:1 thread-to-socket mapping");
            for socket_id in 0..num_sockets {
                let core_id = receiver_offset + socket_id;
                let return_rx = self.shared.net.buffer_return_receivers[socket_id]
                    .lock()
                    .take()
                    .expect("buffer_return_receivers already taken");
                let handle = spawn_receiver_thread(
                    format!("rx-{socket_id}"),
                    packet_length,
                    recv_pool_size,
                    Arc::clone(&shutdown),
                    tx.clone(),
                    Arc::clone(&sockets),
                    Arc::clone(&drop_counters),
                    socket_id,
                    socket_id..socket_id + 1,
                    core_id,
                    vec![return_rx],
                )?;
                handles.push(handle);
                tracing::debug!(socket_id, core_id, "receiver thread spawned");
            }
        } else {
            tracing::warn!(
                receiver_threads,
                num_sockets,
                "receiver_threads < num_sockets, using round-robin polling"
            );
            let sockets_per_thread = num_sockets.div_ceil(receiver_threads);
            for thread_id in 0..receiver_threads {
                let start_socket = thread_id * sockets_per_thread;
                let end_socket = std::cmp::min(start_socket + sockets_per_thread, num_sockets);
                let socket_range = start_socket..end_socket;
                let socket_range_display = socket_range.clone();
                let return_rxs: Vec<flume::Receiver<Vec<u8>>> = (start_socket..end_socket)
                    .map(|sid| {
                        self.shared.net.buffer_return_receivers[sid]
                            .lock()
                            .take()
                            .expect("buffer_return_receivers already taken")
                    })
                    .collect();
                let core_id = receiver_offset + thread_id;
                let handle = spawn_receiver_thread(
                    format!("rx-multi-{thread_id}"),
                    packet_length,
                    recv_pool_size,
                    Arc::clone(&shutdown),
                    tx.clone(),
                    Arc::clone(&sockets),
                    Arc::clone(&drop_counters),
                    thread_id,
                    socket_range,
                    core_id,
                    return_rxs,
                )?;
                handles.push(handle);
                tracing::debug!(
                    thread_id,
                    ?socket_range_display,
                    core_id,
                    "multi-socket receiver spawned"
                );
            }
        }

        tracing::info!("network receiver infrastructure ready");
        Ok(handles)
    }

    /// Spawn resolution threads (one per `system_threads` config value).
    pub(super) fn spawn_resolution_threads(
        &self,
    ) -> Result<Vec<JoinHandle<()>>, crate::RuntimeError> {
        let mut handles = Vec::new();
        for thread_id in 0..self.shared.config.system_threads {
            let shared_clone = Arc::clone(&self.shared);
            let thread_core = self.shared.config.core_offset + thread_id;
            let thread_slot = self.shared.config.slots + thread_id;

            let handle = thread::Builder::new()
                .name(format!("resolution-{thread_id}"))
                .spawn(move || {
                    crate::scheduler::set_current_worker_id(thread_slot);

                    if let Some(ref recorder) = shared_clone.telemetry.async_recorder {
                        if let Some(tx) = recorder.get_worker_sender(thread_slot) {
                            set_worker_recorder(tx);
                        }
                    }

                    if core_affinity::set_for_current(core_affinity::CoreId { id: thread_core }) {
                        tracing::debug!(
                            thread_id,
                            core = thread_core,
                            slot = thread_slot,
                            "resolution thread pinned"
                        );
                    } else {
                        tracing::warn!(
                            thread_id,
                            core = thread_core,
                            "failed to pin resolution thread"
                        );
                    }

                    super::resolution_loop::resolution(
                        shared_clone,
                        thread_core,
                        thread_id,
                        thread_slot,
                    );
                })
                .map_err(crate::RuntimeError::SpawnFailed)?;
            handles.push(handle);
        }
        print_debug(|| {
            format!(
                "{} Resolution threads spawned",
                self.shared.config.system_threads
            )
        });
        Ok(handles)
    }

    /// Signal receiver threads to stop and join them, then report drop statistics.
    #[cfg(feature = "network")]
    pub(super) fn shutdown_receiver_threads(&self, handles: Vec<JoinHandle<()>>) {
        if handles.is_empty() {
            return;
        }

        tracing::info!(count = handles.len(), "shutting down receiver threads");
        self.shared.shutdown_flag.store(true, Ordering::SeqCst);

        for (idx, handle) in handles.into_iter().enumerate() {
            handle.join().unwrap();
            tracing::debug!(idx, "receiver thread shut down");
        }

        // Report packet drop statistics
        if let Some(ref network_config) = self.shared.graph.network_config {
            let num_sockets = network_config.num_sockets;
            let mut total_drops = 0;
            for socket_id in 0..num_sockets {
                let drops = self.shared.net.packet_drop_counters[socket_id].load(Ordering::SeqCst);
                total_drops += drops;
                if drops > 0 {
                    tracing::warn!(socket_id, drops, "packets dropped");
                }
            }
            if total_drops == 0 {
                tracing::info!("no packets dropped");
            } else {
                tracing::warn!(total_drops, "total packets dropped across all sockets");
            }
        }

        let dropped_frames = self.shared.net.dropped_streams.load(Ordering::SeqCst);
        if dropped_frames > 0 {
            tracing::warn!(dropped_frames, "frames dropped (no available slots)");
        }
    }
}
