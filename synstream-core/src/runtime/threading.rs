/// Thread spawning and shutdown for receiver and resolution threads.
use super::SynRt;
use crate::async_recorder::set_worker_recorder;
use crate::debug::print_debug;
#[cfg(feature = "network")]
use crate::network::multi_socket_receiver_loop;
use core_affinity;
#[cfg(feature = "network")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

impl SynRt {
    /// Spawn dedicated network receiver threads (one per socket, or round-robin if fewer threads).
    #[cfg(feature = "network")]
    pub(super) fn spawn_receiver_threads(&self) -> Vec<JoinHandle<()>> {
        let Some(ref network_config) = self.shared.graph.network_config() else {
            println!("No network_config present - skipping network receiver setup");
            return Vec::new();
        };

        let num_sockets = network_config.num_sockets;
        let buffer_depth = network_config.buffer_depth;

        println!("\n=== Initializing Network Receiver Infrastructure ===");
        println!("Number of sockets: {}", num_sockets);
        println!("Buffer depth: {} packets per socket", buffer_depth);

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

        println!(
            "\nSpawning {} receiver threads starting at core {}",
            receiver_threads, receiver_offset
        );
        println!("Using dylib: {} for frame ID extraction", dylib_path);

        let mut handles = Vec::with_capacity(receiver_threads);

        // Extract shared receiver context once — avoid passing all of SharedData into network.rs.
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

        if receiver_threads >= num_sockets {
            println!("Using 1:1 thread-to-socket mapping (optimal)");
            for socket_id in 0..num_sockets {
                let core_id = receiver_offset + socket_id;
                let return_rx = self.shared.net.buffer_return_receivers[socket_id]
                    .lock()
                    .take()
                    .expect("buffer_return_receivers already taken");
                let (pl, rps) = (packet_length, recv_pool_size);
                let (sd, tx2, socks, drops) = (
                    Arc::clone(&shutdown),
                    tx.clone(),
                    Arc::clone(&sockets),
                    Arc::clone(&drop_counters),
                );

                let handle = thread::Builder::new()
                    .name(format!("rx-{}", socket_id))
                    .spawn(move || {
                        multi_socket_receiver_loop(
                            pl,
                            rps,
                            sd,
                            tx2,
                            socks,
                            drops,
                            socket_id,
                            socket_id..socket_id + 1,
                            core_id,
                            vec![return_rx],
                        );
                    })
                    .expect("Failed to spawn receiver thread");
                handles.push(handle);
                println!(
                    "  Receiver thread {} (socket {}) spawned on core {}",
                    socket_id, socket_id, core_id
                );
            }
        } else {
            println!(
                "WARNING: receiver_threads ({}) < num_sockets ({}). Using round-robin polling.",
                receiver_threads, num_sockets
            );
            let sockets_per_thread = (num_sockets + receiver_threads - 1) / receiver_threads;

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
                let (pl, rps) = (packet_length, recv_pool_size);
                let (sd, tx2, socks, drops) = (
                    Arc::clone(&shutdown),
                    tx.clone(),
                    Arc::clone(&sockets),
                    Arc::clone(&drop_counters),
                );

                let handle = thread::Builder::new()
                    .name(format!("rx-multi-{}", thread_id))
                    .spawn(move || {
                        multi_socket_receiver_loop(
                            pl,
                            rps,
                            sd,
                            tx2,
                            socks,
                            drops,
                            thread_id,
                            socket_range,
                            core_id,
                            return_rxs,
                        );
                    })
                    .expect("Failed to spawn receiver thread");
                handles.push(handle);
                println!(
                    "  Multi-socket receiver {} polling sockets {:?} on core {}",
                    thread_id, socket_range_display, core_id
                );
            }
        }

        println!("=== Network Receiver Infrastructure Ready ===\n");
        handles
    }

    /// Spawn resolution threads (one per `system_threads` config value).
    pub(super) fn spawn_resolution_threads(&self) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        for thread_id in 0..self.shared.config.system_threads {
            let shared_clone = Arc::clone(&self.shared);
            let thread_core = self.shared.config.core_offset + thread_id;
            let thread_slot = self.shared.config.slots + thread_id;

            let handle = std::thread::spawn(move || {
                crate::scheduler::set_current_worker_id(thread_slot);

                if let Some(ref recorder) = shared_clone.telemetry.async_recorder {
                    if let Some(tx) = recorder.get_worker_sender(thread_slot) {
                        set_worker_recorder(tx);
                    }
                }

                if core_affinity::set_for_current(core_affinity::CoreId { id: thread_core }) {
                    println!(
                        "Resolution thread {} pinned to core {:?} with slot {}",
                        thread_id, thread_core, thread_slot
                    );
                } else {
                    println!(
                        "Failed to pin resolution thread {} to core {:?}",
                        thread_id, thread_core
                    );
                }

                Self::resolution(shared_clone, thread_core, thread_id, thread_slot);
            });
            handles.push(handle);
        }
        print_debug(|| {
            format!(
                "{} Resolution threads spawned",
                self.shared.config.system_threads
            )
        });
        handles
    }

    /// Signal receiver threads to stop and join them, then report drop statistics.
    #[cfg(feature = "network")]
    pub(super) fn shutdown_receiver_threads(&self, handles: Vec<JoinHandle<()>>) {
        if handles.is_empty() {
            return;
        }

        println!("Shutting down {} receiver threads...", handles.len());
        self.shared.shutdown_flag.store(true, Ordering::SeqCst);

        for (idx, handle) in handles.into_iter().enumerate() {
            handle.join().unwrap();
            println!("  Receiver thread {} shut down successfully", idx);
        }

        // Report packet drop statistics
        if let Some(ref network_config) = self.shared.graph.network_config {
            let num_sockets = network_config.num_sockets;
            let mut total_drops = 0;
            println!("\nPacket Drop Statistics:");
            for socket_id in 0..num_sockets {
                let drops = self.shared.net.packet_drop_counters[socket_id].load(Ordering::SeqCst);
                total_drops += drops;
                if drops > 0 {
                    println!("  Socket {}: {} packets dropped", socket_id, drops);
                }
            }
            if total_drops == 0 {
                println!("  No packets dropped!");
            } else {
                println!(
                    "  TOTAL: {} packets dropped across all sockets",
                    total_drops
                );
            }
        }

        let dropped_frames = self.shared.net.dropped_streams.load(Ordering::SeqCst);
        if dropped_frames > 0 {
            println!("\nDropped Frame Statistics:");
            println!(
                "  TOTAL: {} frames dropped (no available slots)",
                dropped_frames
            );
        }
    }
}
