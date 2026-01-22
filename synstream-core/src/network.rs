//! Network packet reception infrastructure.
//!
//! This module provides dedicated receiver threads that continuously
//! poll UDP/TCP sockets and inject received packets directly into
//! the SynStream resolution system via lock-free SPSC channels.
//!
//! ## Architecture
//!
//! ```text
//! [Receiver Thread] → recv() → [SPSC Channel] → [Resolution] → [Process Node]
//! ```
//!
//! ## Usage
//!
//! 1. User defines `network_config` in graph JSON
//! 2. SynStream creates sockets and spawns receiver threads
//! 3. Threads continuously receive packets and forward to resolution
//! 4. Resolution injects packets and triggers downstream processing

use std::net::UdpSocket;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::debug::print_debug;
use crate::runtime_funcs::SharedData;

/// Raw packet message forwarded from receiver thread to resolution
#[derive(Debug, Clone)]
pub struct PacketMessage {
    pub packet_bytes: Vec<u8>,
    pub socket_id: usize,
    /// Reception timestamp (rdtsc or micros)
    pub timestamp: u64,
}

/// Network socket types supported by SynStream
#[derive(Debug, Clone)]
pub enum SocketType {
    Udp,
    // Tcp, // Future support
}

/// Wrapper for different socket types
pub enum NetworkSocket {
    Udp(UdpSocket),
    // Tcp(TcpStream), // Future support
}

impl NetworkSocket {
    /// Receive packet from socket into provided buffer
    pub fn recv(&self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            NetworkSocket::Udp(sock) => sock.recv(buffer),
        }
    }

    /// Set read timeout for socket operations
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        match self {
            NetworkSocket::Udp(sock) => sock.set_read_timeout(timeout),
        }
    }
}

pub fn bind_udp_socket_range(address: &str, start_port: usize, count: usize) -> Vec<NetworkSocket> {
    let mut sockets = Vec::with_capacity(count);
    for i in 0..count {
        let port = start_port + i;
        let bind_addr = format!("{}:{}", address, port);

        let socket = UdpSocket::bind(&bind_addr)
            .unwrap_or_else(|e| panic!("Failed to bind UDP socket {}: {}", bind_addr, e));

        socket
            .set_nonblocking(false)
            .expect("Failed to set blocking mode");

        sockets.push(NetworkSocket::Udp(socket));
    }
    sockets
}

/// Dedicated receiver loop for single socket (optimal: 1 thread per socket)
///
/// This function runs in a dedicated OS thread, pinned to a specific core.
/// It continuously receives packets, extracts frame IDs, and forwards to resolution.
pub fn single_socket_receiver_loop(
    shared: Arc<SharedData>,
    socket_id: usize,
    core_id: usize,
    dylib_path: String,
) {
    // Pin to core
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if core_id < core_ids.len() {
            core_affinity::set_for_current(core_ids[core_id]);
        }
    }

    // Set thread name
    let _ = thread::Builder::new()
        .name(format!("rx-{}", socket_id))
        .spawn(|| {});

    // Get socket and channel references
    let socket = &shared.receiver_sockets[socket_id];
    let tx = &shared.packet_senders[socket_id];
    let drop_counter = &shared.packet_drop_counters[socket_id];
    let shutdown = &shared.shutdown_flag;

    let network_config = shared.graph
        .network_config()
        .as_ref()
        .expect("Network config must be present for receiver threads");
    let packet_length = network_config.packet_length;
    let slots = shared.slots;

    let mut buffer = vec![0u8; packet_length];

    print_debug(|| format!("Receiver thread {} started on core {}", socket_id, core_id));

    loop {
        // Check shutdown signal
        if shutdown.load(Ordering::Relaxed) {
            print_debug(|| format!("Receiver {} shutting down", socket_id));
            break;
        }

        // Receive packet (blocking)
        match socket.recv(&mut buffer) {
            Ok(size) => {
                if size != packet_length {
                    eprintln!(
                        "Receiver {}: unexpected packet size {} != {}",
                        socket_id, size, packet_length
                    );
                    continue;
                }

                // Create message
                let msg = PacketMessage {
                    packet_bytes: buffer.clone(),
                    socket_id,
                    timestamp: crate::utils_rdtsc::rdtsc(),
                };

                // Try send (non-blocking to avoid stalling receiver)
                if tx.try_send(msg).is_err() {
                    drop_counter.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "Receiver {}: channel full, packet dropped",
                        socket_id
                    );
                }
            }
            Err(e) => {
                eprintln!("Receiver {}: recv error: {}", socket_id, e);
                // For UDP, this is typically fatal; break and let orchestration restart
                break;
            }
        }
    }

    print_debug(|| format!("Receiver thread {} exited", socket_id));
}

/// Receiver loop for multiple sockets (round-robin polling when nrx < num_sockets)
///
/// This function handles the case where we have fewer receiver threads than sockets.
/// Each thread polls multiple sockets with short timeouts to avoid head-of-line blocking.
pub fn multi_socket_receiver_loop(
    shared: Arc<SharedData>,
    thread_id: usize,
    socket_range: std::ops::Range<usize>,
    core_id: usize,
    dylib_path: String,
) {
    // Pin to core
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if core_id < core_ids.len() {
            core_affinity::set_for_current(core_ids[core_id]);
        }
    }

    // Set thread name
    let _ = thread::Builder::new()
        .name(format!("rx-multi-{}", thread_id))
        .spawn(|| {});

    let network_config = shared.graph
        .network_config()
        .as_ref()
        .expect("Network config must be present for receiver threads");
    let packet_length = network_config.packet_length;
    let slots = shared.slots;

    // Pre-allocate buffers for each socket (avoid allocation per recv)
    let mut buffers: Vec<Vec<u8>> = socket_range
        .clone()
        .map(|_| vec![0u8; packet_length])
        .collect();

    let shutdown = &shared.shutdown_flag;
    let read_timeout = Duration::from_micros(100); // Tunable: balance latency vs CPU

    print_debug(|| {
        format!(
            "Multi-socket receiver thread {} polling sockets {:?} on core {}",
            thread_id, socket_range, core_id
        )
    });

    loop {
        // Round-robin poll all assigned sockets
        for (local_idx, socket_id) in socket_range.clone().enumerate() {
            // Check shutdown (amortized across sockets)
            if shutdown.load(Ordering::Relaxed) {
                print_debug(|| format!("Multi-socket receiver {} shutting down", thread_id));
                return;
            }

            let socket = &shared.receiver_sockets[socket_id];
            let buffer = &mut buffers[local_idx];
            let tx = &shared.packet_senders[socket_id];
            let drop_counter = &shared.packet_drop_counters[socket_id];

            // Set short read timeout to avoid blocking on one socket
            let _ = socket.set_read_timeout(Some(read_timeout));

            match socket.recv(buffer) {
                Ok(size) => {
                    if size != packet_length {
                        eprintln!(
                            "Receiver thread {} socket {}: unexpected packet size {} != {}",
                            thread_id, socket_id, size, packet_length
                        );
                        continue;
                    }

                    // Create message
                    let msg = PacketMessage {
                        packet_bytes: buffer.clone(),
                        socket_id,
                        timestamp: crate::utils_rdtsc::rdtsc(),
                    };

                    // Try send (non-blocking)
                    if tx.try_send(msg).is_err() {
                        drop_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Receiver thread {} socket {}: channel full, packet dropped",
                                  thread_id, socket_id);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Timeout - move to next socket (normal in round-robin)
                    continue;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    // Timeout - move to next socket
                    continue;
                }
                Err(e) => {
                    eprintln!(
                        "Receiver thread {} socket {}: recv error: {}",
                        thread_id, socket_id, e
                    );
                    // Fatal error - exit thread
                    return;
                }
            }
        }
    }
}
