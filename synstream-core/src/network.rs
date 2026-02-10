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
use std::os::unix::io::AsRawFd;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Socket receive buffer size (16 MB).
/// Prevents kernel from dropping UDP packets when bursts exceed the default ~208 KB buffer.
/// Requires: sudo sysctl -w net.core.rmem_max=16777216
const SOCKET_RECV_BUF_SIZE: usize = 16 * 1024 * 1024;

use crate::runtime_funcs::SharedData;

/// Raw packet message forwarded from receiver thread to resolution
#[derive(Debug, Clone)]
pub struct PacketMessage {
    pub packet_bytes: Vec<u8>,
    pub socket_id: usize,
    /// Reception timestamp (rdtsc or micros)
    pub timestamp: Instant,
    /// Receiver thread ID (for recording)
    pub receiver_thread_id: usize,
    /// Receiver core ID (for recording)
    pub receiver_core_id: usize,
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

/// Set the SO_RCVBUF size on a raw socket fd.
/// The kernel may cap this at net.core.rmem_max.
fn set_socket_recv_buffer(socket: &UdpSocket, size: usize) {
    let fd = socket.as_raw_fd();
    let buf_size = size as libc::c_int;
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret != 0 {
        eprintln!(
            "Warning: failed to set SO_RCVBUF to {}: {}",
            size,
            std::io::Error::last_os_error()
        );
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

        // Increase kernel receive buffer to absorb packet bursts
        set_socket_recv_buffer(&socket, SOCKET_RECV_BUF_SIZE);

        sockets.push(NetworkSocket::Udp(socket));
    }
    println!(
        "Successfully bound UDP sockets {}-{} on address {} (recv_buf={}MB)",
        start_port,
        start_port + count - 1,
        address,
        SOCKET_RECV_BUF_SIZE / (1024 * 1024)
    );
    sockets
}

/// Dedicated receiver loop for single socket (optimal: 1 thread per socket)
///
/// This function runs in a dedicated OS thread, pinned to a specific core.
/// It continuously receives packets, extracts frame IDs, and forwards to resolution.
///
/// Thread naming is handled by the caller via `thread::Builder::name()`.
pub fn single_socket_receiver_loop(shared: Arc<SharedData>, socket_id: usize, core_id: usize) {
    // Pin to core
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if core_id < core_ids.len() {
            core_affinity::set_for_current(core_ids[core_id]);
        }
    }

    let read_timeout = Duration::from_micros(1);

    // Get socket and channel references
    let socket = &shared.receiver_sockets[socket_id];
    let _ = socket.set_read_timeout(Some(read_timeout));
    let tx = &shared.packet_sender;
    let drop_counter = &shared.packet_drop_counters[socket_id];
    let shutdown = &shared.shutdown_flag;

    let network_config_arc = shared
        .graph
        .network_config()
        .expect("Network config must be present for receiver threads");
    let packet_length = network_config_arc.packet_length;

    println!("Receiver thread {} started on core {}", socket_id, core_id);

    loop {
        // Relaxed is sufficient: eventual visibility of shutdown is fine;
        // a few extra iterations before exit do no harm.
        if shutdown.load(Ordering::Relaxed) {
            println!("Receiver {} shutting down", socket_id);
            break;
        }

        // Allocate fresh buffer per packet — simple, no pool management overhead
        let mut packet_bytes = vec![0u8; packet_length];

        // Receive packet (blocking with read_timeout)
        match socket.recv(&mut packet_bytes) {
            Ok(size) => {
                if size != packet_length {
                    eprintln!(
                        "Receiver {}: unexpected packet size {} != {}",
                        socket_id, size, packet_length
                    );
                    continue;
                }

                let msg = PacketMessage {
                    packet_bytes,
                    socket_id,
                    timestamp: Instant::now(),
                    receiver_thread_id: socket_id,
                    receiver_core_id: core_id,
                };

                // Push to this thread's dedicated channel (no cross-thread CAS)
                if tx.try_send(msg).is_err() {
                    drop_counter.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Receiver {}: channel full, packet dropped", socket_id);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                continue;
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(e) => {
                eprintln!("Receiver {}: recv error: {}", socket_id, e);
                break;
            }
        }
    }

    println!("Receiver thread {} exited", socket_id);
}

/// Receiver loop for multiple sockets (round-robin polling when nrx < num_sockets)
///
/// This function handles the case where we have fewer receiver threads than sockets.
/// Each thread polls multiple sockets with short timeouts to avoid head-of-line blocking.
///
/// Thread naming is handled by the caller via `thread::Builder::name()`.
pub fn multi_socket_receiver_loop(
    shared: Arc<SharedData>,
    thread_id: usize,
    socket_range: std::ops::Range<usize>,
    core_id: usize,
) {
    // Pin to core
    if let Some(core_ids) = core_affinity::get_core_ids() {
        if core_id < core_ids.len() {
            core_affinity::set_for_current(core_ids[core_id]);
        }
    }

    let network_config_arc = shared
        .graph
        .network_config()
        .expect("Network config must be present for receiver threads");
    let packet_length = network_config_arc.packet_length;

    let shutdown = &shared.shutdown_flag;
    let read_timeout = Duration::from_micros(1);

    // Set read timeout ONCE per socket during init — setsockopt on every
    // poll iteration was a syscall (~100-500 ns) burned per socket per loop.
    for socket_id in socket_range.clone() {
        let socket = &shared.receiver_sockets[socket_id];
        let _ = socket.set_read_timeout(Some(read_timeout));
    }

    println!(
        "Multi-socket receiver thread {} polling sockets {:?} on core {}",
        thread_id, socket_range, core_id
    );

    let tx = &shared.packet_sender;

    let mut first_packet_received: bool = false;
    let mut first_packet_timestamp: Instant = Instant::now();
    let mut last_packet_timestamp: Instant = Instant::now();

    loop {
        // Round-robin poll all assigned sockets
        for socket_id in socket_range.clone() {
            if shutdown.load(Ordering::Relaxed) {
                // calculate time from base_instant for first and last packet
                let last_first_dur = last_packet_timestamp.duration_since(first_packet_timestamp);
                println!(
                    "Multi-socket receiver {}: Total receiving: {:?}",
                    thread_id, last_first_dur
                );
                println!("Multi-socket receiver {} shutting down", thread_id);
                return;
            }

            let socket = &shared.receiver_sockets[socket_id];
            let drop_counter = &shared.packet_drop_counters[socket_id];

            // Allocate fresh buffer per packet — simple, no pool management overhead
            let mut packet_bytes = vec![0u8; packet_length];

            match socket.recv(&mut packet_bytes) {
                Ok(size) => {
                    if size != packet_length {
                        eprintln!(
                            "Receiver thread {} socket {}: unexpected packet size {} != {}",
                            thread_id, socket_id, size, packet_length
                        );
                        continue;
                    }
                    let packet_timestamp = Instant::now();

                    if !first_packet_received {
                        first_packet_timestamp = packet_timestamp;
                        first_packet_received = true;
                    } else {
                        last_packet_timestamp = packet_timestamp;
                    }

                    let msg = PacketMessage {
                        packet_bytes,
                        socket_id,
                        timestamp: packet_timestamp,
                        receiver_thread_id: thread_id,
                        receiver_core_id: core_id,
                    };

                    if tx.try_send(msg).is_err() {
                        drop_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "Receiver thread {} socket {}: channel full, packet dropped",
                            thread_id, socket_id
                        );
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    continue;
                }
                Err(e) => {
                    eprintln!(
                        "Receiver thread {} socket {}: recv error: {}",
                        thread_id, socket_id, e
                    );
                    return;
                }
            }
        }
    }
}
