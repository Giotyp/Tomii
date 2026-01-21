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
    /// Which receiver/socket produced this packet
    pub antenna_id: usize,
    /// Slot assignment (frame_id % total_slots)
    pub slot: usize,
    /// Raw packet data
    pub packet_bytes: Vec<u8>,
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

/// Network configuration parsed from graph JSON
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// Socket type (UDP or TCP)
    pub socket_type: SocketType,
    /// Number of sockets to create/bind
    pub num_sockets: usize,
    /// Fixed packet size in bytes
    pub packet_length: usize,
    /// SPSC channel capacity per socket
    pub buffer_depth: usize,

    // Socket reference methods (mutually exclusive)
    pub socket_refs: Option<Vec<String>>,
    pub socket_range_ref: Option<String>,

    // Fixed-offset frame ID extraction
    pub frame_id_offset: Option<usize>,
    pub frame_id_length: Option<usize>,

    /// User-defined function to parse raw packet bytes into structured data
    /// Signature: fn(packet_bytes: &[u8]) -> CmTypes
    pub extract_packet: String,

    /// First graph node to receive parsed packets
    pub first_processing_node: String,

    /// DEPRECATED: Initialization function name for socket creation
    #[deprecated(note = "Use socket_refs or socket_range_ref instead")]
    pub socket_initializer: Option<String>,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        #[allow(deprecated)]
        NetworkConfig {
            socket_type: SocketType::Udp,
            num_sockets: 1,
            packet_length: 1500,
            buffer_depth: 128,
            socket_refs: None,
            socket_range_ref: None,
            frame_id_offset: None,
            frame_id_length: None,
            extract_packet: String::from("process_packet"),
            first_processing_node: String::new(),
            socket_initializer: Some(String::from("init_udp_socket")),
        }
    }
}

/// Extract frame ID from raw packet bytes using user-provided function
///
/// **DEPRECATED:** Use `NetworkConfig.frame_id_offset` and `frame_id_length` with
/// built-in `extract_frame_id_fixed()` instead. This function is maintained for
/// backward compatibility only.
///
/// # Safety
///
/// This function loads and calls a user-provided FFI function.
/// The user library must export `extract_frame_id_from_bytes` with signature:
/// `pub extern "C" fn extract_frame_id_from_bytes(ptr: *const u8, len: usize) -> usize`
pub unsafe fn extract_frame_id(packet_bytes: &[u8], dylib_path: &str) -> usize {
    use libloading::{Library, Symbol};

    // Load user library
    let lib =
        Library::new(dylib_path).expect("Failed to load user library for frame ID extraction");

    // Get user's extract_frame_id_from_bytes function
    let func: Symbol<unsafe extern "C" fn(*const u8, usize) -> usize> = lib
        .get(b"extract_frame_id_from_bytes")
        .expect("User library must export 'extract_frame_id_from_bytes' when using network_config");

    // Call user function with packet bytes
    func(packet_bytes.as_ptr(), packet_bytes.len())
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

    let network_config = shared
        .network_config
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

                // Extract frame ID (fixed-offset if configured, else legacy user function)
                let frame_id = if let (Some(offset), Some(length)) = (
                    network_config.frame_id_offset,
                    network_config.frame_id_length,
                ) {
                    // NEW: Use built-in fixed-offset extraction
                    crate::network_funcs::extract_frame_id_fixed(&buffer[..size], offset, length)
                } else {
                    // LEGACY: Fall back to user-provided dylib function
                    unsafe { extract_frame_id(&buffer, &dylib_path) }
                };
                let slot = frame_id % slots;

                // Create message
                let msg = PacketMessage {
                    antenna_id: socket_id,
                    slot,
                    packet_bytes: buffer.clone(),
                    timestamp: crate::utils_rdtsc::rdtsc(),
                };

                // Try send (non-blocking to avoid stalling receiver)
                if tx.try_send(msg).is_err() {
                    drop_counter.fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "Receiver {}: channel full, packet dropped (frame_id={})",
                        socket_id, frame_id
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

    let network_config = shared
        .network_config
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

                    // Extract frame ID (fixed-offset if configured, else legacy user function)
                    let frame_id = if let (Some(offset), Some(length)) = (
                        network_config.frame_id_offset,
                        network_config.frame_id_length,
                    ) {
                        // NEW: Use built-in fixed-offset extraction
                        crate::network_funcs::extract_frame_id_fixed(
                            &buffer[..size],
                            offset,
                            length,
                        )
                    } else {
                        // LEGACY: Fall back to user-provided dylib function
                        unsafe { extract_frame_id(buffer, &dylib_path) }
                    };
                    let slot = frame_id % slots;

                    // Create message
                    let msg = PacketMessage {
                        antenna_id: socket_id,
                        slot,
                        packet_bytes: buffer.clone(),
                        timestamp: crate::utils_rdtsc::rdtsc(),
                    };

                    // Try send (non-blocking)
                    if tx.try_send(msg).is_err() {
                        drop_counter.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Receiver thread {} socket {}: channel full, packet dropped (frame_id={})",
                                  thread_id, socket_id, frame_id);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packet_message_creation() {
        let msg = PacketMessage {
            antenna_id: 0,
            slot: 5,
            packet_bytes: vec![1, 2, 3, 4],
            timestamp: 1234567890,
        };

        assert_eq!(msg.antenna_id, 0);
        assert_eq!(msg.slot, 5);
        assert_eq!(msg.packet_bytes.len(), 4);
    }

    #[test]
    fn test_network_config_default() {
        let config = NetworkConfig::default();
        assert_eq!(config.packet_length, 1500);
        assert_eq!(config.buffer_depth, 128);
        assert_eq!(config.num_sockets, 1);
    }
}
