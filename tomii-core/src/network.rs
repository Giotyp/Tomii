//! Network packet reception infrastructure.
//!
//! This module provides dedicated receiver threads that continuously
//! poll UDP/TCP sockets and inject received packets directly into
//! the Τομί resolution system via lock-free SPSC channels.
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
//! 2. Τομί creates sockets and spawns receiver threads
//! 3. Threads continuously receive packets and forward to resolution
//! 4. Resolution injects packets and triggers downstream processing

use std::net::UdpSocket;
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Shared between the listener and the receiver threads of the process handing over.
#[derive(Debug, Default)]
pub struct HandoverState {
    /// Set when a takeover process connects: receivers stop reading their sockets.
    pub freeze: AtomicBool,
    /// Receiver threads that have stopped reading.
    pub frozen: AtomicUsize,
    /// Set once the sockets have been sent; the run loop then winds down.
    pub handed_over: AtomicBool,
}

/// The receiver sockets: bound at build time, or published after a live takeover.
/// Receiver threads may start before it is set and wait for it.
pub type SocketCell = Arc<std::sync::OnceLock<Arc<Vec<NetworkSocket>>>>;

/// Raw packet message forwarded from receiver thread to resolution
#[derive(Debug, Clone)]
pub struct PacketMessage {
    pub packet_bytes: Vec<u8>,
    pub socket_id: usize,
    /// Reception timestamp (rdtsc or micros)
    pub timestamp: Instant,
    /// Receiver core ID (for recording)
    pub receiver_core_id: usize,
    /// Set when the receiver thread already ran `extract_packet` (RX-side decode);
    /// `packet_bytes` is then empty and the resolution thread skips decoding.
    pub decoded: Option<tomii_types::CmTypes>,
}

/// Network socket types supported by Τομί
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
    /// The underlying descriptor (for a live handover to another process).
    pub fn raw_fd(&self) -> std::os::unix::io::RawFd {
        match self {
            NetworkSocket::Udp(s) => s.as_raw_fd(),
        }
    }

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

    /// Set non-blocking mode — use for dedicated busy-poll receiver threads.
    pub fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        match self {
            NetworkSocket::Udp(sock) => sock.set_nonblocking(nonblocking),
        }
    }
}

/// Set the SO_RCVBUF size on a raw socket fd.
/// The kernel may cap this at net.core.rmem_max.
fn set_socket_recv_buffer(socket: &UdpSocket, size: usize) {
    let fd = socket.as_raw_fd();
    let buf_size = size as libc::c_int;
    // SAFETY: `fd` is a valid socket descriptor for the lifetime of `socket`; `buf_size` is a
    // stack-allocated `c_int` whose address and size are correctly passed to `setsockopt`.
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
        tracing::warn!(
            size,
            error = %std::io::Error::last_os_error(),
            "failed to set SO_RCVBUF"
        );
    }
}

pub fn bind_udp_socket_range(
    address: &str,
    start_port: usize,
    count: usize,
    socket_recv_buf_bytes: usize,
) -> Vec<NetworkSocket> {
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
        set_socket_recv_buffer(&socket, socket_recv_buf_bytes);

        sockets.push(NetworkSocket::Udp(socket));
    }
    tracing::info!(
        start_port,
        end_port = start_port + count - 1,
        address,
        recv_buf_mb = socket_recv_buf_bytes / (1024 * 1024),
        "bound UDP sockets"
    );
    sockets
}

/// Wraps bound UDP sockets received from another process (see `crate::handover`).
pub fn sockets_from_fds(fds: Vec<std::os::unix::io::RawFd>) -> Vec<NetworkSocket> {
    use std::os::unix::io::FromRawFd;
    fds.into_iter()
        .map(|fd| {
            // SAFETY: each descriptor was received via SCM_RIGHTS and is owned by us now.
            let socket = unsafe { UdpSocket::from_raw_fd(fd) };
            socket
                .set_nonblocking(false)
                .expect("Failed to set blocking mode");
            NetworkSocket::Udp(socket)
        })
        .collect()
}

/// Receiver loop for a range of sockets (round-robin polling).
///
/// This function handles the case where we have fewer receiver threads than sockets.
/// Each thread polls multiple sockets with short timeouts to avoid head-of-line blocking.
///
/// Thread naming is handled by the caller via `thread::Builder::name()`.
#[allow(clippy::too_many_arguments)]
pub fn multi_socket_receiver_loop(
    packet_length: usize,
    recv_pool_size: usize,
    shutdown: Arc<AtomicBool>,
    tx: flume::Sender<PacketMessage>,
    sockets: SocketCell,
    drop_counters: Arc<Vec<AtomicUsize>>,
    thread_id: usize,
    socket_range: std::ops::Range<usize>,
    core_id: usize,
    return_rxs: Vec<flume::Receiver<Vec<u8>>>,
    decode: Option<tomii_types::CmPtr>,
    handover: Arc<HandoverState>,
) {
    // Pin this receiver to its dedicated physical core (`core_id` =
    // receiver_core_offset + thread_id, the core the allocator reserved).
    //
    // Do NOT derive the target from `core_affinity::get_core_ids()`:
    // get_core_ids() returns the CURRENT thread's affinity set, and receiver
    // threads are spawned from the main thread which is pinned to core 0 — so
    // inside a receiver thread get_core_ids() returns only `[core 0]`, the
    // `core_id < len` guard then fails, the pin is skipped, and all receivers
    // inherit main's core 0 (starving epoll busy-poll ingest). Workers avoid
    // this because they pin from the scheduler's allocation, not a per-thread
    // get_core_ids(). Pinning directly to the physical core via `CoreId { id }`
    // works regardless of the inherited mask: sched_setaffinity to a core in
    // the process cpuset is permitted (workers already run on 6..31).
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id });

    // Pre-allocate one local pool per socket in this thread's range.
    // return_rxs[i] corresponds to socket_range.start + i.
    let range_start = socket_range.start;
    let range_len = socket_range.end - socket_range.start;
    let mut local_pools: Vec<Vec<Vec<u8>>> = (0..range_len)
        .map(|_| {
            (0..recv_pool_size)
                .map(|_| vec![0u8; packet_length])
                .collect()
        })
        .collect();

    // Wait for the sockets (set after a live takeover); the pools above are
    // already allocated, so reading starts the moment they are published.
    let sockets = loop {
        if let Some(s) = sockets.get() {
            break Arc::clone(s);
        }
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        std::thread::sleep(Duration::from_micros(20));
    };

    let read_timeout = Duration::from_micros(1);

    // Set read timeout ONCE per socket during init — setsockopt on every
    // poll iteration was a syscall (~100-500 ns) burned per socket per loop.
    for socket_id in socket_range.clone() {
        let socket = &sockets[socket_id];
        let _ = socket.set_read_timeout(Some(read_timeout));
    }

    tracing::info!(
        thread_id,
        ?socket_range,
        core_id,
        "multi-socket receiver thread started"
    );

    let tx = &tx;

    let mut first_packet_received: bool = false;
    let mut first_packet_timestamp: Instant = Instant::now();
    let mut last_packet_timestamp: Instant = Instant::now();

    loop {
        // Check shutdown once per full round-robin sweep, not per socket
        if shutdown.load(Ordering::Relaxed) {
            let last_first_dur = last_packet_timestamp.duration_since(first_packet_timestamp);
            tracing::info!(thread_id, duration = ?last_first_dur, "multi-socket receiver shutting down");
            return;
        }
        // Live handover: stop reading; the sockets are about to move to another process.
        if handover.freeze.load(Ordering::Acquire) {
            handover.frozen.fetch_add(1, Ordering::AcqRel);
            tracing::info!(thread_id, "receiver stopped for handover");
            while !shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(1));
            }
            return;
        }

        // Round-robin poll all assigned sockets
        for socket_id in socket_range.clone() {
            let local_idx = socket_id - range_start;
            let socket = &sockets[socket_id];
            let drop_counter = &drop_counters[socket_id];

            // Drain any buffers returned by the resolution thread into this socket's local pool.
            while let Ok(buf) = return_rxs[local_idx].try_recv() {
                local_pools[local_idx].push(buf);
            }

            // Pop from local pool; fall back to fresh allocation on burst.
            let mut packet_bytes = local_pools[local_idx]
                .pop()
                .unwrap_or_else(|| vec![0u8; packet_length]);

            match socket.recv(&mut packet_bytes) {
                Ok(size) => {
                    if size != packet_length {
                        tracing::warn!(
                            thread_id,
                            socket_id,
                            size,
                            packet_length,
                            "unexpected packet size"
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

                    // RX-side decode: run extract_packet here (in parallel across
                    // receiver threads) and recycle the raw buffer locally.
                    let (packet_bytes, decoded) = match decode {
                        Some(dec) => {
                            let raw = tomii_types::CmTypes::from_bytes(packet_bytes);
                            let out = dec(std::slice::from_ref(&raw));
                            if let tomii_types::CmTypes::Bytes(arc) = raw {
                                if let Ok(buf) = Arc::try_unwrap(arc) {
                                    local_pools[local_idx].push(buf);
                                }
                            }
                            (Vec::new(), Some(out))
                        }
                        None => (packet_bytes, None),
                    };

                    let msg = PacketMessage {
                        packet_bytes,
                        socket_id,
                        timestamp: packet_timestamp,
                        receiver_core_id: core_id,
                        decoded,
                    };

                    if tx.try_send(msg).is_err() {
                        drop_counter.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(thread_id, socket_id, "channel full, packet dropped");
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                    continue;
                }
                Err(e) => {
                    tracing::error!(thread_id, socket_id, error = %e, "recv error, exiting");
                    return;
                }
            }
        }
    }
}
