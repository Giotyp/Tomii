//! Live socket handover between two Tomii processes (Linux).
//!
//! A running process started with `--handover-listen PATH` listens on a Unix socket.
//! A replacement process started with `--takeover PATH` finishes its own setup
//! (graph, kernels, threads), then connects. The running process stops its receivers
//! from reading, passes its bound UDP sockets over the Unix socket (`SCM_RIGHTS`), and
//! exits once its complete frames are done. Packets that arrive meanwhile wait in the
//! kernel socket buffers, which move with the sockets, so the replacement continues
//! the stream where the old process stopped (with `join_stream` numbering).

use crate::network::HandoverState;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Linux limits one `SCM_RIGHTS` message to 253 descriptors.
const FDS_PER_MSG: usize = 200;

/// Sends `fds` over `stream` in one or more `SCM_RIGHTS` messages. Each message
/// carries `[count, total]` (u32 LE) as its payload.
pub fn send_fds(stream: &UnixStream, fds: &[RawFd]) -> io::Result<()> {
    let total = fds.len() as u32;
    if fds.is_empty() {
        return send_chunk(stream, &[], total);
    }
    for chunk in fds.chunks(FDS_PER_MSG) {
        send_chunk(stream, chunk, total)?;
    }
    Ok(())
}

fn send_chunk(stream: &UnixStream, fds: &[RawFd], total: u32) -> io::Result<()> {
    let mut payload = [0u8; 8];
    payload[..4].copy_from_slice(&(fds.len() as u32).to_le_bytes());
    payload[4..].copy_from_slice(&total.to_le_bytes());
    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr().cast(),
        iov_len: payload.len(),
    };
    let fd_bytes = std::mem::size_of_val(fds);
    let space = unsafe { libc::CMSG_SPACE(fd_bytes as u32) } as usize;
    let mut cbuf = vec![0u8; space.max(1)];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    if !fds.is_empty() {
        msg.msg_control = cbuf.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        // SAFETY: cbuf holds CMSG_SPACE(fd_bytes) bytes, enough for one header plus the fds.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&msg);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(fd_bytes as u32) as _;
            std::ptr::copy_nonoverlapping(fds.as_ptr(), libc::CMSG_DATA(cmsg).cast(), fds.len());
        }
    }
    // SAFETY: msg points at live iov/control buffers for the duration of the call.
    let n = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Receives descriptors sent by [`send_fds`], until the announced total has arrived.
pub fn recv_fds(stream: &UnixStream) -> io::Result<Vec<RawFd>> {
    let mut out = Vec::new();
    loop {
        let mut payload = [0u8; 8];
        let mut iov = libc::iovec {
            iov_base: payload.as_mut_ptr().cast(),
            iov_len: payload.len(),
        };
        let space = unsafe { libc::CMSG_SPACE((FDS_PER_MSG * std::mem::size_of::<RawFd>()) as u32) }
            as usize;
        let mut cbuf = vec![0u8; space];
        let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cbuf.as_mut_ptr().cast();
        msg.msg_controllen = space as _;
        // SAFETY: msg points at live iov/control buffers for the duration of the call.
        let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, libc::MSG_CMSG_CLOEXEC) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        if n as usize != payload.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "handover: short header",
            ));
        }
        let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
        let total = u32::from_le_bytes(payload[4..].try_into().unwrap()) as usize;
        if count > 0 {
            // SAFETY: the kernel filled the control buffer; walk its headers.
            unsafe {
                let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
                while !cmsg.is_null() {
                    if (*cmsg).cmsg_level == libc::SOL_SOCKET
                        && (*cmsg).cmsg_type == libc::SCM_RIGHTS
                    {
                        let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
                        let n_fds = ((*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize)
                            / std::mem::size_of::<RawFd>();
                        for i in 0..n_fds {
                            out.push(*data.add(i));
                        }
                    }
                    cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
                }
            }
        }
        if out.len() >= total {
            return Ok(out);
        }
    }
}

/// Replacement side: connects to `path` (retrying while the running process sets up its
/// listener) and returns the sockets it hands over, in socket-id order.
pub fn take_over(path: &str, timeout: Duration) -> io::Result<Vec<RawFd>> {
    let deadline = Instant::now() + timeout;
    let stream = loop {
        match UnixStream::connect(path) {
            Ok(s) => break s,
            Err(e) if Instant::now() < deadline => {
                tracing::debug!(path, error = %e, "handover: waiting for the running process");
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => return Err(e),
        }
    };
    let fds = recv_fds(&stream)?;
    tracing::info!(path, sockets = fds.len(), "handover: took over sockets");
    Ok(fds)
}

/// Running side: listens on `path`; on the first connection, stops the receivers,
/// sends `fds`, and marks the handover done. `receivers` is the number of receiver
/// threads that must acknowledge the freeze before the sockets are sent.
pub fn spawn_listener(
    path: String,
    state: Arc<HandoverState>,
    fds: Vec<RawFd>,
    receivers: usize,
) -> io::Result<std::thread::JoinHandle<()>> {
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    tracing::info!(path, "handover: listening");
    std::thread::Builder::new().name("handover".into()).spawn(move || {
        let stream = match listener.accept() {
            Ok((s, _)) => s,
            Err(e) => {
                tracing::error!(error = %e, "handover: accept failed");
                return;
            }
        };
        let t0 = Instant::now();
        state.freeze.store(true, Ordering::Release);
        while state.frozen.load(Ordering::Acquire) < receivers && t0.elapsed() < Duration::from_millis(500) {
            std::thread::yield_now();
        }
        let frozen_after = t0.elapsed();
        match send_fds(&stream, &fds) {
            Ok(()) => {
                state.handed_over.store(true, Ordering::Release);
                tracing::info!(sockets = fds.len(), ?frozen_after, total = ?t0.elapsed(), "handover: sockets sent");
            }
            Err(e) => tracing::error!(error = %e, "handover: sending sockets failed"),
        }
        let _ = std::fs::remove_file(&path);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::os::unix::io::FromRawFd;

    #[test]
    fn udp_sockets_survive_a_handover_with_their_queued_datagrams() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = rx.local_addr().unwrap();
        let tx = UdpSocket::bind("127.0.0.1:0").unwrap();
        tx.send_to(b"queued", addr).unwrap();

        let (a, b) = UnixStream::pair().unwrap();
        send_fds(&a, &[rx.as_raw_fd()]).unwrap();
        let fds = recv_fds(&b).unwrap();
        assert_eq!(fds.len(), 1);
        drop(rx); // the original descriptor closes; the received one keeps the socket
        let taken = unsafe { UdpSocket::from_raw_fd(fds[0]) };
        let mut buf = [0u8; 16];
        let (n, _) = taken.recv_from(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"queued");
    }

    #[test]
    fn many_descriptors_are_sent_in_chunks() {
        let socks: Vec<UdpSocket> = (0..450)
            .map(|_| UdpSocket::bind("127.0.0.1:0").unwrap())
            .collect();
        let fds: Vec<RawFd> = socks.iter().map(|s| s.as_raw_fd()).collect();
        let (a, b) = UnixStream::pair().unwrap();
        let sender = std::thread::spawn(move || send_fds(&a, &fds).unwrap());
        let got = recv_fds(&b).unwrap();
        sender.join().unwrap();
        assert_eq!(got.len(), 450);
        for fd in got {
            unsafe { libc::close(fd) };
        }
    }
}
