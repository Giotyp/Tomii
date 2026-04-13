//! Built-in network initialization functions
//! These are registered as native SynStream functions and callable from JSON

use std::net::UdpSocket;
use synstream_types::CmTypes;

/// Bind single UDP socket
/// JSON signature: synstream::bind_udp(address: String, port: usize) -> CmTypes::Any(UdpSocket)
#[no_mangle]
pub fn synstream_bind_udp(args: &[CmTypes]) -> CmTypes {
    assert_eq!(
        args.len(),
        2,
        "synstream::bind_udp(address: String, port: usize)"
    );

    let address = args[0]
        .as_string()
        .expect("arg[0] must be String (IP address)");
    let port = args[1]
        .valid_number_to_usize()
        .expect("arg[1] must be numeric (port)");

    let bind_addr = format!("{}:{}", address, port);
    let socket = UdpSocket::bind(&bind_addr)
        .unwrap_or_else(|e| panic!("Failed to bind UDP socket {}: {}", bind_addr, e));

    socket
        .set_nonblocking(false)
        .expect("Failed to set blocking mode");

    tracing::info!(addr = %bind_addr, "bound UDP socket");
    CmTypes::from_any(socket)
}

/// Bind range of sequential UDP sockets (for multi-antenna systems)
/// JSON signature: synstream::bind_udp_range(address: String, start_port: usize, count: usize)
///                 -> CmTypes::VecAny(Vec<UdpSocket>)
#[no_mangle]
pub fn synstream_bind_udp_range(args: &[CmTypes]) -> CmTypes {
    assert_eq!(
        args.len(),
        3,
        "synstream::bind_udp_range(address: String, start_port: usize, count: usize)"
    );

    let address = args[0]
        .as_string()
        .expect("arg[0] must be String (IP address)");
    let start_port = args[1]
        .valid_number_to_usize()
        .expect("arg[1] must be numeric (start port)");
    let count = args[2]
        .valid_number_to_usize()
        .expect("arg[2] must be numeric (socket count)");

    let mut sockets = Vec::with_capacity(count);

    for i in 0..count {
        let port = start_port + i;
        let bind_addr = format!("{}:{}", address, port);

        let socket = UdpSocket::bind(&bind_addr)
            .unwrap_or_else(|e| panic!("Failed to bind UDP socket {}: {}", bind_addr, e));

        socket
            .set_nonblocking(false)
            .expect("Failed to set blocking mode");

        sockets.push(socket);
    }

    tracing::info!(
        count,
        address = %address,
        start_port,
        end_port = start_port + count - 1,
        "bound UDP sockets"
    );

    CmTypes::from_any_vec(sockets)
}
