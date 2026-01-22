//! Built-in network initialization functions
//! These are registered as native SynStream functions and callable from JSON

use std::net::{TcpListener, UdpSocket};
use synstream_types::CmTypes;

/// Bind single UDP socket
/// JSON signature: synstream::bind_udp(address: String, port: usize) -> CmTypes::Any(UdpSocket)
#[no_mangle]
pub fn synstream_bind_udp(args: Vec<CmTypes>) -> CmTypes {
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

    println!("Syn-Net: Bound UDP socket: {}", bind_addr);
    CmTypes::from_any(socket)
}

/// Bind range of sequential UDP sockets (for multi-antenna systems)
/// JSON signature: synstream::bind_udp_range(address: String, start_port: usize, count: usize)
///                 -> CmTypes::VecAny(Vec<UdpSocket>)
#[no_mangle]
pub fn synstream_bind_udp_range(args: Vec<CmTypes>) -> CmTypes {
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

    println!(
        "SynRt - Bound {} UDP sockets: {}:{}-{}",
        count,
        address,
        start_port,
        start_port + count - 1
    );

    CmTypes::from_any_vec(sockets)
}

/// Bind single TCP listener
/// JSON signature: synstream::bind_tcp(address: String, port: usize) -> CmTypes::Any(TcpListener)
#[no_mangle]
pub fn synstream_bind_tcp(args: Vec<CmTypes>) -> CmTypes {
    assert_eq!(
        args.len(),
        2,
        "synstream::bind_tcp(address: String, port: usize)"
    );

    let address = args[0]
        .as_string()
        .expect("arg[0] must be String (IP address)");
    let port = args[1]
        .valid_number_to_usize()
        .expect("arg[1] must be numeric (port)");

    let bind_addr = format!("{}:{}", address, port);
    let listener = TcpListener::bind(&bind_addr)
        .unwrap_or_else(|e| panic!("Failed to bind TCP listener {}: {}", bind_addr, e));

    println!("Syn-Net: Bound TCP listener: {}", bind_addr);
    CmTypes::from_any(listener)
}

/// Extract frame ID from packet bytes at fixed offset (little-endian)
/// Replaces user-provided extract_frame_id_from_bytes()
#[inline]
pub fn extract_frame_id_fixed(packet_bytes: &[u8], offset: usize, length: usize) -> usize {
    assert!(
        offset + length <= packet_bytes.len(),
        "Frame ID offset {} + length {} > packet size {}",
        offset,
        length,
        packet_bytes.len()
    );

    // Use slice pattern matching for optimal performance
    match length {
        8 => {
            let bytes: [u8; 8] = packet_bytes[offset..offset + 8]
                .try_into()
                .expect("slice length mismatch");
            u64::from_le_bytes(bytes) as usize
        }
        4 => {
            let bytes: [u8; 4] = packet_bytes[offset..offset + 4]
                .try_into()
                .expect("slice length mismatch");
            u32::from_le_bytes(bytes) as usize
        }
        2 => {
            let bytes: [u8; 2] = packet_bytes[offset..offset + 2]
                .try_into()
                .expect("slice length mismatch");
            u16::from_le_bytes(bytes) as usize
        }
        1 => packet_bytes[offset] as usize,
        _ => panic!(
            "Unsupported frame ID length: {} (must be 1, 2, 4, or 8 bytes)",
            length
        ),
    }
}
