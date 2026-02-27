use crate::common::config::Config;
use std::net::UdpSocket;
use synstream_types::CmTypes;

pub fn create_udp_socket(ip: &str, port: i32) -> UdpSocket {
    let socket = UdpSocket::bind(format!("{}:{}", ip, port)).expect("Could not bind socket");
    socket
}

#[no_mangle]
pub fn init_udp_socket(config: &CmTypes, index: usize) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let init_port = config_ref.bs_server_port();
            let bs_addr = config_ref.bs_server_addr().to_string();
            let udp_port = init_port + index as i32;

            let udp_socket = create_udp_socket(&bs_addr, udp_port);
            CmTypes::from_any(udp_socket)
        })
        .expect("Failed to access Config struct or wrong type")
}
