use std::net::UdpSocket;

pub fn create_udp_socket(ip: String, port: u16) -> UdpSocket {
    UdpSocket::bind((ip, port)).expect("Failed to bind UDP socket")
}
