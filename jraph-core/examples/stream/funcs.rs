use std::net::UdpSocket;

pub fn udp_receive(socket: &UdpSocket, packet_length: usize) -> Vec<u8> {
    let mut buf = vec![0u8; packet_length];

    match socket.recv(&mut buf) {
        Ok(_size) => buf,
        Err(e) => panic!("Failed to receive packet: {}", e),
    }
}

pub fn print_packet(packet: Vec<u8>) {
    println!("Received packet: {:?}", packet);
}
