use super::shared_data::SharedData;
use crate::graph::Graph;
use crate::network::{bind_udp_socket_range, NetworkSocket};
use flume;
use parking_lot;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use synstream_types::*;

#[inline]
pub(super) fn process_id_function(shared: &Arc<SharedData>, result: &CmTypes) -> Option<usize> {
    let network_config_opt = shared.graph.network_config();

    if let Some(network_config) = network_config_opt {
        let id_function = network_config.id_function.unwrap();
        // Call the id function - wrap single result in Vec as expected by signature
        let id_result = id_function(&[result.clone()]);

        // Extract stream from the result
        if let Some(new_stream) = id_result.valid_number_to_usize() {
            // Validate stream range
            let current_counter = shared
                .telemetry
                .stream_complete_counter
                .load(Ordering::SeqCst);
            let max_allowed_stream = current_counter + shared.config.slots;

            if new_stream >= max_allowed_stream {
                tracing::warn!(
                    new_stream,
                    max_allowed_stream,
                    current_counter,
                    slots = shared.config.slots,
                    "ID function returned out-of-range stream"
                );
                return None;
            }
            return Some(new_stream);
        } else {
            panic!("ID function did not return a valid number for stream");
        }
    } else {
        None
    }
}

pub(super) fn prepare_network_infrastructure(
    graph: &Graph,
    socket_recv_buf_bytes: usize,
    recv_pool_size: usize,
) -> (
    Arc<Vec<NetworkSocket>>,
    flume::Sender<crate::network::PacketMessage>,
    flume::Receiver<crate::network::PacketMessage>,
    Arc<Vec<AtomicUsize>>,
    Vec<flume::Sender<Vec<u8>>>,
    Vec<parking_lot::Mutex<Option<flume::Receiver<Vec<u8>>>>>,
) {
    if let Some(config_spec) = graph.network_config() {
        let num_sockets = config_spec.num_sockets;
        // Size the channel to absorb 4× stream_packets worth of data per socket.
        // stream_packets × num_sockets ≈ one full frame across all sockets; ×4 gives
        // headroom for multiple concurrent frames and resolution-thread stalls.
        // Minimum 65536 ensures adequate buffering even for small packet counts.
        let channel_cap = (config_spec.stream_packets * 4).max(65536);
        let (packet_sender, packet_receiver) = flume::bounded(channel_cap);

        let receiver_sockets = bind_udp_socket_range(
            &config_spec.address,
            config_spec.start_port,
            num_sockets,
            socket_recv_buf_bytes,
        );

        let packet_drop_counters = (0..num_sockets).map(|_| AtomicUsize::new(0)).collect();

        // Create one SPSC return channel per socket.
        // Resolution thread sends reclaimed buffers to the originating receiver thread,
        // eliminating the shared mutex that was the hot-path contention point.
        // Capacity matches recv_pool_size — if full, buffer drops and
        // the receiver falls back to fresh allocation (burst safety valve).
        let mut buffer_return_senders = Vec::with_capacity(num_sockets);
        let mut buffer_return_receivers = Vec::with_capacity(num_sockets);
        for _ in 0..num_sockets {
            let (tx, rx) = flume::bounded::<Vec<u8>>(recv_pool_size);
            buffer_return_senders.push(tx);
            buffer_return_receivers.push(parking_lot::Mutex::new(Some(rx)));
        }

        (
            Arc::new(receiver_sockets),
            packet_sender,
            packet_receiver,
            Arc::new(packet_drop_counters),
            buffer_return_senders,
            buffer_return_receivers,
        )
    } else {
        let (packet_sender, packet_receiver) = flume::bounded(1);
        (
            Arc::new(Vec::new()),
            packet_sender,
            packet_receiver,
            Arc::new(Vec::new()),
            Vec::new(),
            Vec::new(),
        )
    }
}
