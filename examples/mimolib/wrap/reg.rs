use crate::wrappers::*;
use synstream_types::*;

pub fn get_func(func_name: &str) -> Option<CmPtr> {
    match func_name {
        // Built-in SynStream network functions
        "get_frame_id" => Some(get_frame_id_wrap),
        "create_config" => Some(create_config_wrap),
        "create_packet_config" => Some(create_packet_config_wrap),
        "get_packets_per_frame" => Some(get_packets_per_frame_wrap),
        "get_antennas" => Some(get_antennas_wrap),
        "is_pilot" => Some(is_pilot_wrap),
        "get_pilot_symbols" => Some(get_pilot_symbols_wrap),
        "get_uplink_symbols" => Some(get_uplink_symbols_wrap),
        "total_pilot_symbols" => Some(total_pilot_symbols_wrap),
        "total_uplink_symbols" => Some(total_uplink_symbols_wrap),
        "get_packet_length" => Some(get_packet_length_wrap),
        "get_server_address" => Some(get_server_address_wrap),
        "get_base_port" => Some(get_base_port_wrap),
        "process_packet" => Some(process_packet_wrap),
        "init_udp_socket" => Some(init_udp_socket_wrap),
        "receive_packet" => Some(receive_packet_wrap),
        "create_framestats" => Some(create_framestats_wrap),
        "create_ul_base_scs" => Some(create_ul_base_scs_wrap),
        "ul_base_scs_len" => Some(ul_base_scs_len_wrap),
        "beam_events_per_symbol" => Some(beam_events_per_symbol_wrap),
        "create_demul_base_scs" => Some(create_demul_base_scs_wrap),
        "get_ul_symbol" => Some(get_ul_symbol_wrap),
        "total_demul_tasks" => Some(total_demul_tasks_wrap),
        "create_cb_ids" => Some(create_cb_ids_wrap),
        "cb_ids_len" => Some(cb_ids_len_wrap),
        "paired_cb_symbol" => Some(paired_cb_symbol_wrap),
        "total_decode_tasks" => Some(total_decode_tasks_wrap),
        "get_packet_slot" => Some(get_packet_slot_wrap),
        "get_pilot_packet_count" => Some(get_pilot_packet_count_wrap),
        "demul_events_per_symbol" => Some(demul_events_per_symbol_wrap),
        "decode_tasks_per_symbol" => Some(decode_tasks_per_symbol_wrap),
        "create_fft_struct" => Some(create_fft_struct_wrap),
        "create_beam_struct" => Some(create_beam_struct_wrap),
        "create_demul_struct" => Some(create_demul_struct_wrap),
        "create_decode_struct" => Some(create_decode_struct_wrap),
        "create_fft_buffer" => Some(create_fft_buffer_wrap),
        "create_csi_buffer" => Some(create_csi_buffer_wrap),
        "create_demod_buffers" => Some(create_demod_buffers_wrap),
        "create_decode_buffers" => Some(create_decode_buffers_wrap),
        "create_ul_beam_matrices" => Some(create_ul_beam_matrices_wrap),
        "fft_op" => Some(fft_op_wrap),
        "fft_comb" => Some(fft_comb_wrap),
        "csi_op" => Some(csi_op_wrap),
        "beam_op" => Some(beam_op_wrap),
        "demul_op" => Some(demul_op_wrap),
        "decode_op" => Some(decode_op_wrap),
        "write_buffers_to_file" => Some(write_buffers_to_file_wrap),
        _ => {
            println!("Function {} not found", func_name);
            panic!("Panicking...");
        }
    }
}
