#![allow(non_snake_case)]
#![allow(improper_ctypes_definitions)]
pub mod common {
    pub mod comms_constants;
    pub mod comms_lib;
    pub mod config;
    pub mod framestats;
    pub mod ldpc_config;
    pub mod structures;
    pub mod symbols;
    pub mod utils;
    pub mod utils_ldpc;
    pub mod utils_rdtsc;
}

pub mod bindings {
    pub mod beamfuncs_bindings;
    pub mod demod_bindings;
    pub mod fftfuncs_bindings;
    pub mod mkl_bindings;
}

pub mod beam_lib;
pub mod buffer_lib;
pub mod csi_lib;
pub mod demul_lib;
pub mod fft_lib;
pub mod modulation;
pub mod packet_lib;

use buffer_lib::DemodBuffer;
use common::config::Config;
use common::framestats::FrameStats;
use packet_lib::*;
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

/// Parse raw packet bytes into a Packet struct wrapped in CmTypes.
/// The runtime wraps the raw UDP buffer in CmTypes::Bytes before calling this.
#[no_mangle]
pub fn process_packet(bytes_cm: &CmTypes) -> CmTypes {
    if let CmTypes::Bytes(arc) = bytes_cm {
        CmTypes::from_any(Packet::from_bytes_ref(arc.as_slice()))
    } else {
        panic!("process_packet: expected CmTypes::Bytes, got {:?}", bytes_cm)
    }
}

/// Extract frame ID from an already-parsed Packet (for id_function)
#[no_mangle]
pub fn get_frame_id(packet: &CmTypes) -> usize {
    packet
        .with_any(|packet_ref: &Packet| packet_ref.frame_id as usize)
        .expect("Failed to access Packet struct or wrong type")
}

/// Deterministic packet index from packet content.
/// Returns: symbol_id * antennas + antenna_id
/// Used as index_function in network config for content-based packet ordering.
/// Note: symbol_id is the position in the frame schedule (includes all symbol types)
#[no_mangle]
pub fn get_packet_slot(packet: &CmTypes, config: &CmTypes) -> usize {
    packet
        .with_any(|packet_ref: &Packet| {
            config
                .with_any(|config_ref: &Config| {
                    let symbol_id = packet_ref.symbol_id as usize;
                    let ant_id = packet_ref.ant_id as usize;
                    // symbol_id is already the position in the frame schedule
                    // No need to look it up - just use it directly
                    symbol_id * config_ref.bs_ant_num() + ant_id
                })
                .expect("Failed to access Config struct or wrong type")
        })
        .expect("Failed to access Packet struct or wrong type")
}

#[tomii_export]
pub fn create_config(config_file: String) -> Config {
    let mut config = Config::new(&config_file);
    config.Gen_pilots();
    config.UpdateUlMCS();
    config.ScheduleInit();
    config
}

#[tomii_export]
pub fn create_packet_config(config: &Config) -> PacketConfig {
    PacketConfig::new(config)
}

#[tomii_export]
pub fn get_packets_per_frame(config: &Config, packet_config: &PacketConfig) -> usize {
    packet_config.schedule_length * config.bs_ant_num()
}

#[tomii_export]
pub fn get_antennas(config: &Config) -> usize {
    config.bs_ant_num()
}

#[tomii_export]
pub fn get_packet_length(packet_config: &PacketConfig) -> usize {
    packet_config.packet_length
}

/// Extract server address from Config (for network initialization)
#[no_mangle]
pub fn get_server_address(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            CmTypes::new_string(config_ref.bs_server_addr().to_string())
        })
        .expect("Failed to access Config struct or wrong type")
}

/// Extract base port from Config (for network initialization)
#[no_mangle]
pub fn get_base_port(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| CmTypes::Usize(config_ref.bs_server_port() as usize))
        .expect("Failed to access Config struct or wrong type")
}

#[tomii_export]
pub fn create_framestats(config: &Config) -> FrameStats {
    FrameStats::new(config.frame_schedule().to_string())
}

#[tomii_export]
pub fn get_pilot_symbols(framestats: &FrameStats) -> usize {
    framestats.NumPilotSyms()
}

#[tomii_export]
pub fn get_uplink_symbols(framestats: &FrameStats) -> usize {
    framestats.NumUlSyms()
}

#[tomii_export]
pub fn total_pilot_symbols(config: &Config, framestats: &FrameStats) -> usize {
    framestats.NumPilotSyms() * config.bs_ant_num()
}

#[tomii_export]
pub fn total_uplink_symbols(config: &Config, framestats: &FrameStats) -> usize {
    framestats.NumUlSyms() * config.bs_ant_num()
}

#[tomii_export]
pub fn get_pilot_packet_count(config: &Config, framestats: &FrameStats) -> usize {
    framestats.NumPilotSyms() * config.bs_ant_num()
}

#[tomii_export]
pub fn dump_demod_if_env(demod_buffers: &DemodBuffer) {
    let path = match std::env::var("TOMII_VERIFY_PATH") {
        Ok(p) if !p.is_empty() => p,
        _ => return,
    };
    std::fs::write(&path, demod_buffers.flat_bytes())
        .unwrap_or_else(|e| panic!("dump_demod_if_env: write to {path} failed: {e}"));
}

