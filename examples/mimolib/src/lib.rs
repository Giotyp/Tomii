#![allow(non_snake_case)]
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
    pub mod phy_ldpc_decoder_5gnr_bindings;
    pub mod scrambler_bindings;
}

pub mod beam_lib;
pub mod buffer_lib;
pub mod csi_lib;
pub mod decode_lib;
pub mod demul_lib;
pub mod fft_lib;
pub mod modulation;
pub mod packet_lib;
pub mod udp;

use buffer_lib::*;
use common::config::Config;
use common::framestats::FrameStats;
use common::symbols::{Direction, FrameWnd, SymbolType};
use common::utils::roundup;
use num_complex::ComplexFloat;
use packet_lib::*;
use std::cmp::min;
use std::fs::File;
use std::io::Write;
use synstream_types::CmTypes;

/// Parse raw packet bytes into a Packet struct wrapped in CmTypes
/// This replaces the old receive_packet() function
/// Uses zero-copy parsing with reference to avoid buffer duplication
#[no_mangle]
pub fn process_packet(packet_bytes: &[u8]) -> CmTypes {
    let packet = Packet::from_bytes_ref(packet_bytes);
    CmTypes::from_any(packet)
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

/// Get the number of pilot packets per frame: num_pilot_symbols * antennas
#[no_mangle]
pub fn get_pilot_packet_count(config: &CmTypes, framestats: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    framestats_ref.NumPilotSyms() * config_ref.bs_ant_num()
                })
                .expect("Failed to access FrameStats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_config(config_file: String) -> CmTypes {
    let mut config = Config::new(&config_file);
    config.Gen_pilots();
    config.UpdateUlMCS();
    config.ScheduleInit();

    CmTypes::from_any(config)
}

#[no_mangle]
pub fn get_packets_per_frame(config: &CmTypes, packet_config: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| {
            packet_config
                .with_any(|packet_config_ref: &PacketConfig| {
                    let packets = packet_config_ref.schedule_length * config_ref.bs_ant_num();
                    packets
                })
                .expect("Failed to access PacketConfig struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_pilot_symbols(framestats: &CmTypes) -> usize {
    framestats
        .with_any(|framestats_ref: &FrameStats| framestats_ref.NumPilotSyms())
        .expect("Failed to access Framestats struct or wrong type")
}

#[no_mangle]
pub fn get_uplink_symbols(framestats: &CmTypes) -> usize {
    framestats
        .with_any(|framestats_ref: &FrameStats| framestats_ref.NumUlSyms())
        .expect("Failed to access Framestats struct or wrong type")
}

#[no_mangle]
pub fn total_pilot_symbols(config: &CmTypes, framestats: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    framestats_ref.NumPilotSyms() * config_ref.bs_ant_num()
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn total_uplink_symbols(config: &CmTypes, framestats: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    framestats_ref.NumUlSyms() * config_ref.bs_ant_num()
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_packet_config(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let packet_config = PacketConfig::new(&config_ref);
            CmTypes::from_any(packet_config)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_antennas(config: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| config_ref.bs_ant_num())
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_packet_length(packet_config: &CmTypes) -> usize {
    packet_config
        .with_any(|packet_config_ref: &PacketConfig| packet_config_ref.packet_length)
        .expect("Failed to access PacketConfig struct or wrong type")
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

#[no_mangle]
pub fn create_framestats(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let framestats = FrameStats::new(config_ref.frame_schedule().to_string());
            CmTypes::from_any(framestats)
        })
        .expect("Failed to access Config struct or wrong type")
}

/// Check if a packet contains a pilot symbol
/// Now the first processing node - receives parsed Packet from process_packet()
#[no_mangle]
pub fn is_pilot(packet: &CmTypes, framestats: &CmTypes, _index: usize) -> bool {
    packet
        .with_any(|packet_ref: &Packet| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    let _frame_id = packet_ref.frame_id as usize;
                    let _ant_id = packet_ref.ant_id as usize;
                    let symbol_id = packet_ref.symbol_id as usize;
                    let symbol_type = framestats_ref.GetSymbolType(symbol_id);
                    let is_pilot = symbol_type == SymbolType::kPilot;
                    // if is_pilot {
                    //     println!(
                    //         "Packet: frame {}, symbol {}, ant {}, index {} is a pilot symbol",
                    //         _frame_id, symbol_id, _ant_id, _index
                    //     );
                    // } else {
                    //     println!(
                    //         "Packet: frame {}, symbol {}, ant {}, index {} is not a pilot symbol",
                    //         _frame_id, symbol_id, _ant_id, _index
                    //     );
                    // }
                    is_pilot
                })
                .expect("Failed to access FrameStats struct or wrong type")
        })
        .expect("Failed to access Packet struct or wrong type")
}

#[no_mangle]
pub fn write_buffers_to_file(
    file: String,
    fft_buffer: &CmTypes,
    csi_buffers: &CmTypes,
    ul_beam_matrices: &CmTypes,
    demod_buffers: &CmTypes,
    decoded_buffers: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
) {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    fft_buffer
                        .with_any(|fft_buffer_ref: &FftBuffer| {
                            csi_buffers
                                .with_any(|csi_buffers_ref: &CsiBuffer| {
                                    ul_beam_matrices
                                        .with_any(|ul_beam_matrices_ref: &UlBeamMatrix| {
                                            demod_buffers
                                                .with_any(|demod_buffers_ref: &DemodBuffer| {
                                                    decoded_buffers
                                                        .with_any(|decoded_buffers_ref: &DecodedBuffer| {
                                                            write_buffers_to_file_impl(
                                                                file,
                                                                fft_buffer_ref,
                                                                csi_buffers_ref,
                                                                ul_beam_matrices_ref,
                                                                demod_buffers_ref,
                                                                decoded_buffers_ref,
                                                                config_ref,
                                                                framestats_ref,
                                                            )
                                                        })
                                                        .expect("Failed to access DecodedBuffer struct or wrong type")
                                                })
                                                .expect("Failed to access DemodBuffer struct or wrong type")
                                        })
                                        .expect("Failed to access UlBeamMatrix struct or wrong type")
                                })
                                .expect("Failed to access CsiBuffer struct or wrong type")
                        })
                        .expect("Failed to access FftBuffer struct or wrong type")
                })
                .expect("Failed to access FrameStats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

pub fn write_buffers_to_file_impl(
    file: String,
    fft_buffer: &FftBuffer,
    csi_buffers: &CsiBuffer,
    ul_beam_matrices: &UlBeamMatrix,
    demod_buffers: &DemodBuffer,
    decoded_buffers: &DecodedBuffer,
    config: &Config,
    framestats: &FrameStats,
) {
    let mut file = File::create(file).expect("Failed to create file for writing buffers");

    let factor = min(FrameWnd, config.max_frame());
    let symbols_ul = framestats.NumUlSyms() * factor;

    write!(file, "\nMain: FFT Buffer=[").unwrap();
    for frame in 0..symbols_ul {
        let fft_table = fft_buffer.get().get(frame);
        for ul in 0..(config.ofdm_data_num() * config.bs_ant_num()) {
            write!(
                file,
                "{:.5?}+1j* {:.5?}, ",
                fft_table[ul].re(),
                fft_table[ul].im()
            )
            .unwrap();
        }
        write!(file, "\n--------------\n").unwrap();
    }
    writeln!(file, "]\n").unwrap();

    write!(file, "\nMain: CSI Buffer=[").unwrap();
    for frame in 0..factor {
        for ue in 0..config.ue_ant_num() {
            let csi_grid = csi_buffers.get().get(frame, ue);
            for i in 0..(config.bs_ant_num() * config.ofdm_data_num()) {
                write!(
                    file,
                    "{:.5?}+1j* {:.5?}, ",
                    csi_grid[i].re(),
                    csi_grid[i].im()
                )
                .unwrap();
            }
            write!(file, "\n--------------\n").unwrap();
        }
        write!(file, "\n--------------\n").unwrap();
    }
    writeln!(file, "]\n").unwrap();

    write!(file, "Main: ULZF =[").unwrap();
    for frame in 0..factor {
        for sc in 0..config.ofdm_data_num() {
            let ulzf_grid = ul_beam_matrices.get().get(frame, sc);
            for i in 0..config.bs_ant_num() * config.ue_ant_num() {
                write!(
                    file,
                    "{:.5?}+1j* {:.5?}, ",
                    ulzf_grid[i].re(),
                    ulzf_grid[i].im()
                )
                .unwrap();
            }
            write!(file, "\n--------------\n").unwrap();
        }
        write!(file, "\n--------------\n").unwrap();
    }
    writeln!(file, "]\n").unwrap();

    let mod_bits = config.ModOrderBits(Direction::Uplink);
    let ul_data_syms = framestats.NumUlDataSyms(&config);
    write!(file, "Main: DemulF (Mbits: {:?})=[", mod_bits).unwrap();
    for frame in 0..factor {
        for sc in 0..ul_data_syms {
            for ue in 0..config.ue_ant_num() {
                let demod_cube = demod_buffers.get().get(frame, sc, ue);
                for i in 0..(config.ofdm_data_num() * mod_bits) {
                    write!(file, "{:?}, ", demod_cube[i]).unwrap();
                }
                write!(file, "\n-------ue change-------\n").unwrap();
            }
            write!(file, "\n------sc change--------\n").unwrap();
        }
        write!(file, "\n-------fr change-------\n").unwrap();
    }
    writeln!(file, "]\n").unwrap();

    let ldpc_config = config.LdpcConfig(Direction::Uplink);
    let num_bytes_per_cb = config.NumBytesPerCb(Direction::Uplink);
    let dim3 = ldpc_config.GetNumBlocksInSymbol() * roundup(num_bytes_per_cb, 64);

    write!(file, "Main: Decode =[").unwrap();
    for frame in 0..factor {
        for sc in 0..ul_data_syms {
            for ue in 0..config.ue_ant_num() {
                let decode_cube = decoded_buffers.get().get(frame, sc, ue);
                for i in 0..dim3 {
                    write!(file, "{:?}, ", decode_cube[i]).unwrap();
                }
                write!(file, "\n-------ue change-------\n").unwrap();
            }
            write!(file, "\n------sc change--------\n").unwrap();
        }
        write!(file, "\n-------fr change-------\n").unwrap();
    }
    writeln!(file, "]\n").unwrap();
}
