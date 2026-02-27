use crate::bindings::fftfuncs_bindings::*;
use crate::bindings::mkl_bindings::*;
use crate::buffer_lib::{CsiBuffer, FftBuffer};
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::structures::AlignedVec;
use crate::common::symbols::SymbolType;
use crate::common::symbols::{FrameWnd, SCsPerCacheline, TransposeBlockSize};
use crate::packet_lib::*;
use num_complex::Complex32;
use std::sync::Arc;
use synstream_types::CmTypes;

#[no_mangle]
pub fn create_fft_buffer(config: &CmTypes, framestats: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    let fft_buffer = FftBuffer::new(&config_ref, &framestats_ref);
                    CmTypes::from_any(fft_buffer)
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_fft_struct(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let fft_struct = Fft::new(config_ref.ofdm_ca_num());
            CmTypes::from_any(fft_struct)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn fft_op(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    fft_buffer: &CmTypes,
    _index: usize,
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    packet
                        .with_any(|packet_ref: &Packet| {
                            fft_struct
                                .with_any_mut(|fft_struct_mut: &mut Fft| {
                                    fft_buffer
                                        .with_any_mut(|fft_buffer_mut: &mut FftBuffer| {
                                            let frame_id = packet_ref.frame_id as usize;
                                            let frame_slot = frame_id % FrameWnd;

                                            let ant_id = packet_ref.ant_id as usize;
                                            let symbol_id = packet_ref.symbol_id as usize;
                                            let symbol_type =
                                                framestats_ref.GetSymbolType(symbol_id);

                                            let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();
                                            let data_offset = config_ref.GetDataOffset(
                                                frame_slot,
                                                symbol_id,
                                                &framestats_ref,
                                            );
                                            // println!("Frame-id: {}, Ant_id: {}, Symbol_id: {}, index: {}, Data offset: {}", frame_id, ant_id, symbol_id, index, data_offset);

                                            let packet_ptr = unsafe {
                                                packet_ref.data.as_ptr().add(2 * sample_offset)
                                                    as *const i16
                                            };
                                            fft_struct_mut.convert_short_to_float(packet_ptr);
                                            fft_struct_mut.computefft();
                                            fft_struct_mut.inout_shift(config_ref.ofdm_ca_num());

                                            let fft_buf = fft_buffer_mut.get_mut();

                                            let fft_buffer_ptr =
                                                fft_buf.get_mut(data_offset).as_mut_ptr()
                                                    as *mut libc::c_void;

                                            unsafe {
                                                PartialTranspose(
                                                    fft_buffer_ptr,
                                                    ant_id,
                                                    config_ref.bs_ant_num(),
                                                    symbol_type,
                                                    config_ref.ofdm_data_num(),
                                                    config_ref.ofdm_data_start(),
                                                    fft_struct_mut.fft_inout_align.get().as_ptr()
                                                        as *const libc::c_void,
                                                    config_ref.pilots_sgn().as_ptr()
                                                        as *const libc::c_void,
                                                    TransposeBlockSize,
                                                    SCsPerCacheline,
                                                );
                                            }
                                            // println!("UL FFT Locked done for frame {}, slot {}, ant {}, symbol {}",
                                            //          frame_id, frame_slot, ant_id, symbol_id);

                                            CmTypes::Usize(frame_id)
                                        })
                                        .expect("Failed to access FftBuffer struct or wrong type")
                                })
                                .expect("Failed to access Fft struct or wrong type")
                        })
                        .expect("Failed to access PacketConfig struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ************ Sliced FFT operations ************
#[no_mangle]
pub fn create_fft_desc(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let fft_desc = FftDescriptor::new(config_ref.ofdm_ca_num());
            CmTypes::from_any(fft_desc)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_fft_buffer_sliced(config: &CmTypes, framestats: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    let fft_buffer = FftBuffer::new(&config_ref, &framestats_ref);
                    CmTypes::from_any_sliced(fft_buffer)
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_data_offset(packet: &CmTypes, config: &CmTypes, framestats: &CmTypes) -> (usize, usize) {
    packet
        .with_any(|packet_ref: &Packet| {
            config
                .with_any(|config_ref: &Config| {
                    framestats
                        .with_any(|framestats_ref: &FrameStats| {
                            let frame_id = packet_ref.frame_id as usize;
                            let frame_slot = frame_id % FrameWnd;
                            let ant_id = packet_ref.ant_id as usize;
                            let symbol_id = packet_ref.symbol_id as usize;

                            let data_offset =
                                config_ref.GetDataOffset(frame_slot, symbol_id, &framestats_ref);

                            let cols = config_ref.bs_ant_num() * config_ref.ofdm_data_num();

                            let row_offset = data_offset * cols + ant_id * TransposeBlockSize;

                            (row_offset, cols)
                        })
                        .expect("Failed to access Framestats struct or wrong type")
                })
                .expect("Failed to access Config struct or wrong type")
        })
        .expect("Failed to access PacketConfig struct or wrong type")
}

#[no_mangle]
pub fn fft_op_sliced(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_desc: &CmTypes,
    fft_buffer: &mut [Complex32],
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    packet
                        .with_any(|packet_ref: &Packet| {
                            fft_desc
                                .with_any(|fft_desc_ref: &FftDescriptor| {
                                    let frame_id = packet_ref.frame_id as usize;
                                    let _frame_slot = frame_id % FrameWnd;

                                    let _ant_id = packet_ref.ant_id as usize;
                                    let symbol_id = packet_ref.symbol_id as usize;
                                    let symbol_type = framestats_ref.GetSymbolType(symbol_id);

                                    let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();
                                    let ofdm_ca_num = config_ref.ofdm_ca_num();
                                    let n_elems = 2 * ofdm_ca_num;

                                    let mut fft_align: AlignedVec<Complex32> =
                                        AlignedVec::new(n_elems, 64);

                                    let packet_ptr = unsafe {
                                        packet_ref.data.as_ptr().add(2 * sample_offset)
                                            as *const i16
                                    };

                                    convert_short_to_float(
                                        fft_align.get_mut(),
                                        n_elems,
                                        packet_ptr,
                                    );
                                    computefft(fft_align.get_mut(), &fft_desc_ref.desc);
                                    inout_shift(fft_align.get_mut(), ofdm_ca_num);

                                    let fft_buffer_ptr =
                                        fft_buffer.as_mut_ptr() as *mut libc::c_void;

                                    unsafe {
                                        PartialTransposeSliced(
                                            fft_buffer_ptr,
                                            config_ref.bs_ant_num(),
                                            symbol_type,
                                            config_ref.ofdm_data_num(),
                                            config_ref.ofdm_data_start(),
                                            fft_align.get().as_ptr() as *const libc::c_void,
                                            config_ref.pilots_sgn().as_ptr() as *const libc::c_void,
                                            TransposeBlockSize,
                                            SCsPerCacheline,
                                        );
                                    }
                                    // println!(
                                    //     "UL FFT Sliced done for frame {}, slot {}, ant {}, symbol {}",
                                    //     frame_id, frame_slot, ant_id, symbol_id
                                    // );
                                    let res = (frame_id, symbol_id);
                                    CmTypes::from_any(res)
                                })
                                .expect("Failed to access Fft Desc or wrong type")
                        })
                        .expect("Failed to access PacketConfig struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ************ Single FFT (one transpose block) operation ************
#[no_mangle]
pub fn get_sc_blocks(config: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| {
            let num_sc_blocks = config_ref.ofdm_data_num() / TransposeBlockSize;
            num_sc_blocks
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_sc_offset(
    packet_info: &(usize, usize, usize),
    config: &CmTypes,
    framestats: &CmTypes,
    index: usize,
) -> (usize, usize) {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    let (frame_slot, ant_id, symbol_id) = *packet_info;

                    let block_offset = index * TransposeBlockSize * config_ref.bs_ant_num();

                    let data_offset =
                        config_ref.GetDataOffset(frame_slot, symbol_id, &framestats_ref);

                    let cols = config_ref.bs_ant_num() * config_ref.ofdm_data_num();

                    let row_offset =
                        block_offset + data_offset * cols + ant_id * TransposeBlockSize;

                    (row_offset, TransposeBlockSize)
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn fft_data(packet: &CmTypes, config: &CmTypes, fft_desc: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            packet
                .with_any(|packet_ref: &Packet| {
                    fft_desc
                        .with_any(|fft_desc_ref: &FftDescriptor| {
                            let frame_id = packet_ref.frame_id as usize;
                            let frame_slot = frame_id % FrameWnd;

                            let ant_id = packet_ref.ant_id as usize;
                            let symbol_id = packet_ref.symbol_id as usize;

                            let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();
                            let ofdm_ca_num = config_ref.ofdm_ca_num();
                            let n_elems = 2 * ofdm_ca_num;

                            let mut fft_align: AlignedVec<Complex32> = AlignedVec::new(n_elems, 64);

                            let packet_ptr = unsafe {
                                packet_ref.data.as_ptr().add(2 * sample_offset) as *const i16
                            };

                            convert_short_to_float(fft_align.get_mut(), n_elems, packet_ptr);
                            computefft(fft_align.get_mut(), &fft_desc_ref.desc);
                            inout_shift(fft_align.get_mut(), ofdm_ca_num);

                            // println!(
                            //     "UL FFT Single done for frame {}, slot {}, ant {}, symbol {}",
                            //     frame_id, frame_slot, ant_id, symbol_id
                            // );

                            let res1 = fft_align.get().to_vec();
                            let res1 = CmTypes::from_any(res1);
                            let res2 = (frame_slot, ant_id, symbol_id);
                            let res2 = CmTypes::from_any(res2);
                            let vec_res = vec![res1, res2];
                            CmTypes::VecCmt(Arc::new(vec_res))
                        })
                        .expect("Failed to access Fft Desc or wrong type")
                })
                .expect("Failed to access PacketConfig struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn fft_op_single(
    config: &CmTypes,
    index: usize,
    fft_data: &CmTypes,
    fft_buffer: &mut [Complex32],
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            fft_data
                .with_any(|fft_data_ref: &Vec<Complex32>| {
                    let data_elems = TransposeBlockSize;
                    let data_offset = index * data_elems + config_ref.ofdm_data_start();

                    let end = std::cmp::min(data_offset + data_elems, fft_data_ref.len());

                    if data_offset > end || data_offset == end {
                        // println!("Invalid data offset: {} to {}", data_offset, end);
                        return CmTypes::None;
                    }

                    // println!("FFT Data Copy from {} to {}", data_offset, end);
                    fft_buffer.copy_from_slice(&fft_data_ref[data_offset..end]);

                    CmTypes::None
                })
                .expect("Failed to access Fft Data or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ************ Unsafe FFT operations ************
#[no_mangle]
pub fn fft_op_ptr(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    fft_buffer: &CmTypes,
    _index: usize,
) -> CmTypes {
    let fft_struct_ptr = unsafe { fft_struct.as_mut_ptr::<Fft>().unwrap().0 as usize };
    let fft_buffer_ptr = unsafe { fft_buffer.as_mut_ptr::<FftBuffer>().unwrap().0 as usize };
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    packet
                        .with_any(|packet_ref: &Packet| {
                            let frame_id = packet_ref.frame_id as usize;
                            let frame_slot = frame_id % FrameWnd;

                            let ant_id = packet_ref.ant_id as usize;
                            let symbol_id = packet_ref.symbol_id as usize;
                            let symbol_type = framestats_ref.GetSymbolType(symbol_id);

                            let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();
                            let data_offset =
                                config_ref.GetDataOffset(frame_slot, symbol_id, &framestats_ref);
                            // println!("Frame-id: {}, Ant_id: {}, Symbol_id: {}, index: {}, Data offset: {}", frame_id, ant_id, symbol_id, index, data_offset);

                            let packet_ptr = unsafe {
                                packet_ref.data.as_ptr().add(2 * sample_offset) as *const i16
                            };

                            let fft_struct_ptr = fft_struct_ptr as *mut Fft;
                            let fft_struct_mut = unsafe { &mut *fft_struct_ptr };

                            fft_struct_mut.convert_short_to_float(packet_ptr);
                            fft_struct_mut.computefft();
                            fft_struct_mut.inout_shift(config_ref.ofdm_ca_num());

                            let fft_ptr = fft_buffer_ptr as *mut FftBuffer;
                            let fft_buffer_mut = unsafe { &mut *fft_ptr };

                            let fft_buf = fft_buffer_mut.get_mut();
                            let fft_buffer_ptr =
                                fft_buf.get_mut(data_offset).as_mut_ptr() as *mut libc::c_void;

                            unsafe {
                                PartialTranspose(
                                    fft_buffer_ptr,
                                    ant_id,
                                    config_ref.bs_ant_num(),
                                    symbol_type,
                                    config_ref.ofdm_data_num(),
                                    config_ref.ofdm_data_start(),
                                    fft_struct_mut.fft_inout_align.get().as_ptr()
                                        as *const libc::c_void,
                                    config_ref.pilots_sgn().as_ptr() as *const libc::c_void,
                                    TransposeBlockSize,
                                    SCsPerCacheline,
                                );
                            }
                            // println!("UL FFT done for frame {}, slot {}, ant {}, symbol {}",
                            //          frame_id, frame_slot, ant_id, symbol_id);
                            CmTypes::Usize(frame_id)
                        })
                        .expect("Failed to access PacketConfig struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn fft_comb_ptr(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    fft_buffer: &CmTypes,
    csi_buffer: &CmTypes,
) -> CmTypes {
    let fft_struct_ptr = unsafe { fft_struct.as_mut_ptr::<Fft>().unwrap().0 as usize };
    let fft_buffer_ptr = unsafe { fft_buffer.as_mut_ptr::<FftBuffer>().unwrap().0 as usize };
    let csi_buffer_ptr = unsafe { csi_buffer.as_mut_ptr::<CsiBuffer>().unwrap().0 as usize };
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    packet
                        .with_any(|packet_ref: &Packet| {
                            let frame_id = packet_ref.frame_id as usize;
                            let frame_slot = frame_id % FrameWnd;

                            let ant_id = packet_ref.ant_id as usize;
                            let symbol_id = packet_ref.symbol_id as usize;
                            let symbol_type = framestats_ref.GetSymbolType(symbol_id);

                            let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();
                            // println!("Frame-id: {}, Ant_id: {}, Symbol_id: {}, index: {}, Data offset: {}", frame_id, ant_id, symbol_id, index, data_offset);

                            let packet_ptr = unsafe {
                                packet_ref.data.as_ptr().add(2 * sample_offset) as *const i16
                            };

                            let fft_struct_ptr = fft_struct_ptr as *mut Fft;
                            let fft_struct_mut = unsafe { &mut *fft_struct_ptr };

                            fft_struct_mut.convert_short_to_float(packet_ptr);
                            fft_struct_mut.computefft();
                            fft_struct_mut.inout_shift(config_ref.ofdm_ca_num());

                            if symbol_type == SymbolType::kUL {
                                let data_offset = config_ref.GetDataOffset(
                                    frame_slot,
                                    symbol_id,
                                    &framestats_ref,
                                );
                                let fft_ptr = fft_buffer_ptr as *mut FftBuffer;
                                let fft_buffer_mut = unsafe { &mut *fft_ptr };

                                let fft_buf = fft_buffer_mut.get_mut();
                                let fft_buffer_ptr =
                                    fft_buf.get_mut(data_offset).as_mut_ptr() as *mut libc::c_void;

                                unsafe {
                                    PartialTranspose(
                                        fft_buffer_ptr,
                                        ant_id,
                                        config_ref.bs_ant_num(),
                                        symbol_type,
                                        config_ref.ofdm_data_num(),
                                        config_ref.ofdm_data_start(),
                                        fft_struct_mut.fft_inout_align.get().as_ptr()
                                            as *const libc::c_void,
                                        config_ref.pilots_sgn().as_ptr() as *const libc::c_void,
                                        TransposeBlockSize,
                                        SCsPerCacheline,
                                    );
                                }
                            } else if symbol_type == SymbolType::kPilot {
                                let pilot_symbol_id = framestats_ref.GetPilotSymbolIdx(symbol_id);

                                let csi_ptr = csi_buffer_ptr as *mut CsiBuffer;
                                let csi_buffer_mut = unsafe { &mut *csi_ptr };

                                let csi_buf_mut = csi_buffer_mut.get_mut();
                                let csi_buffer_ptr = csi_buf_mut
                                    .get_mut(frame_slot, pilot_symbol_id)
                                    .as_mut_ptr()
                                    as *mut libc::c_void;

                                unsafe {
                                    PartialTranspose(
                                        csi_buffer_ptr,
                                        ant_id,
                                        config_ref.bs_ant_num(),
                                        symbol_type,
                                        config_ref.ofdm_data_num(),
                                        config_ref.ofdm_data_start(),
                                        fft_struct_mut.fft_inout_align.get().as_ptr()
                                            as *const libc::c_void,
                                        config_ref.pilots_sgn().as_ptr() as *const libc::c_void,
                                        TransposeBlockSize,
                                        SCsPerCacheline,
                                    );
                                }
                                // Expand partial CSI from freq-orth pilot to full CSI per UE
                                if config_ref.freq_orth_pilot()
                                    && pilot_symbol_id == framestats_ref.NumPilotSyms() - 1
                                {
                                    let src_buf = csi_buf_mut.get(frame_slot, 0).as_ptr();

                                    let mut dst_bufs: Vec<*mut libc::c_void> =
                                        vec![std::ptr::null_mut(); config_ref.ue_ant_num()];

                                    for ue_id in (0..config_ref.ue_ant_num()).rev() {
                                        let dst_buf =
                                            csi_buf_mut.get_mut(frame_slot, ue_id).as_mut_ptr();
                                        dst_bufs[ue_id] = dst_buf as *mut libc::c_void;
                                    }
                                    unsafe {
                                        expand_csi(
                                            config_ref.ofdm_data_num(),
                                            config_ref.bs_ant_num(),
                                            config_ref.ue_ant_num(),
                                            frame_slot,
                                            ant_id,
                                            src_buf as *const libc::c_void,
                                            TransposeBlockSize,
                                            dst_bufs.as_mut_ptr() as *mut *mut libc::c_void,
                                            dst_bufs.len(),
                                        );
                                    }
                                }
                            }

                            CmTypes::Usize(frame_id)
                        })
                        .expect("Failed to access PacketConfig struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}
