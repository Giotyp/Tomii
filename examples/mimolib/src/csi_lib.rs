use crate::bindings::fftfuncs_bindings::*;
use crate::bindings::mkl_bindings::*;
use crate::buffer_lib::CsiBuffer;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::structures::AlignedVec;
use crate::common::symbols::{FrameWnd, MaxUEs, SCsPerCacheline, TransposeBlockSize};
use crate::packet_lib::*;
use num_complex::Complex32;
use synstream_types::CmTypes;

// ============ Rust CSI Expansion Implementation ============
// Replaces C++ expand_csi() for better efficiency (no FFI overhead, no double Vec allocation)
// Logic: For each block and UE, broadcast one source value across TransposeBlockSize destination positions
#[inline]
fn _expand_csi_rs(
    ofdm_data_num: usize,
    bs_ant_num: usize,
    ue_ant_num: usize,
    ant_id: usize,
    src_buf: *const Complex32,
    block_size: usize,
    dst_bufs: *mut *mut Complex32,
    dst_bufs_len: usize,
) {
    let num_blocks = ofdm_data_num / block_size;

    unsafe {
        for block_idx in 0..num_blocks {
            let block_base_offset = block_idx * (block_size * bs_ant_num);
            let block_offset = block_base_offset + (ant_id * block_size);

            // Process UEs in reverse order to match original C++ implementation
            for ue_id in (0..ue_ant_num).rev() {
                // Bounds check
                if ue_id >= dst_bufs_len {
                    continue;
                }

                // Read single value from source buffer
                let src_val = *src_buf.add(block_offset + ue_id);

                // Get mutable pointer to destination buffer for this UE
                let dst_ptr = *dst_bufs.add(ue_id) as *mut Complex32;

                // Broadcast: fill TransposeBlockSize positions with the same value
                // Using a manual loop instead of slice operations due to raw pointers
                for sc_idx in 0..block_size {
                    *dst_ptr.add(block_offset + sc_idx) = src_val;
                }
            }
        }
    }
}

#[no_mangle]
pub fn create_csi_buffer(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let csi_buffer = CsiBuffer::new(&config_ref);
            CmTypes::from_any(csi_buffer)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn csi_op(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    csi_buffer: &CmTypes,
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    packet
                        .with_any(|packet_ref: &Packet| {
                            fft_struct
                                .with_any_mut(|fft_struct_mut: &mut Fft| {
                                    csi_buffer
                                        .with_any_mut(|csi_buffer_mut: &mut CsiBuffer| {
                                            let frame_id = packet_ref.frame_id as usize;
                                            let frame_slot = frame_id % FrameWnd;

                                            let ant_id = packet_ref.ant_id as usize;
                                            let symbol_id = packet_ref.symbol_id as usize;
                                            let symbol_type =
                                                framestats_ref.GetSymbolType(symbol_id);

                                            let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();

                                            let packet_ptr = unsafe {
                                                packet_ref.data.as_ptr().add(2 * sample_offset)
                                                    as *const i16
                                            };
                                            fft_struct_mut.convert_short_to_float(packet_ptr);
                                            fft_struct_mut.computefft();
                                            fft_struct_mut.inout_shift(config_ref.ofdm_ca_num());

                                            let pilot_symbol_id =
                                                framestats_ref.GetPilotSymbolIdx(symbol_id);

                                            let csi_buf = csi_buffer_mut.get_mut();

                                            let csi_cell_ptr = csi_buf
                                                .get_mut(frame_slot, pilot_symbol_id)
                                                .as_mut_ptr()
                                                as *mut libc::c_void;

                                            unsafe {
                                                PartialTranspose(
                                                    csi_cell_ptr,
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

                                            // Expand partial CSI from freq-orth pilot to full CSI per UE
                                            if config_ref.freq_orth_pilot()
                                                && pilot_symbol_id
                                                    == framestats_ref.NumPilotSyms() - 1
                                            {
                                                let csi_buf = csi_buffer_mut.get_mut();

                                                let src_buf = csi_buf.get(frame_slot, 0).as_ptr();

                                                let mut dst_bufs: Vec<*mut libc::c_void> = vec![
                                                        std::ptr::null_mut();
                                                        config_ref.ue_ant_num()
                                                    ];

                                                for ue_id in (0..config_ref.ue_ant_num()).rev() {
                                                    let dst_buf = csi_buf
                                                        .get_mut(frame_slot, ue_id)
                                                        .as_mut_ptr();
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
                                                        dst_bufs.as_mut_ptr()
                                                            as *mut *mut libc::c_void,
                                                        dst_bufs.len(),
                                                    );
                                                }
                                            }
                                            // println!("Pilot CSI done for frame {}, slot {}, ant {}, symbol {}",
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

// ************ Sliced CSI operations ************
#[no_mangle]
pub fn create_csi_buffer_sliced(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let csi_buffer = CsiBuffer::new(&config_ref);
            CmTypes::from_any_sliced(csi_buffer)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_pilot_offset(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
) -> (usize, usize) {
    packet
        .with_any(|packet_ref: &Packet| {
            config
                .with_any(|config_ref: &Config| {
                    framestats
                        .with_any(|framestats_ref: &FrameStats| {
                            let frame_id = packet_ref.frame_id as usize;
                            let frame_slot = frame_id % FrameWnd;
                            let symbol_id = packet_ref.symbol_id as usize;
                            let pilot_symbol_id = framestats_ref.GetPilotSymbolIdx(symbol_id);

                            let buf_cols = config_ref.ue_ant_num();

                            let start_pointer = frame_slot * buf_cols + pilot_symbol_id;
                            let n_entries = config_ref.bs_ant_num() * config_ref.ofdm_data_num();
                            (start_pointer, n_entries)
                        })
                        .expect("Failed to access Framestats struct or wrong type")
                })
                .expect("Failed to access Config struct or wrong type")
        })
        .expect("Failed to access PacketConfig struct or wrong type")
}

#[no_mangle]
pub fn csi_op_sliced(
    packet: CmTypes,
    config: CmTypes,
    framestats: CmTypes,
    fft_desc: CmTypes,
    csi_buffer: &mut [Complex32],
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

                                    let ant_id = packet_ref.ant_id as usize;
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

                                    let csi_cell_ptr = csi_buffer.as_mut_ptr() as *mut libc::c_void;

                                    unsafe {
                                        PartialTranspose(
                                            csi_cell_ptr,
                                            ant_id,
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
                                    let res = (frame_id, symbol_id, ant_id);
                                    CmTypes::from_any(res)
                                })
                                .expect("Failed to access FftDesc struct or wrong type")
                        })
                        .expect("Failed to access PacketConfig struct or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_expand_offset(config: &CmTypes, csi_res: &CmTypes, node_index: usize) -> (usize, usize) {
    config
        .with_any(|config_ref: &Config| {
            csi_res
                .with_any(|csi_res_ref: &(usize, usize, usize)| {
                    let (frame_id, _, _) = csi_res_ref;
                    let frame_slot = frame_id % FrameWnd;

                    let buf_cols = config_ref.ue_ant_num();

                    let col_offset = node_index % config_ref.ue_ant_num();

                    let start_pointer = frame_slot * buf_cols + col_offset;
                    let n_elems = config_ref.ue_ant_num();
                    (start_pointer, n_elems)
                })
                .expect("Failed to access CsiRes struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn csi_expand(
    config: CmTypes,
    csi_res: CmTypes,
    mut csi_buffers: Vec<&mut [Complex32]>,
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            csi_res
                .with_any(|csi_res_ref: &(usize, usize, usize)| {
                    let (frame_id, symbol_id, ant_id) = csi_res_ref.clone();
                    let frame_slot = frame_id % FrameWnd;

                    let src_buf = csi_buffers[0].as_ptr();

                    let mut dst_bufs: Vec<*mut libc::c_void> = {
                        let mut vec = Vec::new();
                        for buf in csi_buffers.iter_mut() {
                            vec.push(buf.as_mut_ptr() as *mut libc::c_void);
                        }
                        vec
                    };

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

                    let res = (frame_id, symbol_id);
                    CmTypes::from_any(res)
                })
                .expect("Failed to access CsiRes struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ************ Unsafe CSI operations ************
#[no_mangle]
pub fn csi_op_ptr(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    csi_buffer: &CmTypes,
) -> CmTypes {
    // Extract all raw pointers up-front; each as_mut_ptr holds the read lock only briefly.
    let config_ptr = unsafe { config.as_mut_ptr::<Config>().unwrap().0 as usize };
    let framestats_ptr = unsafe { framestats.as_mut_ptr::<FrameStats>().unwrap().0 as usize };
    let packet_raw = unsafe { packet.as_mut_ptr::<Packet>().unwrap().0 as usize };
    let fft_ptr = unsafe { fft_struct.as_mut_ptr::<Fft>().unwrap().0 as usize };
    let csi_buf_raw = unsafe { csi_buffer.as_mut_ptr::<CsiBuffer>().unwrap().0 as usize };

    let config_ref = unsafe { &*(config_ptr as *const Config) };
    let framestats_ref = unsafe { &*(framestats_ptr as *const FrameStats) };
    let packet_ref = unsafe { &*(packet_raw as *const Packet) };
    let fft_struct_mut = unsafe { &mut *(fft_ptr as *mut Fft) };
    let csi_buffer_mut = unsafe { &mut *(csi_buf_raw as *mut CsiBuffer) };

    let frame_id = packet_ref.frame_id as usize;
    let frame_slot = frame_id % FrameWnd;
    let ant_id = packet_ref.ant_id as usize;
    let symbol_id = packet_ref.symbol_id as usize;
    let symbol_type = framestats_ref.GetSymbolType(symbol_id);

    let sample_offset = config_ref.ofdm_rx_zero_prefix_bs();
    let packet_i16 = unsafe { packet_ref.data.as_ptr().add(2 * sample_offset) as *const i16 };

    fft_struct_mut.convert_short_to_float(packet_i16);
    fft_struct_mut.computefft();
    fft_struct_mut.inout_shift(config_ref.ofdm_ca_num());

    let pilot_symbol_id = framestats_ref.GetPilotSymbolIdx(symbol_id);
    let csi_buf_mut = csi_buffer_mut.get_mut();

    let csi_cell_ptr = csi_buf_mut
        .get_mut(frame_slot, pilot_symbol_id)
        .as_mut_ptr() as *mut libc::c_void;

    unsafe {
        PartialTranspose(
            csi_cell_ptr,
            ant_id,
            config_ref.bs_ant_num(),
            symbol_type,
            config_ref.ofdm_data_num(),
            config_ref.ofdm_data_start(),
            fft_struct_mut.fft_inout_align.get().as_ptr() as *const libc::c_void,
            config_ref.pilots_sgn().as_ptr() as *const libc::c_void,
            TransposeBlockSize,
            SCsPerCacheline,
        );
    }

    // Expand partial CSI from freq-orth pilot to full CSI per UE.
    // Uses a stack-allocated pointer array to avoid heap allocation.
    // _expand_csi_rs replaces the expand_csi FFI call (which allocates std::vector internally).
    if config_ref.freq_orth_pilot() && pilot_symbol_id == framestats_ref.NumPilotSyms() - 1 {
        let ue_ant_num = config_ref.ue_ant_num();
        let src_buf = csi_buf_mut.get(frame_slot, 0).as_ptr();

        let mut dst_ptrs = [std::ptr::null_mut::<Complex32>(); MaxUEs];
        for ue_id in 0..ue_ant_num {
            dst_ptrs[ue_id] = csi_buf_mut.get_mut(frame_slot, ue_id).as_mut_ptr();
        }
        _expand_csi_rs(
            config_ref.ofdm_data_num(),
            config_ref.bs_ant_num(),
            ue_ant_num,
            ant_id,
            src_buf,
            TransposeBlockSize,
            dst_ptrs.as_mut_ptr(),
            ue_ant_num,
        );
    }

    CmTypes::Usize(frame_id)
}
