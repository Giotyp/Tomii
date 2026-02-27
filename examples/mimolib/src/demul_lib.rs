#![allow(non_upper_case_globals)]
use std::cmp::min;

use num_complex::Complex32;

use crate::bindings::demod_bindings::*;
use crate::buffer_lib::*;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::structures::AlignedVec;
use crate::common::symbols::{
    Direction, FrameWnd, SCsPerCacheline, TransposeBlockSize, UplinkHardDemod,
};
use synstream_types::CmTypes;

const SIMDGather: bool = true;

#[no_mangle]
pub fn create_demul_base_scs(config: &CmTypes) -> Vec<usize> {
    config
        .with_any(|config_ref: &Config| {
            let mut sc_count = 0;
            let mut demul_base_scs: Vec<usize> = Vec::new();
            for _ in 0..config_ref.demul_events_per_symbol() {
                demul_base_scs.push(sc_count);
                sc_count += config_ref.demul_block_size();
            }
            demul_base_scs
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn get_ul_symbol(framestats: &CmTypes, symbol_id: usize) -> usize {
    framestats
        .with_any(|framestats_ref: &FrameStats| framestats_ref.GetUlSymbol(symbol_id))
        .expect("Failed to access Framestats struct or wrong type")
}

#[no_mangle]
pub fn total_demul_tasks(config: &CmTypes, framestats: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    config_ref.demul_events_per_symbol() * framestats_ref.NumUlSyms()
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn demul_events_per_symbol(config: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| config_ref.demul_events_per_symbol())
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_demul_struct(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let demul_block = config_ref.demul_block_size();
            let demul_struct = Demul::new(demul_block);
            CmTypes::from_any(demul_struct)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_demod_buffers(config: &CmTypes, framestats: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    let demod_buffers = DemodBuffer::new(&config_ref, &framestats_ref);
                    CmTypes::from_any(demod_buffers)
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn demul_op(
    config: &CmTypes,
    framestats: &CmTypes,
    demul_base_scs: &[usize],
    demul_struct: &CmTypes,
    fft_buffer: &CmTypes,
    demod_buffers: &CmTypes,
    ul_beam_matrices: &CmTypes,
    frame_id: usize,
    symbol_id: usize,
    node_index: usize,
) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    ul_beam_matrices
                        .with_any(|ul_beam_matrices_ref: &UlBeamMatrix| {
                            demul_struct
                                        .with_any(|demul_struct_ref: &Demul| {
                                            fft_buffer
                                                .with_any(|fft_buffer_ref: &FftBuffer| {
                                                    demod_buffers
                                                        .with_any_mut(|demod_buffer_mut: &mut DemodBuffer| {

                                                        let frame_slot = frame_id % FrameWnd;
                                                        let base_sc_id = demul_base_scs[node_index % demul_base_scs.len()];

                                                        // Create thread-local working buffers to avoid sharing across concurrent tasks
                                                        let mut data_gather = AlignedVec::<Complex32>::new(SCsPerCacheline * config_ref.bs_ant_num(), 64);
                                                        let mut equaled_buf_temp = AlignedVec::<Complex32>::new(demul_struct_ref.demul_block * config_ref.num_spatial_streams(), 64);
                                                        let mut equaled_buf_temp_trans = AlignedVec::<Complex32>::new(demul_struct_ref.demul_block * config_ref.num_spatial_streams(), 64);

                                                        let symbol_idx_ul = framestats_ref.GetUlSymbolIdx(symbol_id);
                                                        let total_symbol_idx_ul =
                                                            config_ref.GetTotalSymbolIdxUl(frame_id, symbol_idx_ul, framestats_ref);

                                                        let data_symbol_idx_ul = symbol_idx_ul - config_ref.client_ul_pilot_symbols();

                                                        let max_sc_ite = min(demul_struct_ref.demul_block, config_ref.ofdm_data_num() - base_sc_id);
                                                        assert!(max_sc_ite % SCsPerCacheline == 0);

                                                        for i in (0..max_sc_ite).step_by(SCsPerCacheline) {
                                                            // Step 1: Populate dat_gather_buffer as a row-major matrix
                                                            // with ScsPerCacheline rows and BsAntNum columns

                                                            let partial_transpose_block_base = ((base_sc_id + i) / TransposeBlockSize)
                                                                * (TransposeBlockSize * config_ref.bs_ant_num());

                                                            let bs_ant_num = config_ref.bs_ant_num();

                                                            let data_buf_ptr = fft_buffer_ref.get().get(total_symbol_idx_ul).as_ptr()
                                                                as *const libc::c_void;

                                                            unsafe {
                                                                DemulGather(
                                                                    TransposeBlockSize,
                                                                    base_sc_id,
                                                                    data_buf_ptr,
                                                                    data_gather.get_mut().as_mut_ptr() as *mut libc::c_void,
                                                                    SIMDGather,
                                                                    SCsPerCacheline,
                                                                    i,
                                                                    bs_ant_num,
                                                                    partial_transpose_block_base,
                                                                );
                                                            }

                                                            // Step 2: For each subcarrier, perform equalization by multiplying the
                                                            // subcarrier's data from each antenna with the subcarrier's precoder
                                                            for j in 0..SCsPerCacheline {
                                                                let cur_sc_id = base_sc_id + i + j;

                                                                // Assume ExportConstellation = false

                                                                let offset = j * config_ref.bs_ant_num();
                                                                let data_vec: Vec<Complex32> =
                                                                    data_gather.get()[offset..(offset + config_ref.bs_ant_num())].to_vec();

                                                                let ul_beam_vec = ul_beam_matrices_ref
                                                                    .get()
                                                                    .get(frame_slot, config_ref.GetBeamScId(cur_sc_id))
                                                                    .to_vec();

                                                                let ul_buf_ptr = ul_beam_vec.as_ptr() as *const libc::c_void;

                                                                let equal_offset = (cur_sc_id - base_sc_id) * config_ref.num_spatial_streams();
                                                                let eq_slice = &mut equaled_buf_temp.get_mut()[equal_offset..];

                                                                unsafe {
                                                                    Equalization(
                                                                        eq_slice.as_mut_ptr() as *mut libc::c_void,
                                                                        data_vec.as_ptr() as *const libc::c_void,
                                                                        config_ref.num_spatial_streams(),
                                                                        ul_buf_ptr,
                                                                        config_ref.bs_ant_num(),
                                                                    );
                                                                }
                                                            }
                                                        }

                                                        // Demodulation
                                                        if symbol_idx_ul >= config_ref.client_ul_pilot_symbols() {
                                                            unsafe {

                                                                let mut demod_bufs: Vec<*mut libc::c_void> =
                                                                    vec![std::ptr::null_mut(); config_ref.num_spatial_streams()];

                                                                let mod_order_bits = config_ref.ModOrderBits(Direction::Uplink);
                                                                for ss_id in 0..config_ref.num_spatial_streams() {
                                                                    let demod_buf = demod_buffer_mut
                                                                        .get_mut()
                                                                        .get_mut(frame_slot, data_symbol_idx_ul, ss_id);
                                                                    let demod_ptr = demod_buf.as_mut_ptr().add(mod_order_bits * base_sc_id);
                                                                    demod_bufs[ss_id] = demod_ptr as *mut libc::c_void;
                                                                }

                                                                let equal_temp_trans = equaled_buf_temp_trans.get_mut().as_mut_ptr() as *mut libc::c_void;
                                                                let equal_temp = equaled_buf_temp.get().as_ptr() as *mut libc::c_void;

                                                                Demod_wrap(
                                                                    config_ref.num_spatial_streams(),
                                                                    equal_temp,
                                                                    equal_temp_trans,
                                                                    max_sc_ite,
                                                                    total_symbol_idx_ul,
                                                                    mod_order_bits,
                                                                    UplinkHardDemod,
                                                                    demod_bufs.as_mut_ptr() as *mut *mut libc::c_void,
                                                                    demod_bufs.len(),
                                                                );
                                                            }
                                                        }
                                                        // println!(
                                                        //     "Demul done for frame {}, slot {}, ant {}, symbol {}",
                                                        //     frame_id, frame_slot, node_index, symbol_id
                                                        // );
                                                        CmTypes::from_any((frame_id, symbol_id, base_sc_id))
                                            })
                                        .expect("Failed to access DemodBuffers struct or wrong type")
                                        })
                                        .expect("Failed to access FftBuffer struct or wrong type")
                                })
                                .expect("Failed to access Demul struct or wrong type")
                        })
                        .expect("Failed to access UlBeamMatrix or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ********* Unsafe Demul Operations *********
#[no_mangle]
pub fn demul_op_ptr(
    config: &CmTypes,
    framestats: &CmTypes,
    demul_base_scs: &[usize],
    demul_struct: &CmTypes,
    fft_buffer: &CmTypes,
    demod_buffers: &CmTypes,
    ul_beam_matrices: &CmTypes,
    frame_id: usize,
    symbol_id: usize,
    node_index: usize,
) -> CmTypes {
    let demul_struct_ptr = unsafe { demul_struct.as_mut_ptr::<Demul>().unwrap().0 as usize };
    let demod_buffers_ptr =
        unsafe { demod_buffers.as_mut_ptr::<DemodBuffer>().unwrap().0 as usize };
    config
        .with_any(|config_ref: &Config| {
            framestats
                .with_any(|framestats_ref: &FrameStats| {
                    ul_beam_matrices
                        .with_any(|ul_beam_matrices_ref: &UlBeamMatrix| {
                            fft_buffer
                                .with_any(|fft_buffer_ref: &FftBuffer| {
                                    let frame_slot = frame_id % FrameWnd;
                                    let base_sc_id =
                                        demul_base_scs[node_index % demul_base_scs.len()];

                                    let demul_struct_ptr = demul_struct_ptr as *mut Demul;
                                    let demul_struct_mut = unsafe { &mut *demul_struct_ptr };

                                    let demod_buffers_ptr = demod_buffers_ptr as *mut DemodBuffer;
                                    let demod_buffer_mut = unsafe { &mut *demod_buffers_ptr };

                                    let data_gather = demul_struct_mut.data_gather_align.get_mut();
                                    let equaled_buf_temp = demul_struct_mut.equaled_align.get_mut();
                                    let equaled_buf_temp_trans =
                                        demul_struct_mut.equaled_trans_align.get_mut();

                                    let symbol_idx_ul = framestats_ref.GetUlSymbolIdx(symbol_id);
                                    let total_symbol_idx_ul = config_ref.GetTotalSymbolIdxUl(
                                        frame_id,
                                        symbol_idx_ul,
                                        framestats_ref,
                                    );

                                    let data_symbol_idx_ul =
                                        symbol_idx_ul - config_ref.client_ul_pilot_symbols();

                                    let max_sc_ite = min(
                                        demul_struct_mut.demul_block,
                                        config_ref.ofdm_data_num() - base_sc_id,
                                    );
                                    assert!(max_sc_ite % SCsPerCacheline == 0);

                                    for i in (0..max_sc_ite).step_by(SCsPerCacheline) {
                                        // Step 1: Populate dat_gather_buffer as a row-major matrix
                                        // with ScsPerCacheline rows and BsAntNum columns

                                        let partial_transpose_block_base = ((base_sc_id + i)
                                            / TransposeBlockSize)
                                            * (TransposeBlockSize * config_ref.bs_ant_num());

                                        let bs_ant_num = config_ref.bs_ant_num();

                                        let data_buf_ptr =
                                            fft_buffer_ref.get().get(total_symbol_idx_ul).as_ptr()
                                                as *const libc::c_void;

                                        unsafe {
                                            DemulGather(
                                                TransposeBlockSize,
                                                base_sc_id,
                                                data_buf_ptr,
                                                data_gather.as_mut_ptr() as *mut libc::c_void,
                                                SIMDGather,
                                                SCsPerCacheline,
                                                i,
                                                bs_ant_num,
                                                partial_transpose_block_base,
                                            );
                                        }

                                        // Step 2: For each subcarrier, perform equalization by multiplying the
                                        // subcarrier's data from each antenna with the subcarrier's precoder
                                        for j in 0..SCsPerCacheline {
                                            let cur_sc_id = base_sc_id + i + j;

                                            // Assume ExportConstellation = false

                                            let offset = j * config_ref.bs_ant_num();
                                            let data_vec: Vec<Complex32> = data_gather
                                                [offset..(offset + config_ref.bs_ant_num())]
                                                .to_vec();

                                            let ul_beam_vec = ul_beam_matrices_ref
                                                .get()
                                                .get(frame_slot, config_ref.GetBeamScId(cur_sc_id))
                                                .to_vec();

                                            let ul_buf_ptr =
                                                ul_beam_vec.as_ptr() as *const libc::c_void;

                                            let equal_offset = (cur_sc_id - base_sc_id)
                                                * config_ref.num_spatial_streams();
                                            let eq_slice = &mut equaled_buf_temp[equal_offset..];

                                            unsafe {
                                                Equalization(
                                                    eq_slice.as_mut_ptr() as *mut libc::c_void,
                                                    data_vec.as_ptr() as *const libc::c_void,
                                                    config_ref.num_spatial_streams(),
                                                    ul_buf_ptr,
                                                    config_ref.bs_ant_num(),
                                                );
                                            }
                                        }
                                    }

                                    // Demodulation
                                    if symbol_idx_ul >= config_ref.client_ul_pilot_symbols() {
                                        unsafe {
                                            let mut demod_bufs: Vec<*mut libc::c_void> = vec![
                                                    std::ptr::null_mut();
                                                    config_ref.num_spatial_streams()
                                                ];

                                            let mod_order_bits =
                                                config_ref.ModOrderBits(Direction::Uplink);
                                            for ss_id in 0..config_ref.num_spatial_streams() {
                                                let demod_buf = demod_buffer_mut.get_mut().get_mut(
                                                    frame_slot,
                                                    data_symbol_idx_ul,
                                                    ss_id,
                                                );
                                                let demod_ptr = demod_buf
                                                    .as_mut_ptr()
                                                    .add(mod_order_bits * base_sc_id);
                                                demod_bufs[ss_id] = demod_ptr as *mut libc::c_void;
                                            }

                                            let equal_temp_trans = equaled_buf_temp_trans
                                                .as_mut_ptr()
                                                as *mut libc::c_void;
                                            let equal_temp =
                                                equaled_buf_temp.as_ptr() as *mut libc::c_void;

                                            Demod_wrap(
                                                config_ref.num_spatial_streams(),
                                                equal_temp,
                                                equal_temp_trans,
                                                max_sc_ite,
                                                total_symbol_idx_ul,
                                                mod_order_bits,
                                                UplinkHardDemod,
                                                demod_bufs.as_mut_ptr() as *mut *mut libc::c_void,
                                                demod_bufs.len(),
                                            );
                                        }
                                    }
                                    // println!(
                                    //     "Demul done for frame {}, slot {}, ant {}, symbol {}",
                                    //     frame_id, frame_slot, node_index, symbol_id
                                    // );
                                    CmTypes::from_any((frame_id, symbol_id, base_sc_id))
                                })
                                .expect("Failed to access FftBuffer struct or wrong type")
                        })
                        .expect("Failed to access UlBeamMatrix or wrong type")
                })
                .expect("Failed to access Framestats struct or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}
