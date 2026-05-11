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
use tomii_macro::tomii_export;

const SIMDGather: bool = true;

#[tomii_export]
pub fn create_demul_base_scs(config: &Config) -> Vec<usize> {
    let mut sc_count = 0;
    let mut demul_base_scs: Vec<usize> = Vec::new();
    for _ in 0..config.demul_events_per_symbol() {
        demul_base_scs.push(sc_count);
        sc_count += config.demul_block_size();
    }
    demul_base_scs
}

#[tomii_export]
pub fn get_ul_symbol(framestats: &FrameStats, symbol_id: usize) -> usize {
    framestats.GetUlSymbol(symbol_id)
}

#[tomii_export]
pub fn total_demul_tasks(config: &Config, framestats: &FrameStats) -> usize {
    config.demul_events_per_symbol() * framestats.NumUlSyms()
}

#[tomii_export]
pub fn demul_events_per_symbol(config: &Config) -> usize {
    config.demul_events_per_symbol()
}

#[tomii_export]
pub fn create_demul_struct(config: &Config) -> Demul {
    Demul::new(config.demul_block_size())
}

#[tomii_export]
pub fn create_demod_buffers(config: &Config, framestats: &FrameStats) -> DemodBuffer {
    DemodBuffer::new(config, framestats)
}

#[tomii_export]
pub fn demul_op(
    config: &Config,
    framestats: &FrameStats,
    demul_base_scs: &Vec<usize>,
    demul_struct: &Demul,
    fft_buffer: &FftBuffer,
    demod_buffers: &mut DemodBuffer,
    ul_beam_matrices: &UlBeamMatrix,
    frame_id: usize,
    symbol_id: usize,
    node_index: usize,
) -> (usize, usize, usize) {
    let frame_slot = frame_id % FrameWnd;
    let base_sc_id = demul_base_scs[node_index % demul_base_scs.len()];

    // Create thread-local working buffers to avoid sharing across concurrent tasks
    let mut data_gather = AlignedVec::<Complex32>::new(SCsPerCacheline * config.bs_ant_num(), 64);
    let mut equaled_buf_temp =
        AlignedVec::<Complex32>::new(demul_struct.demul_block * config.num_spatial_streams(), 64);
    let mut equaled_buf_temp_trans =
        AlignedVec::<Complex32>::new(demul_struct.demul_block * config.num_spatial_streams(), 64);

    let symbol_idx_ul = framestats.GetUlSymbolIdx(symbol_id);
    let total_symbol_idx_ul = config.GetTotalSymbolIdxUl(frame_id, symbol_idx_ul, framestats);

    let data_symbol_idx_ul = symbol_idx_ul - config.client_ul_pilot_symbols();

    let max_sc_ite = min(demul_struct.demul_block, config.ofdm_data_num() - base_sc_id);
    assert!(max_sc_ite % SCsPerCacheline == 0);

    for i in (0..max_sc_ite).step_by(SCsPerCacheline) {
        let partial_transpose_block_base =
            ((base_sc_id + i) / TransposeBlockSize) * (TransposeBlockSize * config.bs_ant_num());

        let bs_ant_num = config.bs_ant_num();

        let data_buf_ptr = fft_buffer.get().get(total_symbol_idx_ul).as_ptr() as *const libc::c_void;

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

        for j in 0..SCsPerCacheline {
            let cur_sc_id = base_sc_id + i + j;

            let offset = j * config.bs_ant_num();
            let data_vec: Vec<Complex32> =
                data_gather.get()[offset..(offset + config.bs_ant_num())].to_vec();

            let ul_beam_vec = ul_beam_matrices
                .get()
                .get(frame_slot, config.GetBeamScId(cur_sc_id))
                .to_vec();

            let ul_buf_ptr = ul_beam_vec.as_ptr() as *const libc::c_void;

            let equal_offset = (cur_sc_id - base_sc_id) * config.num_spatial_streams();
            let eq_slice = &mut equaled_buf_temp.get_mut()[equal_offset..];

            unsafe {
                Equalization(
                    eq_slice.as_mut_ptr() as *mut libc::c_void,
                    data_vec.as_ptr() as *const libc::c_void,
                    config.num_spatial_streams(),
                    ul_buf_ptr,
                    config.bs_ant_num(),
                );
            }
        }
    }

    // Demodulation
    if symbol_idx_ul >= config.client_ul_pilot_symbols() {
        unsafe {
            let mut demod_bufs: Vec<*mut libc::c_void> =
                vec![std::ptr::null_mut(); config.num_spatial_streams()];

            let mod_order_bits = config.ModOrderBits(Direction::Uplink);
            for ss_id in 0..config.num_spatial_streams() {
                let demod_buf = demod_buffers
                    .get_mut()
                    .get_mut(frame_slot, data_symbol_idx_ul, ss_id);
                let demod_ptr = demod_buf.as_mut_ptr().add(mod_order_bits * base_sc_id);
                demod_bufs[ss_id] = demod_ptr as *mut libc::c_void;
            }

            let equal_temp_trans = equaled_buf_temp_trans.get_mut().as_mut_ptr() as *mut libc::c_void;
            let equal_temp = equaled_buf_temp.get().as_ptr() as *mut libc::c_void;

            Demod_wrap(
                config.num_spatial_streams(),
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

    (frame_id, symbol_id, base_sc_id)
}
