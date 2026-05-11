use crate::common::structures::AlignedVec;
use crate::common::symbols::{MaxAntennas, MaxUEs, SCsPerCacheline};
use num_complex::Complex32;

#[link(name = "demod")]
extern "C" {

    pub fn Equalization(
        equal_buf: *mut libc::c_void,
        data_gather_buf: *const libc::c_void,
        n_users: usize,
        ul_beam_buf: *const libc::c_void,
        bs_ant_num: usize,
    );

    pub fn Demod_wrap(
        n_users: usize,
        equaled_buffer_temp: *mut libc::c_void,
        equaled_buffer_temp_transposed: *mut libc::c_void,
        max_sc_ite: usize,
        total_symbol_idx_ul: usize,
        mod_order: usize,
        hard_demod: bool,
        demod_bufs_ptr: *mut *mut libc::c_void,
        demod_bufs_len: usize,
    );

    pub fn DemulGather(
        TransposeBlockSize: usize,
        base_sc_id: usize,
        data_buf: *const libc::c_void,
        data_gather_buffer_: *mut libc::c_void,
        UseSIMDGather: bool,
        SCsPerCacheline: usize,
        i: usize,
        bs_ant_num: usize,
        partial_transpose_block_base: usize,
    );
}

pub struct Demul {
    pub demul_block: usize,
    pub data_gather_align: AlignedVec<Complex32>,
    pub equaled_align: AlignedVec<Complex32>,
    pub equaled_trans_align: AlignedVec<Complex32>,
}

impl Demul {
    pub fn new(demul_block: usize) -> Self {
        let data_gather_align: AlignedVec<Complex32> =
            AlignedVec::new(SCsPerCacheline * MaxAntennas as usize, 64);

        let equaled_align: AlignedVec<Complex32> =
            AlignedVec::new(demul_block * MaxUEs as usize, 64);

        let equaled_trans_align: AlignedVec<Complex32> =
            AlignedVec::new(demul_block * MaxUEs as usize, 64);

        Self {
            demul_block,
            data_gather_align,
            equaled_align,
            equaled_trans_align,
        }
    }
}
