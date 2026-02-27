#![allow(non_upper_case_globals)]

use crate::common::structures::AlignedVec;
use crate::common::symbols::{MaxAntennas, MaxUEs};
use libc;
use num_complex::Complex32;
#[link(name = "beamfuncs")]
extern "C" {
    pub fn Precoder(
        csi_gather_mem: *mut libc::c_void,
        ul_beam_mem: *mut libc::c_void,
        bs_ant_num: usize,
        num_streams: usize,
        ue_num: usize,
    );

    pub fn PartialTransposeGather(
        cur_sc_id: usize,
        src: *const libc::c_void,
        dst: *mut libc::c_void,
        bs_ant_num: usize,
        UseSIMDGather: bool,
        TransposeBlockSize: usize,
    );
}

pub struct Beam {
    pub csi_gather: AlignedVec<Complex32>,
    /// Pre-allocated workspace for the Gram matrix G = H^H H (MaxUEs × MaxUEs).
    /// Used by the native Rust ZF precoder to avoid per-call heap allocation.
    pub gram_workspace: AlignedVec<Complex32>,
}

impl Beam {
    pub fn new(alignment: usize) -> Self {
        let csi_gather = AlignedVec::new(MaxAntennas * MaxUEs, alignment);
        let gram_workspace = AlignedVec::new(MaxUEs * MaxUEs, alignment);

        Self {
            csi_gather,
            gram_workspace,
        }
    }
}
