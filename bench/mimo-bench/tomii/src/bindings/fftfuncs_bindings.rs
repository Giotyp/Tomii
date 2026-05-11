use crate::common::symbols::SymbolType;
use libc;

#[link(name = "fftfuncs")]
extern "C" {
    pub fn PartialTranspose(
        out_buffer: *mut libc::c_void,
        ant_id: usize,
        bs_ant_num: usize,
        symbol_type: SymbolType,
        ofdm_data_num: usize,
        ofdm_data_start: usize,
        fft_inout: *const libc::c_void,
        pilots_sgn: *const libc::c_void,
        TransposeBlockSize: usize,
        SCsPerCacheline: usize,
    );

    pub fn SimdConvertShortToFloat(
        in_buf: *const libc::c_void,
        out_buf: *mut libc::c_void,
        n_elems: usize,
    );

    pub fn expand_csi(
        ofdm_data_num: usize,
        bs_ant_num: usize,
        ue_ant_num: usize,
        frame_slot: usize,
        ant_id: usize,
        src_buf: *const libc::c_void,
        TransposeBlockSize: usize,
        dst_bufs_ptr: *mut *mut libc::c_void,
        dst_bufs_len: usize,
    );

    pub fn PartialTransposeSliced(
        out_buffer: *mut libc::c_void,
        bs_ant_num: usize,
        symbol_type: SymbolType,
        ofdm_data_num: usize,
        ofdm_data_start: usize,
        fft_inout: *const libc::c_void,
        pilots_sgn: *const libc::c_void,
        TransposeBlockSize: usize,
        SCsPerCacheline: usize,
    );
}
