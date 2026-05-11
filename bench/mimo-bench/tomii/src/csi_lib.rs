use crate::bindings::fftfuncs_bindings::*;
use crate::bindings::mkl_bindings::Fft;
use crate::buffer_lib::CsiBuffer;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::symbols::{FrameWnd, SCsPerCacheline, TransposeBlockSize};
use crate::packet_lib::*;
use tomii_macro::tomii_export;

#[tomii_export]
pub fn create_csi_buffer(config: &Config) -> CsiBuffer {
    CsiBuffer::new(config)
}

#[tomii_export]
pub fn csi_op(
    packet: &Packet,
    config: &Config,
    framestats: &FrameStats,
    fft_struct: &mut Fft,
    csi_buffer: &mut CsiBuffer,
) -> usize {
    let frame_id = packet.frame_id as usize;
    let frame_slot = frame_id % FrameWnd;

    let ant_id = packet.ant_id as usize;
    let symbol_id = packet.symbol_id as usize;
    let symbol_type = framestats.GetSymbolType(symbol_id);

    let sample_offset = config.ofdm_rx_zero_prefix_bs();

    let packet_ptr = unsafe {
        packet.data.as_ptr().add(2 * sample_offset) as *const i16
    };
    fft_struct.convert_short_to_float(packet_ptr);
    fft_struct.computefft();
    fft_struct.inout_shift(config.ofdm_ca_num());

    let pilot_symbol_id = framestats.GetPilotSymbolIdx(symbol_id);

    let csi_buf = csi_buffer.get_mut();

    let csi_cell_ptr = csi_buf
        .get_mut(frame_slot, pilot_symbol_id)
        .as_mut_ptr() as *mut libc::c_void;

    unsafe {
        PartialTranspose(
            csi_cell_ptr,
            ant_id,
            config.bs_ant_num(),
            symbol_type,
            config.ofdm_data_num(),
            config.ofdm_data_start(),
            fft_struct.fft_inout_align.get().as_ptr() as *const libc::c_void,
            config.pilots_sgn().as_ptr() as *const libc::c_void,
            TransposeBlockSize,
            SCsPerCacheline,
        );
    }

    // Expand partial CSI from freq-orth pilot to full CSI per UE
    if config.freq_orth_pilot() && pilot_symbol_id == framestats.NumPilotSyms() - 1 {
        let csi_buf = csi_buffer.get_mut();
        let src_buf = csi_buf.get(frame_slot, 0).as_ptr();

        let mut dst_bufs: Vec<*mut libc::c_void> =
            vec![std::ptr::null_mut(); config.ue_ant_num()];

        for ue_id in (0..config.ue_ant_num()).rev() {
            let dst_buf = csi_buf.get_mut(frame_slot, ue_id).as_mut_ptr();
            dst_bufs[ue_id] = dst_buf as *mut libc::c_void;
        }
        unsafe {
            expand_csi(
                config.ofdm_data_num(),
                config.bs_ant_num(),
                config.ue_ant_num(),
                frame_slot,
                ant_id,
                src_buf as *const libc::c_void,
                TransposeBlockSize,
                dst_bufs.as_mut_ptr() as *mut *mut libc::c_void,
                dst_bufs.len(),
            );
        }
    }

    frame_id
}
