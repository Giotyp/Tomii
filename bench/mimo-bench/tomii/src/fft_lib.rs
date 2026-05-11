use crate::bindings::fftfuncs_bindings::*;
use crate::bindings::mkl_bindings::Fft;
use crate::buffer_lib::FftBuffer;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::symbols::{FrameWnd, SCsPerCacheline, TransposeBlockSize};
use crate::packet_lib::*;
use tomii_macro::tomii_export;

#[tomii_export]
pub fn create_fft_buffer(config: &Config, framestats: &FrameStats) -> FftBuffer {
    FftBuffer::new(config, framestats)
}

#[tomii_export]
pub fn create_fft_struct(config: &Config) -> Fft {
    Fft::new(config.ofdm_ca_num())
}

#[tomii_export]
pub fn fft_op(
    packet: &Packet,
    config: &Config,
    framestats: &FrameStats,
    fft_struct: &mut Fft,
    fft_buffer: &mut FftBuffer,
    _index: usize,
) -> usize {
    let frame_id = packet.frame_id as usize;
    let frame_slot = frame_id % FrameWnd;

    let ant_id = packet.ant_id as usize;
    let symbol_id = packet.symbol_id as usize;
    let symbol_type = framestats.GetSymbolType(symbol_id);

    let sample_offset = config.ofdm_rx_zero_prefix_bs();
    let data_offset = config.GetDataOffset(frame_slot, symbol_id, framestats);

    let packet_ptr = unsafe {
        packet.data.as_ptr().add(2 * sample_offset) as *const i16
    };
    fft_struct.convert_short_to_float(packet_ptr);
    fft_struct.computefft();
    fft_struct.inout_shift(config.ofdm_ca_num());

    let fft_buf = fft_buffer.get_mut();
    let fft_buffer_ptr = fft_buf.get_mut(data_offset).as_mut_ptr() as *mut libc::c_void;

    unsafe {
        PartialTranspose(
            fft_buffer_ptr,
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

    frame_id
}
