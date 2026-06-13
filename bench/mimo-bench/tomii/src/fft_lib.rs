use crate::bindings::fftfuncs_bindings::*;
use crate::bindings::mkl_bindings::Fft;
use crate::buffer_lib::FftBuffer;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::symbols::{FrameWnd, SCsPerCacheline, TransposeBlockSize};
use crate::packet_lib::*;
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

// Obtain a *mut T from CmTypes::Any or CmTypes::AnyHeld (zero-lock bulk path).
unsafe fn raw_mut<T: std::any::Any + Send + Sync + 'static>(cm: &CmTypes) -> *mut T {
    if let CmTypes::AnyHeld(data) = cm {
        return unsafe { data.downcast_ref::<T>() }
            .map(|r| r as *const T as *mut T)
            .unwrap_or_else(|| {
                panic!(
                    "raw_mut AnyHeld: wrong type for {}",
                    std::any::type_name::<T>()
                )
            });
    }
    unsafe { cm.as_mut_ptr::<T>() }
        .map(|g| g.ptr)
        .unwrap_or_else(|| panic!("raw_mut Any: wrong type for {}", std::any::type_name::<T>()))
}

#[tomii_export]
pub fn create_fft_buffer(config: &Config, framestats: &FrameStats) -> FftBuffer {
    FftBuffer::new(config, framestats)
}

#[tomii_export]
pub fn create_fft_struct(config: &Config) -> Fft {
    Fft::new(config.ofdm_ca_num())
}

// Hand-coded `_cm` bridge: `fft_buffer` is taken as `&CmTypes` (shared `&`), not
// `&mut FftBuffer`. The `&mut` parameter carries LLVM `noalias`, which is UB when
// concurrent fft tasks share the buffer; writes go to disjoint rows via raw
// `row_ptr`. `fft_struct` stays `&mut` — it is per-task (factored), so unique.
#[no_mangle]
pub fn fft_op_cm(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    fft_buffer: &CmTypes,
    _index: usize,
) -> CmTypes {
    let packet = unsafe { &*raw_mut::<Packet>(packet) };
    let config = unsafe { &*raw_mut::<Config>(config) };
    let framestats = unsafe { &*raw_mut::<FrameStats>(framestats) };
    let fft_struct = unsafe { &mut *raw_mut::<Fft>(fft_struct) };
    let fft_buffer = unsafe { &*raw_mut::<FftBuffer>(fft_buffer) };

    let frame_id = packet.frame_id as usize;
    let frame_slot = frame_id % FrameWnd;

    let ant_id = packet.ant_id as usize;
    let symbol_id = packet.symbol_id as usize;
    let symbol_type = framestats.GetSymbolType(symbol_id);

    let sample_offset = config.ofdm_rx_zero_prefix_bs();
    let data_offset = config.GetDataOffset(frame_slot, symbol_id, framestats);

    let packet_ptr = unsafe { packet.data.as_ptr().add(2 * sample_offset) as *const i16 };
    fft_struct.convert_short_to_float(packet_ptr);
    fft_struct.computefft();
    fft_struct.inout_shift(config.ofdm_ca_num());

    // Disjoint per-antenna write into this symbol's row via raw ptr (shared &self).
    let fft_buffer_ptr = fft_buffer.row_ptr(data_offset) as *mut libc::c_void;

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

    CmTypes::Usize(frame_id)
}
