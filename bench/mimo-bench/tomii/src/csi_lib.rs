use crate::bindings::fftfuncs_bindings::*;
use crate::bindings::mkl_bindings::Fft;
use crate::buffer_lib::CsiBuffer;
use crate::common::config::Config;
use crate::common::framestats::FrameStats;
use crate::common::symbols::{frame_wnd, SCsPerCacheline, TransposeBlockSize};
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
pub fn create_csi_buffer(config: &Config) -> CsiBuffer {
    CsiBuffer::new(config)
}

// Hand-coded `_cm` bridge: `csi_buffer` is taken as `&CmTypes` (shared `&`), not
// `&mut CsiBuffer`, to avoid the aliased-`&mut` `noalias` UB under W>1; writes go
// to disjoint cells via raw `cell_ptr`. `fft_struct` stays `&mut` (per-task).
#[no_mangle]
pub fn csi_op_cm(
    packet: &CmTypes,
    config: &CmTypes,
    framestats: &CmTypes,
    fft_struct: &CmTypes,
    csi_buffer: &CmTypes,
) -> CmTypes {
    let packet = unsafe { &*raw_mut::<Packet>(packet) };
    let config = unsafe { &*raw_mut::<Config>(config) };
    let framestats = unsafe { &*raw_mut::<FrameStats>(framestats) };
    // Shared `&`: one Fft per instance index is shared by every slot, so its
    // scratch must not be used; the FFT runs into per-thread scratch instead.
    let fft_struct = unsafe { &*raw_mut::<Fft>(fft_struct) };
    let csi_buffer = unsafe { &*raw_mut::<CsiBuffer>(csi_buffer) };

    let frame_id = packet.frame_id as usize;
    crate::e1probe::on_task_start(frame_id);
    crate::mimo_count(0, frame_id);
    let frame_slot = frame_id % frame_wnd();

    let ant_id = packet.ant_id as usize;
    let symbol_id = packet.symbol_id as usize;
    let symbol_type = framestats.GetSymbolType(symbol_id);

    let sample_offset = config.ofdm_rx_zero_prefix_bs();

    let packet_ptr = unsafe { packet.data.as_ptr().add(2 * sample_offset) as *const i16 };
    let pilot_symbol_id = framestats.GetPilotSymbolIdx(symbol_id);

    crate::bindings::mkl_bindings::with_fft_scratch(
        fft_struct,
        packet_ptr,
        config.ofdm_ca_num(),
        |fft_out| {
            // Disjoint per-antenna write into this pilot's cell via raw ptr (shared &self).
            let csi_cell_ptr =
                csi_buffer.cell_ptr(frame_slot, pilot_symbol_id) as *mut libc::c_void;

            unsafe {
                PartialTranspose(
                    csi_cell_ptr,
                    ant_id,
                    config.bs_ant_num(),
                    symbol_type,
                    config.ofdm_data_num(),
                    config.ofdm_data_start(),
                    fft_out as *const libc::c_void,
                    config.pilots_sgn().as_ptr() as *const libc::c_void,
                    TransposeBlockSize,
                    SCsPerCacheline,
                );
            }
        },
    );

    // Expand partial CSI from freq-orth pilot to full CSI per UE
    if config.freq_orth_pilot()
        && pilot_symbol_id == framestats.NumPilotSyms() - 1
        && config.pilot_sc_group_size() != TransposeBlockSize
    {
        expand_csi_grouped(config, csi_buffer, frame_slot, ant_id);
    } else if config.freq_orth_pilot() && pilot_symbol_id == framestats.NumPilotSyms() - 1 {
        let src_buf = csi_buffer.get().get(frame_slot, 0).as_ptr();

        let mut dst_bufs: Vec<*mut libc::c_void> = vec![std::ptr::null_mut(); config.ue_ant_num()];

        for ue_id in (0..config.ue_ant_num()).rev() {
            dst_bufs[ue_id] = csi_buffer.cell_ptr(frame_slot, ue_id) as *mut libc::c_void;
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

    crate::e1probe::on_fftcsi(frame_id, None);
    CmTypes::Usize(frame_id)
}

/// Frequency-orthogonal CSI expansion for pilot groups spanning several transpose
/// blocks (e.g. 16 UEs / 16-subcarrier groups, as on Agora's conext branch). UE `u`
/// transmits on subcarrier `group_start + u`; its estimate there is copied to every
/// subcarrier of the group in UE `u`'s CSI cell. Agora's `expand_csi` assumes the
/// group equals one transpose block (UEs <= 8). Only this antenna's rows are touched,
/// so concurrent CSI tasks (one per antenna) stay disjoint.
fn expand_csi_grouped(config: &Config, csi_buffer: &CsiBuffer, frame_slot: usize, ant_id: usize) {
    use num_complex::Complex;
    let t = TransposeBlockSize;
    let n_ant = config.bs_ant_num();
    let n_ue = config.ue_ant_num();
    let g = config.pilot_sc_group_size();
    let n_sc = config.ofdm_data_num();
    assert!(
        g % t == 0 && n_ue <= g && g <= 64 && n_sc % g == 0,
        "unsupported freq-orth pilot group {g} (transpose {t}, ue {n_ue}, sc {n_sc})"
    );
    // Position of (subcarrier, this antenna) in the transposed CSI layout.
    let idx = |sc: usize| (sc / t) * (t * n_ant) + ant_id * t + (sc % t);
    // The pilot symbol's cell is also UE 0's cell: read a group's pilots before writing it.
    let src = csi_buffer.cell_ptr(frame_slot, 0) as *const Complex<f32>;
    let mut dst = [std::ptr::null_mut::<Complex<f32>>(); 64];
    for (u, d) in dst.iter_mut().enumerate().take(n_ue) {
        *d = csi_buffer.cell_ptr(frame_slot, u);
    }
    let mut vals = [Complex::new(0.0f32, 0.0); 64];
    for gs in (0..n_sc).step_by(g) {
        for (u, v) in vals.iter_mut().enumerate().take(n_ue) {
            *v = unsafe { *src.add(idx(gs + u)) };
        }
        for u in 0..n_ue {
            for sc in gs..gs + g {
                unsafe { *dst[u].add(idx(sc)) = vals[u] };
            }
        }
    }
}
