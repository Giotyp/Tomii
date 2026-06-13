#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::cmp::min;

use num_complex::Complex32;

use crate::bindings::beamfuncs_bindings::*;
use crate::buffer_lib::*;
use crate::common::config::Config;
use crate::common::symbols::*;
use tomii_macro::tomii_export;
use tomii_types::CmTypes;

// Obtain a *mut T from either CmTypes::Any or CmTypes::AnyHeld.
// AnyHeld is the zero-lock upgrade used by the bulk-task execution path.
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
        .unwrap_or_else(|| {
            panic!(
                "raw_mut Any: wrong type for {} (got {:?})",
                std::any::type_name::<T>(),
                cm
            )
        })
}

const SIMDGather: bool = true;
const Alignment: usize = 64;

// Per-worker-thread scratch — avoids lock contention across parallel beam tasks.
thread_local! {
    static TL_CSI_GATHER: RefCell<Vec<Complex32>> =
        RefCell::new(vec![Complex32::new(0.0, 0.0); MaxAntennas * MaxUEs]);
}

#[no_mangle]
pub fn create_ul_base_scs(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let v: Vec<CmTypes> = (0..config_ref.beam_events_per_symbol())
                .map(|i| CmTypes::Usize(i * config_ref.beam_block_size()))
                .collect();
            CmTypes::new_vec(v)
        })
        .expect("create_ul_base_scs: expected Config")
}

#[tomii_export]
pub fn beam_events_per_symbol(config: &Config) -> usize {
    config.beam_events_per_symbol()
}

#[tomii_export]
pub fn create_beam_struct() -> Beam {
    Beam::new(Alignment)
}

#[tomii_export]
pub fn create_ul_beam_matrices(config: &Config) -> UlBeamMatrix {
    UlBeamMatrix::new(config)
}

// beam_op_cm: directly exported under the name the wrapper looks for.
// Uses raw pointers + thread-local scratch to allow all 400 beam tasks to
// run in parallel without lock contention on csi_buffer / ul_beam_matrices.
// Safety: beam tasks partition by subcarrier range so memory accesses are disjoint.
#[no_mangle]
pub fn beam_op_cm(
    config: &CmTypes,
    ul_base_scs: &CmTypes, // CmTypes::new_vec of Usize values (one per beam instance)
    _beam_struct: &CmTypes, // kept for API compat; scratch is thread-local
    csi_buffer: &CmTypes,
    ul_beam_matrices: &CmTypes,
    frame_id: usize,
    node_index: usize,
) -> CmTypes {
    // Extract base_sc_id for this instance.
    // create_ul_base_scs returns CmTypes::new_vec of Usize elements.
    let base_sc_id = if let Some(v) = ul_base_scs.as_vec() {
        match v[node_index % v.len()] {
            CmTypes::Usize(x) => x,
            _ => panic!("beam_op_cm: expected Usize in ul_base_scs vec"),
        }
    } else {
        ul_base_scs
            .with_any::<Vec<usize>, _, _>(|v| v[node_index % v.len()])
            .expect("beam_op_cm: expected Vec<usize> or VecCmt for ul_base_scs")
    };

    // Raw ptr access — bypasses RwLock; safe because:
    //   - csi_buffer is read-only (we only call .get())
    //   - ul_beam_matrices writes are partitioned by subcarrier index
    //   - args arrive as AnyHeld (zero-lock) in the bulk-task path
    let config_ref = unsafe { &*raw_mut::<Config>(config) };
    let csi_buffer_ref = unsafe { &*raw_mut::<CsiBuffer>(csi_buffer) };
    // Shared `&` (NOT `&mut`): concurrent beam tasks write disjoint subcarrier
    // cells via raw `cell_ptr`, never forming an aliased `&mut UlBeamMatrix`
    // (which is UB and miscompiles under W>1).
    let ul_beam_matrices_ref = unsafe { &*raw_mut::<UlBeamMatrix>(ul_beam_matrices) };

    let frame_slot = frame_id % FrameWnd;
    let beam_block = config_ref.beam_block_size();
    let last_sc_id = base_sc_id + min(beam_block, config_ref.ofdm_data_num() - base_sc_id);

    let (sc_inc, start_sc) = match config_ref.freq_orth_pilot() {
        true => {
            let remain = base_sc_id % config_ref.pilot_sc_group_size();
            if remain != 0 {
                (
                    config_ref.pilot_sc_group_size(),
                    base_sc_id + config_ref.pilot_sc_group_size() - remain,
                )
            } else {
                (config_ref.pilot_sc_group_size(), base_sc_id)
            }
        }
        false => (1, base_sc_id),
    };

    TL_CSI_GATHER.with(|tl| {
        let mut csi_gather = tl.borrow_mut();

        for cur_sc_id in (start_sc..last_sc_id).step_by(sc_inc) {
            let ue_list = config_ref.ScheduledUeList(frame_id, cur_sc_id);
            let num_streams = ue_list.len();
            if num_streams == 0 {
                continue;
            }
            // Zero the gather region before filling it: Precoder reads up to
            // ue_ant_num channel rows, but only `num_streams` are gathered here.
            // Without this, unscheduled-UE rows hold residual from a previously
            // processed subcarrier/beam task — and task dispatch order is not
            // pinned, so that residual (and thus the beam weights) varies
            // run-to-run, breaking determinism. Zeroing makes it reproducible.
            let clear_len = config_ref.bs_ant_num() * config_ref.ue_ant_num();
            csi_gather[..clear_len].fill(Complex32::new(0.0, 0.0));
            for selected_ue_idx in 0..num_streams {
                let ue_idx = ue_list[selected_ue_idx];
                let csi_gather_ptr = unsafe {
                    csi_gather
                        .as_mut_ptr()
                        .add(config_ref.bs_ant_num() * selected_ue_idx)
                };
                let csi_buf = csi_buffer_ref.get().get(frame_slot, ue_idx);
                unsafe {
                    PartialTransposeGather(
                        cur_sc_id,
                        csi_buf.as_ptr() as *const libc::c_void,
                        csi_gather_ptr as *mut libc::c_void,
                        config_ref.bs_ant_num(),
                        SIMDGather,
                        TransposeBlockSize,
                    );
                }
            }

            let ul_buf_ptr =
                ul_beam_matrices_ref.cell_ptr(frame_slot, cur_sc_id) as *mut libc::c_void;
            unsafe {
                Precoder(
                    csi_gather.as_mut_ptr() as *mut libc::c_void,
                    ul_buf_ptr,
                    config_ref.bs_ant_num(),
                    num_streams,
                    config_ref.ue_ant_num(),
                );
            }
        }
    });

    CmTypes::Usize(frame_id)
}
