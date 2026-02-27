#![allow(non_upper_case_globals)]

use std::cell::RefCell;
use std::cmp::min;

use cblas;
use lapack;
use num_complex::Complex32;

use crate::bindings::beamfuncs_bindings::*;
use crate::buffer_lib::*;
use crate::common::config::Config;
use crate::common::structures::AlignedVec;
use crate::common::symbols::*;
use synstream_types::CmTypes;

// Per-worker-thread scratch workspace for beam_op_ptr.
// Avoids the shared-singleton contention that serialized all 400 beam tasks
// when `beam_struct.csi_gather / gram_workspace` was a single CmTypes::Any object.
thread_local! {
    static TL_CSI_GATHER: RefCell<AlignedVec<Complex32>> =
        RefCell::new(AlignedVec::new(MaxAntennas * MaxUEs, Alignment));
    static TL_GRAM_WORKSPACE: RefCell<AlignedVec<Complex32>> =
        RefCell::new(AlignedVec::new(MaxUEs * MaxUEs, Alignment));
}

/// Zero-Forcing precoder: W = (H^H H)^{-1} H^H, using pre-allocated workspace.
///
/// All matrices are column-major.
/// - `csi_ptr`:   H  [bs_ant × num_streams], lda = bs_ant
/// - `ul_beam_ptr`: W [ue_ant × bs_ant], ldb = ue_ant  (ue_ant >= num_streams)
/// - `gram_ptr`:  scratch [num_streams × num_streams]
///
/// Uses MKL sequential BLAS/LAPACK (via cblas/lapack crates). No heap allocation.
/// Falls back to Armadillo `Precoder` if the Gram matrix is singular.
unsafe fn zf_precoder(
    csi_ptr: *const Complex32,
    ul_beam_ptr: *mut Complex32,
    gram_ptr: *mut Complex32,
    bs_ant: usize,
    num_streams: usize,
    ue_ant: usize,
) {
    let n = num_streams as i32;
    let k = bs_ant as i32;
    let ldb = ue_ant as i32;

    // Step 1: G = H^H * H  (upper triangle of num_streams × num_streams Gram matrix)
    // cherk(ConjTrans): C = alpha * A^H * A + beta * C
    //   A = H [bs_ant × num_streams], lda = bs_ant
    //   C = G [num_streams × num_streams], ldc = num_streams
    {
        let csi_slice =
            std::slice::from_raw_parts(csi_ptr as *const cblas::c32, bs_ant * num_streams);
        let gram_slice =
            std::slice::from_raw_parts_mut(gram_ptr as *mut cblas::c32, num_streams * num_streams);
        cblas::cherk(
            cblas::Layout::ColumnMajor,
            cblas::Part::Upper,
            cblas::Transpose::Conjugate,
            n,
            k,
            1.0_f32,
            csi_slice,
            k,
            0.0_f32,
            gram_slice,
            n,
        );
    }

    // Step 2: Write H^H into the output buffer as the initial RHS for the solve.
    // H[j, i]  at csi_ptr[i * bs_ant + j]   (col-major, bs_ant rows, num_streams cols)
    // H^H[i,j] = conj(H[j, i])
    // ul_beam[i, j] at ul_beam_ptr[j * ue_ant + i] (col-major, ldb = ue_ant)
    for i in 0..num_streams {
        for j in 0..bs_ant {
            let h_val = *csi_ptr.add(i * bs_ant + j);
            *ul_beam_ptr.add(j * ue_ant + i) = h_val.conj();
        }
    }

    // Step 3: Cholesky factorisation of G in-place: G = U^H U  (upper stored)
    let mut info = 0i32;
    {
        let gram_slice =
            std::slice::from_raw_parts_mut(gram_ptr as *mut cblas::c32, num_streams * num_streams);
        lapack::cpotrf(b'U', n, gram_slice, n, &mut info);
    }
    if info != 0 {
        // G is singular or near-singular — fall back to Armadillo (pseudoinverse path).
        Precoder(
            csi_ptr as *mut libc::c_void,
            ul_beam_ptr as *mut libc::c_void,
            bs_ant,
            num_streams,
            ue_ant,
        );
        return;
    }

    // Step 4: Solve (U^H U) W = H^H in-place → W = (H^H H)^{-1} H^H
    // cpotrs: n = num_streams, nrhs = bs_ant, lda = n, ldb = ue_ant
    {
        let gram_slice =
            std::slice::from_raw_parts(gram_ptr as *const cblas::c32, num_streams * num_streams);
        let ul_slice =
            std::slice::from_raw_parts_mut(ul_beam_ptr as *mut cblas::c32, ue_ant * bs_ant);
        lapack::cpotrs(b'U', n, k, gram_slice, n, ul_slice, ldb, &mut info);
    }
}

const SIMDGather: bool = true;
const Alignment: usize = 64;

#[no_mangle]
pub fn create_ul_base_scs(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let v: Vec<CmTypes> = (0..config_ref.beam_events_per_symbol())
                .map(|i| CmTypes::Usize(i * config_ref.beam_block_size()))
                .collect();
            CmTypes::new_vec(v)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn ul_base_scs_len(ul_base_scs: Vec<usize>) -> usize {
    ul_base_scs.len()
}

#[no_mangle]
pub fn beam_events_per_symbol(config: &CmTypes) -> usize {
    config
        .with_any(|config_ref: &Config| config_ref.beam_events_per_symbol())
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn create_beam_struct() -> CmTypes {
    let beam_struct = Beam::new(Alignment);
    CmTypes::from_any(beam_struct)
}

#[no_mangle]
pub fn create_ul_beam_matrices(config: &CmTypes) -> CmTypes {
    config
        .with_any(|config_ref: &Config| {
            let ul_beam_matrices = UlBeamMatrix::new(&config_ref);
            CmTypes::from_any(ul_beam_matrices)
        })
        .expect("Failed to access Config struct or wrong type")
}

#[no_mangle]
pub fn beam_op(
    config: &CmTypes,
    ul_base_scs: &[usize],
    beam_struct: &CmTypes,
    csi_buffer: &CmTypes,
    ul_beam_matrices: &CmTypes,
    frame_id: usize,
    node_index: usize,
) -> usize {
    config
        .with_any(|config_ref: &Config| {
            ul_beam_matrices
                .with_any_mut(|ul_beam_matrices_mut: &mut UlBeamMatrix| {
                    beam_struct
                        .with_any_mut(|beam_struct_mut: &mut Beam| {
                            csi_buffer
                                .with_any_mut(|csi_buffer_mut: &mut CsiBuffer| {
                                    let frame_slot = frame_id % FrameWnd;

                                    let base_sc_id = ul_base_scs[node_index % ul_base_scs.len()];
                                    let beam_block = config_ref.beam_block_size();

                                    let csi_gather = beam_struct_mut.csi_gather.get_mut();

                                    let last_sc_id = base_sc_id
                                        + min(beam_block, config_ref.ofdm_data_num() - base_sc_id);

                                    // Choose Sc iteration
                                    let (sc_inc, start_sc) = match config_ref.freq_orth_pilot() {
                                        true => {
                                            //For FreqOrthogonalPilot only process the first sc in each group
                                            let remain =
                                                base_sc_id % config_ref.pilot_sc_group_size();
                                            if remain != 0 {
                                                (
                                                    config_ref.pilot_sc_group_size(),
                                                    base_sc_id + config_ref.pilot_sc_group_size()
                                                        - remain,
                                                )
                                            } else {
                                                (config_ref.pilot_sc_group_size(), base_sc_id)
                                            }
                                        }
                                        false => (1, base_sc_id),
                                    };

                                    // Handle each subcarrier in range (base_sc_id : last_sc_id -1)
                                    for cur_sc_id in (start_sc..last_sc_id).step_by(sc_inc) {
                                        // Gather CSI matrices of each pilot from csi buffer
                                        // For each subcarrier iterate through all users and gather
                                        // data for all antennas

                                        let ue_list =
                                            config_ref.ScheduledUeList(frame_id, cur_sc_id);
                                        let num_streams = ue_list.len();
                                        if num_streams == 0 {
                                            continue;
                                        }
                                        for selected_ue_idx in 0..num_streams {
                                            let ue_idx = ue_list[selected_ue_idx];

                                            let csi_gather_offset =
                                                config_ref.bs_ant_num() * selected_ue_idx;
                                            let csi_gather_ptr = unsafe {
                                                csi_gather.as_mut_ptr().add(csi_gather_offset)
                                            };

                                            let csi_buf =
                                                csi_buffer_mut.get().get(frame_slot, ue_idx);
                                            let csi_buf_ptr = csi_buf.as_ptr();

                                            unsafe {
                                                PartialTransposeGather(
                                                    cur_sc_id,
                                                    csi_buf_ptr as *const libc::c_void,
                                                    csi_gather_ptr as *mut libc::c_void,
                                                    config_ref.bs_ant_num(),
                                                    SIMDGather,
                                                    TransposeBlockSize,
                                                );
                                            }
                                        }
                                        // Get uplink buffer
                                        let ul_buf_ptr = ul_beam_matrices_mut
                                            .get_mut()
                                            .get_mut(frame_slot, cur_sc_id)
                                            .as_mut_ptr()
                                            as *mut libc::c_void;

                                        let csi_ptr = csi_gather.as_mut_ptr();
                                        unsafe {
                                            Precoder(
                                                csi_ptr as *mut libc::c_void,
                                                ul_buf_ptr,
                                                config_ref.bs_ant_num(),
                                                num_streams,
                                                config_ref.ue_ant_num(),
                                            );
                                        }
                                    }
                                    // println!(
                                    //     "Beam operation completed for frame_id: {}, node_index: {}",
                                    //     frame_id, node_index
                                    // );
                                    frame_id
                                })
                                .expect("Failed to access CsiBuffer struct or wrong type")
                        })
                        .expect("Failed to access Beam struct or wrong type")
                })
                .expect("Failed to access UlBeamMatrix or wrong type")
        })
        .expect("Failed to access Config struct or wrong type")
}

// ************ Unsafe Beam operations ************
#[no_mangle]
pub fn beam_op_ptr(
    config: &CmTypes,
    base_sc_id: usize,
    _beam_struct: &CmTypes, // workspace is now per-thread (TL_CSI_GATHER / TL_GRAM_WORKSPACE)
    csi_buffer: &CmTypes,
    ul_beam_matrices: &CmTypes,
    frame_id: usize,
) -> usize {
    let config_ptr = unsafe { config.as_mut_ptr::<Config>().unwrap().0 as usize };
    let csi_buffer_ptr = unsafe { csi_buffer.as_mut_ptr::<CsiBuffer>().unwrap().0 as usize };
    let ul_beam_matrices_ptr =
        unsafe { ul_beam_matrices.as_mut_ptr::<UlBeamMatrix>().unwrap().0 as usize };

    let config_ref = unsafe { &*(config_ptr as *const Config) };
    let csi_buffer_mut = unsafe { &mut *(csi_buffer_ptr as *mut CsiBuffer) };
    let ul_beam_matrices_mut = unsafe { &mut *(ul_beam_matrices_ptr as *mut UlBeamMatrix) };

    let frame_slot = frame_id % FrameWnd;
    let beam_block = config_ref.beam_block_size();
    let last_sc_id = base_sc_id + min(beam_block, config_ref.ofdm_data_num() - base_sc_id);

    // Choose Sc iteration
    let (sc_inc, start_sc) = match config_ref.freq_orth_pilot() {
        true => {
            //For FreqOrthogonalPilot only process the first sc in each group
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

    // Use per-thread scratch buffers — each Rayon worker gets its own
    // csi_gather / gram_workspace, so all 400 beam tasks run in parallel
    // with no shared-state contention.
    TL_CSI_GATHER.with(|tl_csi| {
        TL_GRAM_WORKSPACE.with(|tl_gram| {
            let mut csi_guard = tl_csi.borrow_mut();
            let mut gram_guard = tl_gram.borrow_mut();
            let csi_gather = csi_guard.as_mut_slice();
            let gram_workspace = gram_guard.as_mut_slice();

            // Handle each subcarrier in range (base_sc_id : last_sc_id -1)
            for cur_sc_id in (start_sc..last_sc_id).step_by(sc_inc) {
                let mut ue_buf = [0usize; MaxUEs];
                let num_streams =
                    config_ref.scheduled_ue_slice(frame_id, cur_sc_id, &mut ue_buf);
                if num_streams == 0 {
                    continue;
                }
                let ue_list = &ue_buf[..num_streams];
                for selected_ue_idx in 0..num_streams {
                    let ue_idx = ue_list[selected_ue_idx];

                    let csi_gather_offset = config_ref.bs_ant_num() * selected_ue_idx;
                    let csi_gather_ptr =
                        unsafe { csi_gather.as_mut_ptr().add(csi_gather_offset) };

                    let csi_buf = csi_buffer_mut.get().get(frame_slot, ue_idx);
                    let csi_buf_ptr = csi_buf.as_ptr();

                    unsafe {
                        PartialTransposeGather(
                            cur_sc_id,
                            csi_buf_ptr as *const libc::c_void,
                            csi_gather_ptr as *mut libc::c_void,
                            config_ref.bs_ant_num(),
                            SIMDGather,
                            TransposeBlockSize,
                        );
                    }
                }
                // Get uplink buffer
                let ul_buf_ptr = ul_beam_matrices_mut
                    .get_mut()
                    .get_mut(frame_slot, cur_sc_id)
                    .as_mut_ptr() as *mut libc::c_void;

                let csi_ptr = csi_gather.as_mut_ptr();
                let gram_ptr = gram_workspace.as_mut_ptr();
                unsafe {
                    zf_precoder(
                        csi_ptr,
                        ul_buf_ptr as *mut Complex32,
                        gram_ptr,
                        config_ref.bs_ant_num(),
                        num_streams,
                        config_ref.ue_ant_num(),
                    );
                }
            }
            frame_id
        })
    })
}
