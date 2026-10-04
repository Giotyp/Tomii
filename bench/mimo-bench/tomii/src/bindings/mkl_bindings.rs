#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(dead_code)]

use crate::bindings::fftfuncs_bindings::*;
use crate::common::structures::AlignedVec;

use libc;
use num_complex::Complex;

pub const DFTI_CONFIG_VALUE_DFTI_SINGLE: DFTI_CONFIG_VALUE = 35;
pub const DFTI_CONFIG_VALUE_DFTI_COMPLEX: DFTI_CONFIG_VALUE = 32;
pub type DFTI_CONFIG_VALUE = ::std::os::raw::c_uint;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct DFTI_DESCRIPTOR {
    _unused: [u8; 0],
}
pub type DFTI_DESCRIPTOR_HANDLE = *mut DFTI_DESCRIPTOR;

extern "C" {
    pub fn DftiCreateDescriptor(
        arg1: *mut DFTI_DESCRIPTOR_HANDLE,
        arg2: DFTI_CONFIG_VALUE,
        arg3: DFTI_CONFIG_VALUE,
        arg4: ::std::os::raw::c_long,
        ...
    ) -> ::std::os::raw::c_long;
}

extern "C" {
    pub fn DftiCommitDescriptor(arg1: DFTI_DESCRIPTOR_HANDLE) -> ::std::os::raw::c_long;
}

extern "C" {
    pub fn DftiComputeForward(
        arg1: DFTI_DESCRIPTOR_HANDLE,
        arg2: *mut ::std::os::raw::c_void,
        ...
    ) -> ::std::os::raw::c_long;
}

extern "C" {
    pub fn memcpy(
        dest: *mut std::ffi::c_void,
        src: *const std::ffi::c_void,
        n: usize,
    ) -> *mut std::ffi::c_void;
}

#[repr(C, align(64))]
struct FftConfig {
    dfti_single: DFTI_CONFIG_VALUE,
    dfti_complex: DFTI_CONFIG_VALUE,
    dim: i64,
    fft_size: i64,
}

impl FftConfig {
    fn new(ofdm_ca_num: usize) -> Self {
        let dim: i64 = 1;
        let ofdm_ca_num = ofdm_ca_num as i64;
        Self {
            dfti_single: DFTI_CONFIG_VALUE_DFTI_SINGLE,
            dfti_complex: DFTI_CONFIG_VALUE_DFTI_COMPLEX,
            dim,
            fft_size: ofdm_ca_num,
        }
    }
}

pub struct FftDescriptor {
    pub desc: DFTI_DESCRIPTOR_HANDLE,
}
unsafe impl Send for FftDescriptor {}
unsafe impl Sync for FftDescriptor {}

impl FftDescriptor {
    pub fn new(ofdm_ca_num: usize) -> Self {
        let fft_conf = FftConfig::new(ofdm_ca_num);
        let mut mkl_handle: DFTI_DESCRIPTOR_HANDLE = std::ptr::null_mut();
        unsafe {
            DftiCreateDescriptor(
                &mut mkl_handle,
                fft_conf.dfti_single,
                fft_conf.dfti_complex,
                fft_conf.dim,
                fft_conf.fft_size,
            );

            // Commit the descriptor
            DftiCommitDescriptor(mkl_handle);
        }
        Self { desc: mkl_handle }
    }
}

pub fn convert_short_to_float(
    input_data: &mut [Complex<f32>],
    n_elems: usize,
    packet_ptr: *const i16,
) {
    let fft_ptr = input_data.as_mut_ptr() as *mut f32;

    unsafe {
        SimdConvertShortToFloat(
            packet_ptr as *const libc::c_void,
            fft_ptr as *mut libc::c_void,
            n_elems,
        );
    }
}

pub fn computefft(input_data: &mut [Complex<f32>], desc: &DFTI_DESCRIPTOR_HANDLE) {
    let input_data_ptr = input_data.as_mut_ptr();
    unsafe {
        DftiComputeForward(*desc, input_data_ptr as *mut libc::c_void);
    }
}

pub fn inout_shift(fft_data: &mut [Complex<f32>], ofdm_ca_num: usize) {
    // shift fft_inout to center DC frequency component
    let n_elems = 2 * ofdm_ca_num;
    let mut fft_shift_align: AlignedVec<Complex<f32>> = AlignedVec::new(n_elems, 64);
    let fft_shift = fft_shift_align.get_mut();

    // copy fft_inout to a shift buffer
    unsafe {
        memcpy(
            fft_shift.as_mut_ptr() as *mut std::ffi::c_void,
            fft_data.as_ptr() as *const std::ffi::c_void,
            ofdm_ca_num * std::mem::size_of::<i32>(),
        );

        // copy the second half of the shift buffer to the first half of fft_inout
        memcpy(
            fft_data.as_mut_ptr() as *mut std::ffi::c_void,
            fft_data.as_ptr().add(ofdm_ca_num / 2) as *const std::ffi::c_void,
            ofdm_ca_num * std::mem::size_of::<i32>(),
        );

        // copy the first half stored in shift buffer back to second half of fft_inout
        memcpy(
            fft_data.as_mut_ptr().add(ofdm_ca_num / 2) as *mut std::ffi::c_void,
            fft_shift.as_ptr() as *const std::ffi::c_void,
            ofdm_ca_num * std::mem::size_of::<i32>(),
        );
    }
}

#[repr(C, align(64))]
pub struct Fft {
    desc: *mut DFTI_DESCRIPTOR,
    prec: u32,
    domain: u32,
    dim: i64,
    sizes: i64,
    nelems: usize,
    pub fft_inout_align: AlignedVec<Complex<f32>>,
    pub fft_shift_align: AlignedVec<Complex<f32>>,
}
unsafe impl Send for Fft {}
unsafe impl Sync for Fft {}

impl Fft {
    pub fn new(ofdm_ca_num: usize) -> Self {
        unsafe {
            // allocate memory for aligned fft_inout
            let n_elems = 2 * ofdm_ca_num;
            let fft_align: AlignedVec<Complex<f32>> = AlignedVec::new(n_elems, 64);

            let fft_shift_align: AlignedVec<Complex<f32>> = AlignedVec::new(n_elems, 64);

            let fft_conf = FftConfig::new(ofdm_ca_num);

            let mut mkl_handle: DFTI_DESCRIPTOR_HANDLE = std::ptr::null_mut();
            DftiCreateDescriptor(
                &mut mkl_handle,
                fft_conf.dfti_single,
                fft_conf.dfti_complex,
                fft_conf.dim,
                fft_conf.fft_size,
            );

            // Commit the descriptor
            DftiCommitDescriptor(mkl_handle);

            Self {
                desc: mkl_handle,
                prec: fft_conf.dfti_single,
                domain: fft_conf.dfti_complex,
                dim: fft_conf.dim,
                sizes: fft_conf.fft_size,
                nelems: n_elems,
                fft_inout_align: fft_align,
                fft_shift_align: fft_shift_align,
            }
        }
    }

    /// convert -> forward FFT -> fftshift, exactly as convert_short_to_float +
    /// computefft + inout_shift, but into CALLER-provided scratch (`inout`,
    /// `shift`, each >= 2 * ofdm_ca_num elements, 64-B aligned) instead of the
    /// struct's own buffers. The committed DFTI descriptor is only read (MKL
    /// DFTI compute on one committed descriptor is thread-safe), so `&self`
    /// may be shared by concurrent tasks. Needed because a factored var such as
    /// `fft_struct` is ONE object per node index shared by every slot: two
    /// frames in flight could run the same fft instance concurrently and race
    /// on the struct's scratch.
    pub fn run_into(
        &self,
        packet_ptr: *const i16,
        inout: &mut [Complex<f32>],
        shift: &mut [Complex<f32>],
        ofdm_ca_num: usize,
    ) {
        unsafe {
            SimdConvertShortToFloat(
                packet_ptr as *const libc::c_void,
                inout.as_mut_ptr() as *mut libc::c_void,
                self.nelems,
            );
            DftiComputeForward(self.desc, inout.as_mut_ptr() as *mut libc::c_void);
            memcpy(
                shift.as_mut_ptr() as *mut std::ffi::c_void,
                inout.as_ptr() as *const std::ffi::c_void,
                ofdm_ca_num * std::mem::size_of::<i32>(),
            );
            memcpy(
                inout.as_mut_ptr() as *mut std::ffi::c_void,
                inout.as_ptr().add(ofdm_ca_num / 2) as *const std::ffi::c_void,
                ofdm_ca_num * std::mem::size_of::<i32>(),
            );
            memcpy(
                inout.as_mut_ptr().add(ofdm_ca_num / 2) as *mut std::ffi::c_void,
                shift.as_ptr() as *const std::ffi::c_void,
                ofdm_ca_num * std::mem::size_of::<i32>(),
            );
        }
    }

    pub fn convert_short_to_float(&mut self, packet_ptr: *const i16) {
        let fft_ptr = self.fft_inout_align.get_mut().as_mut_ptr() as *mut f32;

        unsafe {
            SimdConvertShortToFloat(
                packet_ptr as *const libc::c_void,
                fft_ptr as *mut libc::c_void,
                self.nelems,
            );
        }
    }

    pub fn computefft(&mut self) {
        let input_data = self.fft_inout_align.get_mut().as_mut_ptr();

        unsafe {
            DftiComputeForward(self.desc, input_data as *mut libc::c_void);
        }
    }

    pub fn inout_shift(&mut self, ofdm_ca_num: usize) {
        // shift fft_inout to center DC frequency component
        let fft_inout = self.fft_inout_align.get_mut();
        let fft_shift = self.fft_shift_align.get_mut();

        // copy fft_inout to a shift buffer
        unsafe {
            memcpy(
                fft_shift.as_mut_ptr() as *mut std::ffi::c_void,
                fft_inout.as_ptr() as *const std::ffi::c_void,
                ofdm_ca_num * std::mem::size_of::<i32>(),
            );

            // copy the second half of the shift buffer to the first half of fft_inout
            memcpy(
                fft_inout.as_mut_ptr() as *mut std::ffi::c_void,
                fft_inout.as_ptr().add(ofdm_ca_num / 2) as *const std::ffi::c_void,
                ofdm_ca_num * std::mem::size_of::<i32>(),
            );

            // copy the first half stored in shift buffer back to second half of fft_inout
            memcpy(
                fft_inout.as_mut_ptr().add(ofdm_ca_num / 2) as *mut std::ffi::c_void,
                fft_shift.as_ptr() as *const std::ffi::c_void,
                ofdm_ca_num * std::mem::size_of::<i32>(),
            );
        }
    }
}

thread_local! {
    /// Per-thread FFT scratch (inout, shift), see `Fft::run_into`.
    pub static TL_FFT_SCRATCH: std::cell::RefCell<(crate::common::structures::AlignedVec<num_complex::Complex<f32>>, crate::common::structures::AlignedVec<num_complex::Complex<f32>>, usize)> =
        std::cell::RefCell::new((
            crate::common::structures::AlignedVec::new(0, 64),
            crate::common::structures::AlignedVec::new(0, 64),
            0,
        ));
}

/// Run the FFT front half into this thread's scratch and hand the result to `f`.
pub fn with_fft_scratch<R>(
    fft: &Fft,
    packet_ptr: *const i16,
    ofdm_ca_num: usize,
    f: impl FnOnce(*const Complex<f32>) -> R,
) -> R {
    TL_FFT_SCRATCH.with(|tl| {
        let mut g = tl.borrow_mut();
        if g.2 != ofdm_ca_num {
            *g = (
                crate::common::structures::AlignedVec::new(2 * ofdm_ca_num, 64),
                crate::common::structures::AlignedVec::new(2 * ofdm_ca_num, 64),
                ofdm_ca_num,
            );
        }
        let (a, b, _) = &mut *g;
        fft.run_into(packet_ptr, a.get_mut(), b.get_mut(), ofdm_ca_num);
        f(a.get().as_ptr())
    })
}
