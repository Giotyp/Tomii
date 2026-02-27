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
