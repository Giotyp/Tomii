#[allow(non_upper_case_globals)]
#[allow(dead_code)]

use num_complex::Complex32;

const DFTI_CONFIG_VALUE_DFTI_SINGLE: DFTI_CONFIG_VALUE = 35;
const DFTI_CONFIG_VALUE_DFTI_COMPLEX: DFTI_CONFIG_VALUE = 32;
#[allow(non_camel_case_types)]
type DFTI_CONFIG_VALUE = ::std::os::raw::c_uint;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
struct DFTI_DESCRIPTOR {
    _unused: [u8; 0],
}
#[allow(non_camel_case_types)]
type DFTI_DESCRIPTOR_HANDLE = *mut DFTI_DESCRIPTOR;

extern "C" {
    fn DftiCreateDescriptor(
        arg1: *mut DFTI_DESCRIPTOR_HANDLE,
        arg2: DFTI_CONFIG_VALUE,
        arg3: DFTI_CONFIG_VALUE,
        arg4: ::std::os::raw::c_long,
        ...
    ) -> ::std::os::raw::c_long;
}

extern "C" {
    fn DftiCommitDescriptor(arg1: DFTI_DESCRIPTOR_HANDLE) -> ::std::os::raw::c_long;
}

extern "C" {
    fn DftiComputeForward(
        arg1: DFTI_DESCRIPTOR_HANDLE,
        arg2: *mut ::std::os::raw::c_void,
        ...
    ) -> ::std::os::raw::c_long;
}

fn generate_set_complex_float_array(n: usize) -> Vec<Complex32> {
    let mut data = Vec::with_capacity(n);
    for i in 1..(n+1) {
        data.push(Complex32::new(i as f32, i as f32));
    }
    data
}

struct FftConfig {
    dfti_single: DFTI_CONFIG_VALUE,
    dfti_complex: DFTI_CONFIG_VALUE,
    dim: i64,
    fft_size: i64,
}

impl FftConfig {
    fn new(fft_size: usize) -> Self {
        let dim: i64 = 1;
        let fft_size = fft_size as i64;
        Self {
            dfti_single: DFTI_CONFIG_VALUE_DFTI_SINGLE,
            dfti_complex: DFTI_CONFIG_VALUE_DFTI_COMPLEX,
            dim,
            fft_size: fft_size,
        }
    }
}

pub struct Fft {
    desc: *mut DFTI_DESCRIPTOR,
    fft_buf: Vec<Complex32>,
}
unsafe impl Send for Fft {}
unsafe impl Sync for Fft {}

impl Fft {
    pub fn new(fft_size: usize) -> Self {
        unsafe {
            // allocate memory for aligned fft_inout

            let fft_conf = FftConfig::new(fft_size);

            let fft_buf = generate_set_complex_float_array(fft_size);

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
                fft_buf: fft_buf,
            }
        }
    }

    pub fn get_buf(&self) -> Vec<Complex32> {
        self.fft_buf.clone()
    }

    pub fn computefft(&mut self) {
        let input_data = self.fft_buf.as_mut_ptr();

        unsafe {
            DftiComputeForward(self.desc, input_data as *mut libc::c_void);
        }
    }
}


pub fn create_buffer(size: usize) -> Vec<usize> {
    let mut buffer = Vec::new();
    for i in 0..size {
        buffer.push(i);
    }
    buffer
}