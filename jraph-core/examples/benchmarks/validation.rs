use crate::bindings::*;
use crate::functions::*;
use nalgebra::*;
use num_complex::Complex32;

fn find_index(idx: isize, mult_factor: usize) -> usize {
    if idx >= 0 {
        idx as usize
    } else {
        mult_factor - idx.abs() as usize
    }
}

pub fn validate1(mult_factor: usize, fft_size: usize) -> Vec<DMatrix<Complex32>> {
    let mut results: Vec<DMatrix<Complex32>> = Vec::new();
    let mut fft_buffers: Vec<Fft> = Vec::new();

    for _ in 0..mult_factor {
        fft_buffers.push(Fft::new(fft_size));
    }

    // fft computation in-place -> matrix -> cgemm
    for buf in fft_buffers.iter_mut() {
        buf.computefft();
        let vecmat = vec_to_mat(&buf.get_buf());
        let result = blas_cgemm(&vecmat, &vecmat);
        results.push(result);
    }
    results
}

pub fn validate2(mult_factor: usize, fft_size: usize) -> Vec<DMatrix<Complex32>> {
    let mut results: Vec<DMatrix<Complex32>> = Vec::new();
    let mut fft_buffers: Vec<Fft> = Vec::new();

    for _ in 0..mult_factor {
        fft_buffers.push(Fft::new(fft_size));
    }

    for buf in fft_buffers.iter_mut() {
        buf.computefft();
    }

    for i in 0..mult_factor {
        let buf1 = fft_buffers[i].get_buf();
        let vector1 = vec_to_mat(&buf1);

        let idx: isize = i as isize - 1;
        let buf2 = fft_buffers[find_index(idx, mult_factor)].get_buf();
        let vector2 = vec_to_mat(&buf2);

        let result = blas_cgemm(&vector1, &vector2);
        results.push(result);
    }
    results
}

pub fn validate3(mult_factor: usize, fft_size: usize) -> Vec<DMatrix<Complex32>> {
    let mut results: Vec<DMatrix<Complex32>> = Vec::new();
    let mut fft_buffers: Vec<Fft> = Vec::new();

    for _ in 0..mult_factor {
        fft_buffers.push(Fft::new(fft_size));
    }

    let mut vecmats: Vec<DMatrix<Complex32>> = Vec::new();
    for buf in fft_buffers.iter_mut() {
        buf.computefft();
        let vecmat = vec_to_mat(&buf.get_buf());
        vecmats.push(vecmat);
    }

    let refmats: Vec<&DMatrix<Complex32>> = vecmats.iter().collect();

    for _ in 0..mult_factor {
        let result = multiple_cgemm(refmats.clone());
        results.push(result);
    }
    results
}
