use nalgebra::*;
use num_complex::Complex32;
use rustfft::{Fft, FftPlanner};
use std::sync::Arc;
/// Generates a vector of complex float numbers
/// in the form [(1.0, 1.0), (2.0, 2.0), ..., (n.0, n.0)]
pub fn generate_vector(n: usize) -> Vec<Complex32> {
    let mut data = Vec::with_capacity(n);
    for i in 1..(n + 1) {
        data.push(Complex32::new(i as f32, i as f32));
    }
    data
}

/// Creates and returns an FFT planner for the specified buffer size
pub fn fft_planner(buf_size: usize) -> Arc<dyn Fft<f32>> {
    let mut planner: FftPlanner<f32> = FftPlanner::new();
    planner.plan_fft_forward(buf_size)
}

/// Computes fft in-place on the provided buffer using the given planner
pub fn compute_fft(fft: Arc<dyn Fft<f32>>, buffer: &mut [Complex32]) {
    fft.process(buffer);
}

/// Converts vector to matrix (size must be a perfect square)
pub fn vec_to_mat(vector: &Vec<Complex32>) -> DMatrix<Complex32> {
    let len = vector.len();
    let n = (len as f64).sqrt() as usize;

    // Check if len is a perfect square
    if n * n == len {
        DMatrix::from_vec(n, n, vector.to_vec())
    } else {
        panic!("Length of vector is not a perfect square")
    }
}

/// Performs matrix multiplication
pub fn mat_mul(a: &DMatrix<Complex32>, b: &DMatrix<Complex32>) -> DMatrix<Complex32> {
    a * b
}
