use nalgebra::*;
use num_complex::Complex32;
use rustfft::{Fft, FftPlanner};
use std::cell::RefCell;
use std::sync::Arc;
use synstream_types::CmTypes;
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

/// Performs matrix multiplication between multiple matrices
pub fn mat_mul(vectors: &Vec<CmTypes>) -> DMatrix<Complex32> {
    let c_res = RefCell::new(DMatrix::from_element(0, 0, Complex32::new(0.0, 0.0)));
    let n = vectors.len();

    // first matrix multiplication: vectors[0] * vectors[n-1]
    vectors[0]
        .with_any(|a: &DMatrix<Complex32>| {
            vectors[n - 1]
                .with_any(|b: &DMatrix<Complex32>| {
                    *c_res.borrow_mut() = a * b;
                })
                .expect("Failed to access matrix b or wrong type")
        })
        .expect("Failed to access matrix a or wrong type");

    for i in 1..n / 2 {
        vectors[i]
            .with_any(|a: &DMatrix<Complex32>| {
                vectors[n - i - 1]
                    .with_any(|b: &DMatrix<Complex32>| {
                        let current = c_res.borrow().clone();
                        *c_res.borrow_mut() = a * b + current;
                    })
                    .expect("Failed to access matrix b or wrong type")
            })
            .expect("Failed to access matrix a or wrong type");
    }
    c_res.into_inner()
}

pub fn mat_mul_dm(vectors: &Vec<DMatrix<Complex32>>) -> DMatrix<Complex32> {
    let n = vectors.len();

    // first matrix multiplication: vectors[0] * vectors[n-1]
    let mut c_res = &vectors[0] * &vectors[n - 1];

    for i in 1..n / 2 {
        let current = c_res.clone();
        c_res = &vectors[i] * &vectors[n - i - 1] + current;
    }
    c_res
}
