use crate::functions::*;

pub fn generate_vector_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let buf_size = match args[0] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type for packet length"),
    };
    generate_vector_cm(buf_size)
}

pub fn fft_planner_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let buf_size = match args[0] {
        CmTypes::Usize(x) => x.clone(),
        _ => panic!("Invalid argument type for packet length"),
    };
    fft_planner_cm(buf_size)
}

pub fn compute_fft_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let fft_planner = &args[0];
    let buffer = &args[1];

    compute_fft_cm(fft_planner, buffer);
    CmTypes::None
}

pub fn vec_to_mat_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let vector = &args[0];
    vec_to_mat_cm(vector)
}

pub fn mat_mul_cm_wrap(args: Vec<CmTypes>) -> CmTypes {
    mat_mul_cm(&args[0], &args[1])
}

use nalgebra::*;
use num_complex::Complex32;
use rustfft::Fft;
use std::sync::Arc;
use synstream_types::CmTypes;

// Functions that Return CmTypes and will be wrapped
pub fn generate_vector_cm(n: usize) -> CmTypes {
    let vector = generate_vector(n);

    CmTypes::from_any(vector)
}

pub fn fft_planner_cm(buf_size: usize) -> CmTypes {
    CmTypes::from_any(fft_planner(buf_size))
}

pub fn compute_fft_cm(fft_planner: &CmTypes, buffer: &CmTypes) {
    fft_planner
        .with_any(|fft_planner_ref: &Arc<dyn Fft<f32>>| {
            buffer
                .with_any_mut(|buffer_mut: &mut Vec<Complex32>| {
                    compute_fft(fft_planner_ref.clone(), buffer_mut);
                })
                .expect("Failed to access buffer struct or wrong type")
        })
        .expect("Failed to access fft_planner struct or wrong type")
}

pub fn vec_to_mat_cm(vector: &CmTypes) -> CmTypes {
    vector
        .with_any(|vector_ref: &Vec<Complex32>| CmTypes::from_any(vec_to_mat(vector_ref)))
        .expect("Failed to access vector or wrong type")
}

pub fn mat_mul_cm(a: &CmTypes, b: &CmTypes) -> CmTypes {
    a.with_any(|a_ref: &DMatrix<Complex32>| {
        b.with_any(|b_ref: &DMatrix<Complex32>| CmTypes::from_any(mat_mul(a_ref, b_ref)))
            .expect("Failed to access matrix b or wrong type")
    })
    .expect("Failed to access matrix a or wrong type")
}
