use crate::functions::*;
use nalgebra::*;
use num_complex::Complex32;
use shared::CmTypes;
use std::sync::Arc;

pub fn vec_to_mat_wrap(args: Vec<CmTypes>) -> CmTypes {
    let x: &Vec<Complex32> = match &args[0] {
        CmTypes::VecC32(v) => v.as_ref(),
        _ => panic!("Invalid argument type"),
    };

    CmTypes::DMatrixC32(Arc::new(vec_to_mat(x)))
}

pub fn blas_cgemm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let a: &DMatrix<Complex32> = match &args[0] {
        CmTypes::DMatrixC32(a) => a,
        _ => panic!("Invalid argument type"),
    };

    let b: &DMatrix<Complex32> = match &args[1] {
        CmTypes::DMatrixC32(b) => b,
        _ => panic!("Invalid argument type"),
    };

    CmTypes::DMatrixC32(Arc::new(blas_cgemm(a, b)))
}

pub fn multiple_cgemm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let mut vectors: Vec<&DMatrix<Complex32>> = Vec::new();
    for i in 0..args.len() {
        let x = match &args[i] {
            CmTypes::DMatrixC32(x) => x,
            _ => panic!("Invalid argument type"),
        };
        vectors.push(x);
    }

    // let vectors: Vec<&DMatrix<Complex32>> = args
    //     .iter()
    //     .map(|arg| match arg {
    //         CmTypes::DMatrixC32(m) => m.as_ref(),
    //         _ => panic!("Invalid argument type"),
    //     })
    //     .collect();

    CmTypes::DMatrixC32(Arc::new(multiple_cgemm(vectors)))
}
