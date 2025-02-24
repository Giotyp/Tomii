use nalgebra::*;
use num_complex::Complex32;
use std::sync::Arc;

pub fn vec_to_mat_wrap(args: Vec<CmTypes>) -> CmTypes {
    let x: Arc<DVector<Complex32>> = match &args[0] {
        CmTypes::DVectorC32(v) => Arc::clone(v),
        _ => panic!("Invalid argument type"),
    };
    
    CmTypes::DMatrixC32(Arc::new(vec_to_mat(&x)))
}

pub fn blas_cgemm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let a: Arc<DMatrix<Complex32>> = match &args[0] {
        CmTypes::DMatrixC32(a) => Arc::clone(a),
        _ => panic!("Invalid argument type"),
    };

    let b: Arc<DMatrix<Complex32>> = match &args[1] {
        CmTypes::DMatrixC32(b) => Arc::clone(b),
        _ => panic!("Invalid argument type"),
    };

    CmTypes::DMatrixC32(Arc::new(blas_cgemm(&a, &b)))
}

pub fn multiple_cgemm_wrap(args: Vec<CmTypes>) -> CmTypes {
    let vectors: Vec<Arc<DMatrix<Complex32>>> = args.iter().map(|arg| {
        match arg {
            CmTypes::DMatrixC32(m) => Arc::clone(m),
            _ => panic!("Invalid argument type"),
        }
    }).collect();

    CmTypes::DMatrixC32(Arc::new(multiple_cgemm(&vectors)))
}