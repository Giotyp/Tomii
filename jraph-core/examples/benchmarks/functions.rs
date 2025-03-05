use nalgebra::*;
use num_complex::Complex32;
extern crate intel_mkl_src;

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

pub fn blas_cgemm(a: &DMatrix<Complex32>, b: &DMatrix<Complex32>) -> DMatrix<Complex32> {
    let m = a.nrows();
    let n = b.ncols();
    let k = a.ncols();

    let a_slice: &[Complex32] = a.as_slice();
    let b_slice: &[Complex32] = b.as_slice();

    let mut c = DMatrix::<Complex32>::zeros(m, n);
    let mut c_slice: &mut [Complex32] = c.as_mut_slice();

    let alpha = Complex32::new(1.0, 0.0);
    let beta = Complex32::new(0.0, 0.0);

    unsafe {
        cblas::cgemm(
            cblas::Layout::ColumnMajor,
            cblas::Transpose::None,
            cblas::Transpose::None,
            m as i32,
            n as i32,
            k as i32,
            alpha,
            a_slice,
            m as i32,
            b_slice,
            k as i32,
            beta,
            &mut c_slice,
            m as i32,
        );
    }
    c
}

pub fn multiple_cgemm(vectors: Vec<&DMatrix<Complex32>>) -> DMatrix<Complex32> {
    let mut c_res = Vec::new();

    let alpha = Complex32::new(1.0, 0.0);
    let beta = Complex32::new(0.0, 0.0);

    // first matrix
    let a = vectors[0];
    let b = vectors[1];

    let m = a.nrows();
    let n = b.ncols();
    let k = a.ncols();

    let a_slice: &[Complex32] = a.as_slice();
    let b_slice: &[Complex32] = b.as_slice();

    let mut c0 = DMatrix::<Complex32>::zeros(m, n);
    let mut c0_slice: &mut [Complex32] = c0.as_mut_slice();

    unsafe {
        cblas::cgemm(
            cblas::Layout::ColumnMajor,
            cblas::Transpose::None,
            cblas::Transpose::None,
            m as i32,
            n as i32,
            k as i32,
            alpha,
            a_slice,
            m as i32,
            b_slice,
            k as i32,
            beta,
            &mut c0_slice,
            m as i32,
        );
    }

    c_res.push(c0);

    for i in 1..vectors.len() {
        let a = vectors[i];
        let b = vectors[i];
        let mut c_prev = c_res[i - 1].clone();

        let m = a.nrows();
        let n = b.ncols();
        let k = a.ncols();

        let a_slice: &[Complex32] = a.as_slice();
        let b_slice: &[Complex32] = b.as_slice();

        let mut c_slice: &mut [Complex32] = c_prev.as_mut_slice();

        unsafe {
            cblas::cgemm(
                cblas::Layout::ColumnMajor,
                cblas::Transpose::None,
                cblas::Transpose::None,
                m as i32,
                n as i32,
                k as i32,
                alpha,
                a_slice,
                m as i32,
                b_slice,
                k as i32,
                beta,
                &mut c_slice,
                m as i32,
            );
        }
        c_res.push(c_prev.clone());
    }
    c_res[c_res.len() - 1].clone()
}
