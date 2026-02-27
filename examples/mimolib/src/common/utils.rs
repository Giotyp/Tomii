#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
use crate::common::structures::MultiVector;
use num_complex::{Complex, Complex32};
use std::ops::{Add, BitAnd, Not, Sub};

use lapack::*;
use nalgebra::*;
extern crate intel_mkl_src;

pub fn DoubleToCFloat(input: &MultiVector<f64>) -> Vec<Complex<f32>> {
    let size = input.length();
    let mut output: Vec<Complex<f32>> = Vec::new();

    for i in 0..size {
        output.push(Complex::new(input[0][i] as f32, input[1][i] as f32));
    }
    output
}

pub fn roundup<T>(x: T, round_num: T) -> T
where
    T: Add<Output = T>
        + BitAnd<Output = T>
        + Sub<Output = T>
        + Not<Output = T>
        + Copy
        + From<u8>
        + PartialEq,
{
    // assert if round_num is power of two
    assert!(power_of_two::<T>(round_num));
    (x + round_num - T::from(1)) & !(round_num - T::from(1))
}

pub fn power_of_two<T>(x: T) -> bool
where
    T: Sub<Output = T> + BitAnd<Output = T> + PartialEq + Copy + From<u8>,
{
    // e.g. for x = 4 = 100, x - 1 = 3 = 011,
    // x & (x - 1) = 0
    let x2 = x & (x - T::from(1));
    let x3 = x2 == 0.into();
    // For x = 0 , result should be false
    x3 && (x != 0.into())
}
#[inline(always)]
pub fn invert_matrix(matrix: &DMatrix<Complex<f32>>) -> Option<DMatrix<Complex<f32>>> {
    let n = matrix.nrows() as i32;
    let mut a = matrix.as_slice().to_vec(); // Directly get the column-major order slice
    let mut ipiv = vec![0; n as usize];
    let mut info = 0;

    unsafe {
        // LU decomposition
        cgetrf(n, n, &mut a, n, &mut ipiv, &mut info);
        if info != 0 {
            return None;
        }

        // Query the optimal workspace size
        let mut lwork: i32 = -1;
        let mut work = vec![Complex::new(0.0, 0.0)];
        cgetri(n, &mut a, n, &ipiv, &mut work, lwork, &mut info);
        if info != 0 {
            return None;
        }

        // Perform the actual inversion with optimal workspace
        lwork = work[0].re as i32;
        let mut work = vec![Complex::new(0.0, 0.0); lwork as usize];
        cgetri(n, &mut a, n, &ipiv, &mut work, lwork, &mut info);
        if info != 0 {
            return None;
        }
    }

    // Directly create the result matrix from the column-major order data
    let result = DMatrix::from_column_slice(matrix.nrows(), matrix.ncols(), &a);

    Some(result)
}
#[inline(always)]
pub fn blas_cgemm_new(
    a: &DMatrix<Complex<f32>>,
    b: &DMatrix<Complex<f32>>,
) -> DMatrix<Complex<f32>> {
    let m = a.nrows();
    let n = b.ncols();
    let k = a.ncols();

    let a_slice: &[Complex<f32>] = a.as_slice();
    let b_slice: &[Complex<f32>] = b.as_slice();

    let mut c = DMatrix::<Complex32>::zeros(m, n);
    let mut c_slice: &mut [Complex<f32>] = c.as_mut_slice();

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
#[inline(always)]
pub fn blas_cgemm_dmat(
    a: &DMatrix<Complex<f32>>,
    b: &DMatrix<Complex<f32>>,
    c: &mut [Complex<f32>],
) {
    let m = a.nrows();
    let n = b.ncols();
    let k = a.ncols();

    let a_slice: &[Complex<f32>] = a.as_slice();
    let b_slice: &[Complex<f32>] = b.as_slice();

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
            c,
            m as i32,
        );
    }
}

#[inline(always)]
pub fn conjugate_transpose(matrix: &DMatrix<Complex32>) -> DMatrix<Complex32> {
    let mut result = matrix.transpose();
    result.apply(|x| *x = x.conj());
    result
}
