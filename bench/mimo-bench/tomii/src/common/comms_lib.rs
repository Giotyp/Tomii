use crate::common::comms_constants::kPrimeArray;
use crate::common::structures::MultiVector;
use crate::common::symbols::MaxModType;

use num_complex::Complex;
use std::f64::consts::PI;

pub fn get_sequence(seq_len: usize) -> MultiVector<f64> {
    // Currently returns sequence for kLteZadoffChu seq_type

    let mut matrix: MultiVector<f64> = MultiVector::new(2);

    let u = 1;
    let v = 0;
    let mut m: f64 = kPrimeArray[308] as f64;

    for j in 0..308 {
        if kPrimeArray[j] < seq_len && kPrimeArray[j + 1] > seq_len {
            m = kPrimeArray[j] as f64;
            break;
        }
    }

    let qh = m as f64 * (u + 1) as f64 / 31.0;
    let q = (qh + 0.5).floor() + v as f64 * (-1.0f64).powf(2.0 * qh.floor());

    for i in 0..seq_len {
        let m_loop = (i as f64) % m;
        let a_re = (-PI * q * m_loop as f64 * (m_loop + 1.0) / m).cos();
        let a_im = (-PI * q * m_loop as f64 * (m_loop + 1.0) / m).sin();

        matrix[0].push(a_re);
        matrix[1].push(a_im);
    }
    matrix
}

pub fn seq_cyclic_shift(input: Vec<Complex<f32>>, alpha: f32) -> Vec<Complex<f32>> {
    let size = input.len();
    let mut output: Vec<Complex<f32>> = Vec::new();

    for i in 0..size {
        let shift = Complex::new(0.0, i as f32 * alpha).exp();
        output.push(input[i] * shift);
    }
    output
}

static MCS: [(usize, usize); 32] = [
    (2, 120),
    (2, 157),
    (2, 193),
    (2, 251),
    (2, 308),
    (2, 379),
    (2, 449),
    (2, 526),
    (2, 602),
    (2, 679),
    (4, 340),
    (4, 378),
    (4, 434),
    (4, 490),
    (4, 553),
    (4, 616),
    (4, 658),
    (6, 438),
    (6, 466),
    (6, 517),
    (6, 567),
    (6, 616),
    (6, 666),
    (6, 719),
    (6, 772),
    (6, 822),
    (6, 873),
    (6, 910),
    (6, 948),
    (8, 754),
    (8, 797),
    (8, 841),
];

pub fn get_code_rate(mcs_index: usize) -> usize {
    MCS[mcs_index].1
}

pub fn get_mod_order_bits(mcs_index: usize) -> usize {
    MCS[mcs_index].0
}

fn get_available_mcs() -> Vec<Vec<usize>> {
    let mut available_mcs: Vec<Vec<usize>> = vec![Vec::new(); 1 + MaxModType / 2];

    for i in 0..MCS.len() {
        let mod_order_bits = get_mod_order_bits(i);
        available_mcs[mod_order_bits / 2].push(get_code_rate(i));
    }

    available_mcs
}

pub fn get_mcs_index(in_mod_order: usize, in_code_rate: usize) -> usize {
    let mcs_vec = get_available_mcs();
    let mut mcs_index = 0;

    for i in 0..(in_mod_order / 2) {
        mcs_index += mcs_vec[i].len();
    }

    let code_vec = &mcs_vec[in_mod_order / 2];

    if code_vec.is_empty() {
        panic!("Input vector is empty.");
    }

    if in_code_rate <= code_vec[0] {
        return mcs_index;
    }

    if in_code_rate >= code_vec[code_vec.len() - 1] {
        return mcs_index + code_vec.len() - 1;
    }

    match code_vec.binary_search(&in_code_rate) {
        Ok(index) => mcs_index + index,
        Err(lower_bound_index) => {
            let prev_index = lower_bound_index - 1;

            // Calculate the closest element
            let closest_element_index = if in_code_rate - code_vec[prev_index]
                <= code_vec[lower_bound_index] - in_code_rate
            {
                prev_index
            } else {
                lower_bound_index
            };

            mcs_index + closest_element_index
        }
    }
}
