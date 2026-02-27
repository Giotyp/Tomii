// use std::arch::x86_64::*;
// use num_integer::Roots;

// const SCALE_BYTE_CONV_QAM16: f32 = 100.0;
//const SCALE_BYTE_CONV_QAM64: f32 = 100.0;

pub fn map_mod_to_str(mod_order: usize) -> &'static str {
    match mod_order {
        1 => "BPSK",
        2 => "QPSK",
        4 => "16QAM",
        6 => "64QAM",
        8 => "256QAM",
        10 => "1024QAM",
        _ => "UNKNOWN!",
    }
}

pub fn map_mod_to_usize(mod_order: &str) -> usize {
    match mod_order {
        "BPSK" => 1,
        "QPSK" => 2,
        "16QAM" => 4,
        "64QAM" => 6,
        "256QAM" => 8,
        "1024QAM" => 10,
        _ => 0,
    }
}

// fn Demod16qamSoftSse(vec_in: *const f32, llr: *mut i8, num: usize){

//     let mut symbols_ptr = vec_in;
//     let mut result_ptr = llr as *mut __m128i;

//     let mut symbol1: __m128;
//     let mut symbol2: __m128;
//     let mut symbol3: __m128;
//     let mut symbol4: __m128;

//     let mut symbol_i1: __m128i;
//     let mut symbol_i2: __m128i;
//     let mut symbol_i3: __m128i;
//     let mut symbol_i4: __m128i;

//     let mut symbol_i: __m128i;
//     let mut symbol_abs: __m128i;
//     let mut symbol_12: __m128i;
//     let mut symbol_34: __m128i;

//     let offset: __m128i = unsafe {_mm_set1_epi8( 2 * (SCALE_BYTE_CONV_QAM16 as i8/ 10_i8.sqrt()))};

//     let mut result1n: __m128i;
//     let mut result1a: __m128i;
//     let mut result2n: __m128i;
//     let mut result2a: __m128i;

//     let scale_v: __m128 = unsafe {_mm_set1_ps(SCALE_BYTE_CONV_QAM16)};

//     unsafe {
//         let ff = 0xffu8 as i8;

//         let shuffle_negated_1: __m128i = _mm_set_epi8(
//             ff, ff, 7, 6, ff, ff, 5, 4, ff, ff, 3, 2, ff, ff, 1, 0);
//         let shuffle_abs_1: __m128i = _mm_set_epi8(
//             7, 6, ff, ff, 5, 4, ff, ff, 3, 2, ff, ff, 1, 0, ff, ff);

//         let shuffle_negated_2: __m128i = _mm_set_epi8(
//             ff, ff, 15, 14, ff, ff, 13, 12, ff, ff, 11, 10,ff, ff, 9, 8);
//         let shuffle_abs_2: __m128i = _mm_set_epi8(
//             15, 14, ff, ff, 13, 12, ff, ff, 11, 10, ff, ff, 9, 8, ff, ff);

//         for _ in 0..(num/8) {
//             symbol1 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol2 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol3 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol4 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol_i1 = _mm_cvtps_epi32(_mm_mul_ps(symbol1, scale_v));
//             symbol_i2 = _mm_cvtps_epi32(_mm_mul_ps(symbol2, scale_v));
//             symbol_i3 = _mm_cvtps_epi32(_mm_mul_ps(symbol3, scale_v));
//             symbol_i4 = _mm_cvtps_epi32(_mm_mul_ps(symbol4, scale_v));
//             symbol_12 = _mm_packs_epi32(symbol_i1, symbol_i2);
//             symbol_34 = _mm_packs_epi32(symbol_i3, symbol_i4);
//             symbol_i = _mm_packs_epi16(symbol_12, symbol_34);

//             symbol_abs = _mm_abs_epi8(symbol_i);
//             symbol_abs = _mm_sub_epi8(offset, symbol_abs);

//             result1n = _mm_shuffle_epi8(symbol_i, shuffle_negated_1);
//             result1a = _mm_shuffle_epi8(symbol_abs, shuffle_abs_1);

//             result2n = _mm_shuffle_epi8(symbol_i, shuffle_negated_2);
//             result2a = _mm_shuffle_epi8(symbol_abs, shuffle_abs_2);

//             _mm_store_si128(result_ptr, _mm_or_si128(result1n, result1a));
//             result_ptr = result_ptr.add(1);
//             _mm_store_si128(result_ptr, _mm_or_si128(result2n, result2a));
//             result_ptr = result_ptr.add(1);
//         }
//     }
//     // Demodulate last symbols
//     let vec_slice = unsafe {std::slice::from_raw_parts(vec_in, 2 * num)};
//     let llr_slice = unsafe {std::slice::from_raw_parts_mut(llr, 4 * num)};
//     for i in (8 * (num/8))..num {
//         let yre = (SCALE_BYTE_CONV_QAM16 * vec_slice[2 * i]) as i8;
//         let yim = (SCALE_BYTE_CONV_QAM16 * vec_slice[2 * i + 1]) as i8;

//         llr_slice[4 * i + 0] = yre;
//         llr_slice[4 * i + 1] = yim;
//         llr_slice[4 * i + 2] = 2 * (SCALE_BYTE_CONV_QAM16 as i8 / 10_i8.sqrt()) - yre.abs();
//         llr_slice[4 * i + 3] = 2 * (SCALE_BYTE_CONV_QAM16 as i8 / 10_i8.sqrt()) - yim.abs();
//     }
// }

// fn Demod64qamSoftSse(vec_in: *const f32, llr: *mut i8, num: usize) {
//     let mut symbols_ptr = vec_in;
//     let mut result_ptr = llr as *mut __m128i;

//     let mut symbol1: __m128;
//     let mut symbol2: __m128;
//     let mut symbol3: __m128;
//     let mut symbol4: __m128;

//     let mut symbol_i1: __m128i;
//     let mut symbol_i2: __m128i;
//     let mut symbol_i3: __m128i;
//     let mut symbol_i4: __m128i;

//     let mut symbol_i: __m128i;
//     let mut symbol_abs: __m128i;
//     let mut symbol_abs2: __m128i;
//     let mut symbol_12: __m128i;
//     let mut symbol_34: __m128i;

//     let offset1: __m128i = unsafe {_mm_set1_epi8( 4 * (SCALE_BYTE_CONV_QAM64 as i8 / 42_i8.sqrt()))};
//     let offset2: __m128i = unsafe {_mm_set1_epi8( 2 * (SCALE_BYTE_CONV_QAM64 as i8 / 42_i8.sqrt()))};

//     let mut result11: __m128i;
//     let mut result12: __m128i;
//     let mut result13: __m128i;
//     let mut result21: __m128i;
//     let mut result22: __m128i;
//     let mut result23: __m128i;
//     let mut result31: __m128i;
//     let mut result32: __m128i;
//     let mut result33: __m128i;

//     let scale_v: __m128 = unsafe {_mm_set1_ps(SCALE_BYTE_CONV_QAM64)};

//     unsafe {
//         let ff = 0xffu8 as i8;

//         let shuffle_negated_1: __m128i = _mm_set_epi8(
//             ff, ff, 5, 4, ff, ff, ff, ff, 3, 2, ff, ff, ff, ff, 1, 0);
//         let shuffle_negated_2: __m128i = _mm_set_epi8(
//             11, 10, ff, ff, ff, ff, 9, 8, ff, ff, ff, ff, 7, 6, ff, ff);
//         let shuffle_negated_3: __m128i = _mm_set_epi8(
//             ff, ff, ff, ff, 15, 14, ff, ff, ff, ff, 13, 12, ff, ff, ff, ff);

//         let shuffle_abs_1: __m128i = _mm_set_epi8(
//             5, 4, ff, ff, ff, ff, 3, 2,ff, ff, ff, ff, 1, 0, ff, ff);
//         let shuffle_abs_2: __m128i = _mm_set_epi8(
//             ff, ff, ff, ff, 9, 8, ff, ff, ff, ff, 7, 6, ff, ff, ff, ff);
//         let shuffle_abs_3: __m128i = _mm_set_epi8(
//             ff, ff, 15, 14, ff, ff, ff, ff, 13, 12, ff, ff, ff, ff, 11, 10);

//         let shuffle_abs2_1: __m128i = _mm_set_epi8(
//             ff, ff, ff, ff, 3, 2, ff, ff, ff, ff, 1, 0, ff, ff, ff, ff);
//         let shuffle_abs2_2: __m128i = _mm_set_epi8(
//             ff, ff, 9, 8, ff, ff, ff, ff, 7, 6, ff, ff, ff, ff, 5, 4);
//         let shuffle_abs2_3: __m128i = _mm_set_epi8(
//             15, 14, ff, ff, ff, ff, 13, 12, ff, ff, ff, ff, 11, 10, ff, ff);

//         for _ in 0..(num/8) {
//             symbol1 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol2 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol3 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol4 = _mm_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(4);
//             symbol_i1 = _mm_cvtps_epi32(_mm_mul_ps(symbol1, scale_v));
//             symbol_i2 = _mm_cvtps_epi32(_mm_mul_ps(symbol2, scale_v));
//             symbol_i3 = _mm_cvtps_epi32(_mm_mul_ps(symbol3, scale_v));
//             symbol_i4 = _mm_cvtps_epi32(_mm_mul_ps(symbol4, scale_v));
//             symbol_12 = _mm_packs_epi32(symbol_i1, symbol_i2);
//             symbol_34 = _mm_packs_epi32(symbol_i3, symbol_i4);
//             symbol_i = _mm_packs_epi16(symbol_12, symbol_34);

//             symbol_abs = _mm_abs_epi8(symbol_i);
//             symbol_abs = _mm_sub_epi8(offset1, symbol_abs);
//             symbol_abs2 = _mm_sub_epi8(offset2, _mm_abs_epi8(symbol_abs));

//             result11 = _mm_shuffle_epi8(symbol_i, shuffle_negated_1);
//             result12 = _mm_shuffle_epi8(symbol_abs, shuffle_abs_1);
//             result13 = _mm_shuffle_epi8(symbol_abs2, shuffle_abs2_1);

//             result21 = _mm_shuffle_epi8(symbol_i, shuffle_negated_2);
//             result22 = _mm_shuffle_epi8(symbol_abs, shuffle_abs_2);
//             result23 = _mm_shuffle_epi8(symbol_abs2, shuffle_abs2_2);

//             result31 = _mm_shuffle_epi8(symbol_i, shuffle_negated_3);
//             result32 = _mm_shuffle_epi8(symbol_abs, shuffle_abs_3);
//             result33 = _mm_shuffle_epi8(symbol_abs2, shuffle_abs2_3);

//             _mm_store_si128(result_ptr,
//                             _mm_or_si128(_mm_or_si128(result11, result12), result13));
//             result_ptr = result_ptr.add(1);
//             _mm_store_si128(result_ptr,
//                             _mm_or_si128(_mm_or_si128(result21, result22), result23));
//             result_ptr = result_ptr.add(1);
//             _mm_store_si128(result_ptr,
//                             _mm_or_si128(_mm_or_si128(result31, result32), result33));
//             result_ptr = result_ptr.add(1);
//         }

//         let sq42 = 42_i8.sqrt();
//         let val4 =  4 * (SCALE_BYTE_CONV_QAM64 as i8 / sq42);
//         let val2 = 2 * (SCALE_BYTE_CONV_QAM64 as i8 / sq42);

//         // Demodulate last symbols
//         for i in (8 * (num/8))..num {
//             let v1 = vec_in.add(2 * i);
//             let v2 = vec_in.add(2*i + 1);
//             let yre = (SCALE_BYTE_CONV_QAM64 * std::ptr::read(v1)) as i8;
//             let yim = (SCALE_BYTE_CONV_QAM64 * std::ptr::read(v2)) as i8;

//             let mut llr_ptr = llr.add(6*i); // 6*i + 0
//             std::ptr::write(llr_ptr, yre);

//             llr_ptr = llr_ptr.add(1); // 6*i + 1
//             std::ptr::write(llr_ptr, yim);

//             llr_ptr = llr_ptr.add(1); // 6*i + 2
//             std::ptr::write(llr_ptr, val4 - yre.abs());
//             let llr_2 = std::ptr::read(llr_ptr);

//             llr_ptr = llr_ptr.add(1); // 6*i + 3
//             std::ptr::write(llr_ptr, val4 - yim.abs());
//             let llr_3 = std::ptr::read(llr_ptr);

//             llr_ptr = llr_ptr.add(1); // 6*i + 4
//             std::ptr::write(llr_ptr, val2 - llr_2.abs());

//             llr_ptr = llr_ptr.add(1); // 6*i + 5
//             std::ptr::write(llr_ptr, val2 - llr_3.abs());
//         }
//     }
// }

// fn Demod16qamSoftAvx2(vec_in: *const f32, llr: *mut i8, num: usize) {
//     let mut symbols_ptr = vec_in;
//     let mut result_ptr = llr as *mut __m256i;

//     let mut symbol1: __m256;
//     let mut symbol2: __m256;
//     let mut symbol3: __m256;
//     let mut symbol4: __m256;

//     let mut symbol_i1: __m256i;
//     let mut symbol_i2: __m256i;
//     let mut symbol_i3: __m256i;
//     let mut symbol_i4: __m256i;

//     let mut symbol_i: __m256i;
//     let mut symbol_abs: __m256i;
//     let mut symbol_12: __m256i;
//     let mut symbol_34: __m256i;

//     let offset: __m256i = unsafe {_mm256_set1_epi8( 2 * (SCALE_BYTE_CONV_QAM16 as i8/ 10_i8.sqrt()))};

//     let mut result1n: __m256i;
//     let mut result1a: __m256i;
//     let mut result1na: __m256i;
//     let mut result2n: __m256i;
//     let mut result2a: __m256i;
//     let mut result2na: __m256i;

//     let scale_v: __m256 = unsafe {_mm256_set1_ps(SCALE_BYTE_CONV_QAM16)};

//     unsafe {
//         let ff = 0xffu8 as i8;

//         let shuffle_negated_1: __m256i = _mm256_set_epi8(
//             ff, ff, 7, 6, ff, ff, 5, 4, ff, ff, 3, 2, ff, ff, 1, 0,
//             ff, ff, 7, 6, ff, ff, 5, 4, ff, ff, 3, 2, ff, ff, 1, 0);

//         let shuffle_abs_1: __m256i = _mm256_set_epi8(
//             7, 6, ff, ff, 5, 4, ff, ff, 3, 2, ff, ff, 1, 0, ff, ff, 7,
//             6, ff, ff, 5, 4, ff, ff, 3, 2, ff, ff, 1, 0, ff, ff);

//         let shuffle_negated_2: __m256i = _mm256_set_epi8(
//             ff, ff, 15, 14, ff, ff, 13, 12, ff, ff, 11,10, ff, ff, 9, 8,
//             ff, ff, 15, 14, ff, ff, 13, 12, ff, ff, 11, 10, ff, ff, 9, 8);

//         let shuffle_abs_2: __m256i = _mm256_set_epi8(
//             15, 14, ff, ff, 13, 12, ff, ff, 11, 10, ff, ff, 9, 8, ff, ff,
//             15, 14, ff, ff, 13, 12, ff, ff, 11, 10, ff, ff, 9, 8, ff, ff);

//         for _ in 0..(num/16) {
//             symbol1 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             symbol2 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             symbol3 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             symbol4 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);

//             symbol_i1 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol1, scale_v));
//             symbol_i2 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol2, scale_v));
//             symbol_i3 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol3, scale_v));
//             symbol_i4 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol4, scale_v));
//             symbol_12 = _mm256_packs_epi32(symbol_i1, symbol_i2);
//             symbol_12 = _mm256_permute4x64_epi64(symbol_12, 0xd8);
//             symbol_34 = _mm256_packs_epi32(symbol_i3, symbol_i4);
//             symbol_34 = _mm256_permute4x64_epi64(symbol_34, 0xd8);
//             symbol_i = _mm256_packs_epi16(symbol_12, symbol_34);
//             symbol_i = _mm256_permute4x64_epi64(symbol_i, 0xd8);

//             symbol_abs = _mm256_abs_epi8(symbol_i);
//             symbol_abs = _mm256_sub_epi8(offset, symbol_abs);

//             result1n = _mm256_shuffle_epi8(symbol_i, shuffle_negated_1);
//             result1a = _mm256_shuffle_epi8(symbol_abs, shuffle_abs_1);

//             result2n = _mm256_shuffle_epi8(symbol_i, shuffle_negated_2);
//             result2a = _mm256_shuffle_epi8(symbol_abs, shuffle_abs_2);

//             result1na = _mm256_or_si256(result1n, result1a);
//             result2na = _mm256_or_si256(result2n, result2a);

//             result_ptr = result_ptr.add(1);
//             _mm256_store_si256(result_ptr,
//                             _mm256_permute2x128_si256(result1na, result2na, 0x31));
//             result_ptr = result_ptr.add(1);
//         }
//     }
//     // Demodulate last symbols
//     let next_start = 16 * (num / 16);
//     let mut vec_in = vec_in;
//     let mut llr = llr;
//     unsafe {
//         vec_in = vec_in.add(2 * next_start);
//         llr = llr.add(next_start * 6);
//     }
//     Demod16qamSoftSse(vec_in, llr, num - next_start);
// }

// fn Demod64qamSoftAvx2(vec_in: *const f32, llr: *mut i8, num: usize) {
//     let mut symbols_ptr = vec_in;
//     let mut result_ptr = llr as *mut __m256i;

//     let mut symbol1: __m256;
//     let mut symbol2: __m256;
//     let mut symbol3: __m256;
//     let mut symbol4: __m256;

//     let mut symbol_i1: __m256i;
//     let mut symbol_i2: __m256i;
//     let mut symbol_i3: __m256i;
//     let mut symbol_i4: __m256i;

//     let mut symbol_i: __m256i;
//     let mut symbol_abs: __m256i;
//     let mut symbol_abs2: __m256i;
//     let mut symbol_12: __m256i;
//     let mut symbol_34: __m256i;

//     let offset1: __m256i = unsafe {_mm256_set1_epi8( (4.0 * (SCALE_BYTE_CONV_QAM64 / 42_f32.sqrt() )) as i8)};
//     let offset2: __m256i = unsafe {_mm256_set1_epi8( (2.0 * (SCALE_BYTE_CONV_QAM64 / 42_f32.sqrt() )) as i8)};

//     let mut result11: __m256i;
//     let mut result12: __m256i;
//     let mut result13: __m256i;
//     let mut result21: __m256i;
//     let mut result22: __m256i;
//     let mut result23: __m256i;
//     let mut result31: __m256i;
//     let mut result32: __m256i;
//     let mut result33: __m256i;

//     let mut result_final1: __m256i;
//     let mut result_final2: __m256i;
//     let mut result_final3: __m256i;

//     let scale_v: __m256 = unsafe {_mm256_set1_ps(SCALE_BYTE_CONV_QAM64)};

//     unsafe {

//         let ff = 0xffu8 as i8;

//         let shuffle_negated_1: __m256i = _mm256_set_epi8(
//             ff, ff, 5, 4, ff, ff, ff, ff, 3, 2, ff,
//             ff, ff, ff, 1, 0, ff, ff, 5, 4, ff, ff,
//             ff, ff, 3, 2, ff, ff, ff, ff, 1, 0);

//         let shuffle_negated_2: __m256i = _mm256_set_epi8(
//             11, 10, ff, ff, ff, ff, 9, 8, ff, ff, ff,
//             ff, 7, 6, ff, ff, 11, 10, ff, ff, ff, ff, 9,
//             8, ff, ff, ff, ff, 7, 6, ff, ff);

//         let shuffle_negated_3: __m256i = _mm256_set_epi8(
//         ff, ff, ff, ff, 15, 14, ff, ff, ff, ff, 13, 12, ff,
//         ff, ff, ff, ff, ff, ff, ff, 15, 14, ff, ff, ff, ff,
//         13, 12, ff, ff, ff, ff);

//         let shuffle_abs_1: __m256i = _mm256_set_epi8(
//             5, 4, ff, ff, ff, ff, 3, 2, ff, ff, ff,
//             ff, 1, 0, ff, ff, 5, 4, ff, ff, ff, ff, 3,
//             2, ff, ff, ff, ff, 1, 0, ff, ff);

//         let shuffle_abs_2: __m256i = _mm256_set_epi8(
//             ff, ff, ff, ff, 9, 8, ff, ff, ff, ff, 7,
//             6, ff, ff, ff, ff, ff, ff, ff, ff, 9, 8,
//             ff, ff, ff, ff, 7, 6, ff, ff, ff, ff);

//         let shuffle_abs_3: __m256i = _mm256_set_epi8(
//             ff, ff, 15, 14, ff, ff, ff, ff, 13, 12, ff,
//             ff, ff, ff, 11, 10, ff, ff, 15, 14, ff, ff,
//             ff, ff, 13, 12, ff, ff, ff, ff, 11, 10);

//         let shuffle_abs2_1: __m256i = _mm256_set_epi8(
//             ff, ff, ff, ff, 3, 2, ff, ff, ff, ff, 1,
//             0, ff, ff, ff, ff, ff, ff, ff, ff, 3, 2,
//             ff, ff, ff, ff, 1, 0, ff, ff, ff, ff);

//         let shuffle_abs2_2: __m256i = _mm256_set_epi8(
//             ff, ff, 9, 8, ff, ff, ff, ff, 7, 6, ff,
//             ff, ff, ff, 5, 4, ff, ff, 9, 8, ff, ff,
//             ff, ff, 7, 6, ff, ff, ff, ff, 5, 4);

//         let shuffle_abs2_3: __m256i = _mm256_set_epi8(
//             15, 14, ff, ff, ff, ff, 13, 12, ff, ff, ff,
//             ff, 11, 10, ff, ff, 15, 14, ff, ff, ff, ff,
//             13, 12, ff, ff, ff, ff, 11, 10, ff, ff);

//         for _ in 0..(num/16) {
//             // Load symbols, 4 real and 4 imaginary values at a time
//             symbol1 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             symbol2 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             symbol3 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             symbol4 = _mm256_load_ps(symbols_ptr);
//             symbols_ptr = symbols_ptr.add(8);
//             // Cast symbols into integers
//             symbol_i1 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol1, scale_v));
//             symbol_i2 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol2, scale_v));
//             symbol_i3 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol3, scale_v));
//             symbol_i4 = _mm256_cvtps_epi32(_mm256_mul_ps(symbol4, scale_v));
//             // Pack symbols into 16 bit integers
//             symbol_12 = _mm256_packs_epi32(symbol_i1, symbol_i2);
//             symbol_12 = _mm256_permute4x64_epi64(symbol_12, 0xd8);
//             symbol_34 = _mm256_packs_epi32(symbol_i3, symbol_i4);
//             symbol_34 = _mm256_permute4x64_epi64(symbol_34, 0xd8);
//             // Pack symbols into 8 bit integers (one 256 bit vector)
//             symbol_i = _mm256_packs_epi16(symbol_12, symbol_34);
//             symbol_i = _mm256_permute4x64_epi64(symbol_i, 0xd8);
//             // first LLR is simply the symbol
//             // this LLR corresponds to bit 5 and 4 (both flip over the I and Q axis)
//             // LLR(b5,b4) = |x|
//             symbol_abs = _mm256_abs_epi8(symbol_i);
//             // Take distance between offset1 and symbols for second LLR
//             // offset1 here divides the point where bit 3 and 2 flip (over 4d)
//             // LLR(b3,b2) = 4d - |x|
//             symbol_abs = _mm256_sub_epi8(offset1, symbol_abs);
//             // third LLR is difference between offset2 and first distance
//             // offset2 is 2d (lower point where bit 1 and 0 flip)
//             // LLR(b1,b0) = 2d - |4d - |x||
//             symbol_abs2 = _mm256_sub_epi8(offset2, _mm256_abs_epi8(symbol_abs));

//             // Pack so that the LLRs for real and imaginary part of each modulated value
//             // are distributed as follows:
//             // real msb: imag msb: real: imag: real lsb: imag lsb
//             result11 = _mm256_shuffle_epi8(symbol_i, shuffle_negated_1);
//             result12 = _mm256_shuffle_epi8(symbol_abs, shuffle_abs_1);
//             result13 = _mm256_shuffle_epi8(symbol_abs2, shuffle_abs2_1);

//             result21 = _mm256_shuffle_epi8(symbol_i, shuffle_negated_2);
//             result22 = _mm256_shuffle_epi8(symbol_abs, shuffle_abs_2);
//             result23 = _mm256_shuffle_epi8(symbol_abs2, shuffle_abs2_2);

//             result31 = _mm256_shuffle_epi8(symbol_i, shuffle_negated_3);
//             result32 = _mm256_shuffle_epi8(symbol_abs, shuffle_abs_3);
//             result33 = _mm256_shuffle_epi8(symbol_abs2, shuffle_abs2_3);

//             // OR all results together
//             result_final1 =
//                 _mm256_or_si256(_mm256_or_si256(result11, result12), result13);
//             result_final2 =
//                 _mm256_or_si256(_mm256_or_si256(result21, result22), result23);
//             result_final3 =
//                 _mm256_or_si256(_mm256_or_si256(result31, result32), result33);

//             // Permute to string all results together
//             _mm256_storeu_si256(result_ptr, _mm256_permute2x128_si256(
//                                                 result_final1, result_final2, 0x20));
//             result_ptr = result_ptr.add(1);

//             _mm256_storeu_si256(result_ptr, _mm256_permute2x128_si256(
//                                                 result_final3, result_final1, 0x30));
//             result_ptr = result_ptr.add(1);

//             _mm256_storeu_si256(result_ptr, _mm256_permute2x128_si256(
//                                                 result_final2, result_final3, 0x31));
//             result_ptr = result_ptr.add(1);
//         }
//         // Demodulate last symbols
//         let next_start = 16 * (num / 16);
//         let mut vec_in = vec_in;
//         vec_in = vec_in.add(2 * next_start);
//         let mut llr = llr;
//         llr = llr.add(next_start * 6);
//         Demod64qamSoftSse(vec_in, llr, num - next_start);
//     }
// }

// pub fn Demodulate(equal_ptr: *const f32, demod_ptr: *mut i8,
//     data_num: usize, mod_order: usize, hard_demod: bool) {
//         match mod_order{
//             4 => if !hard_demod {Demod16qamSoftAvx2(equal_ptr, demod_ptr, data_num);}
//             6 => if !hard_demod {Demod64qamSoftAvx2(equal_ptr, demod_ptr, data_num);}
//             _ => {}
//         }
// }
