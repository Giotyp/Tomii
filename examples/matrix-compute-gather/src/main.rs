mod functions;

use functions::*;
use nalgebra::DMatrix;
use num_complex::Complex32;
use std::collections::HashMap;
use std::time::{Duration, Instant};

pub fn main() {
    let buf_size = 100;
    let vector_num = 200;
    let repeat = 100;
    let warmup = 10;
    let fft_planner = fft_planner(buf_size);

    let mut timings: HashMap<&str, Vec<Duration>> = HashMap::new();

    // Time gen_vec
    timings.insert("gen_vec", vec![]);
    for _ in 0..warmup {
        let _ = generate_vector(buf_size);
    }
    for _ in 0..repeat {
        let start = Instant::now();
        let _ = generate_vector(buf_size);
        let duration = start.elapsed();
        timings.get_mut("gen_vec").unwrap().push(duration);
    }

    // Time compute_fft
    timings.insert("compute_fft", vec![]);
    let orig_vector = generate_vector(buf_size);
    for _ in 0..warmup {
        let mut vector = orig_vector.clone();
        compute_fft(fft_planner.clone(), &mut vector);
    }
    for _ in 0..repeat {
        let mut vector = orig_vector.clone();
        let planner = fft_planner.clone();
        let start = Instant::now();
        compute_fft(planner, &mut vector);
        let duration = start.elapsed();
        timings.get_mut("compute_fft").unwrap().push(duration);
    }

    // Time vec_to_mat
    timings.insert("vec_to_mat", vec![]);
    let mut orig_vector = generate_vector(buf_size);
    compute_fft(fft_planner.clone(), &mut orig_vector);
    for _ in 0..warmup {
        let v = orig_vector.clone();
        let _ = vec_to_mat(&v);
    }
    for _ in 0..repeat {
        let v = orig_vector.clone();
        let start = Instant::now();
        let _ = vec_to_mat(&v);
        let duration = start.elapsed();
        timings.get_mut("vec_to_mat").unwrap().push(duration);
    }

    // Time mat_mul
    timings.insert("mat_mul_casc", vec![]);
    let mut vectors: Vec<DMatrix<Complex32>> = Vec::new();
    for _ in 0..vector_num {
        let mut vec = generate_vector(buf_size);
        compute_fft(fft_planner.clone(), &mut vec);
        let mat = vec_to_mat(&vec);
        vectors.push(mat);
    }

    for _ in 0..warmup {
        let _ = mat_mul_dm(&vectors);
    }
    for _ in 0..repeat {
        let start = Instant::now();
        let _ = mat_mul_dm(&vectors);
        let duration = start.elapsed();
        timings.get_mut("mat_mul_casc").unwrap().push(duration);
    }

    // Print timings - Avg, Min, Max for each function
    for (func, times) in &timings {
        let total: Duration = times.iter().sum();
        let avg = total / (times.len() as u32);
        let min = times.iter().min().unwrap();
        let max = times.iter().max().unwrap();
        println!("{} - Avg: {:?}, Min: {:?}, Max: {:?}", func, avg, min, max);
    }
}
