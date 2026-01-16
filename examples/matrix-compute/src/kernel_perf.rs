use crate::functions::*;
use crate::wrap;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use synstream_types::CmTypes;

pub fn measure_performance(buf_size: usize, repeat: usize, warmup: usize) {
    println!(
        "--- Measuring performance with buf_size: {}, repeat: {}, warmup: {} ---\n",
        buf_size, repeat, warmup
    );
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

    // Time wrapper
    let buf_size_cm = CmTypes::Usize(buf_size);
    timings.insert("gen_vec_wrap", vec![]);
    for _ in 0..warmup {
        let _ = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
    }
    for _ in 0..repeat {
        let vec = vec![buf_size_cm.clone()];
        let start = Instant::now();
        let _ = wrap::generate_vector_cm_wrap(vec);
        let duration = start.elapsed();
        timings.get_mut("gen_vec_wrap").unwrap().push(duration);
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

    // Time compute_fft_wrap
    timings.insert("compute_fft_wrap", vec![]);
    let fft_planner_cm = wrap::fft_planner_cm_wrap(vec![buf_size_cm.clone()]);
    let buf_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
    for _ in 0..warmup {
        let args = vec![fft_planner_cm.clone(), buf_cm.clone()];
        wrap::compute_fft_cm_wrap(args);
    }
    for _ in 0..repeat {
        let args = vec![fft_planner_cm.clone(), buf_cm.clone()];
        let start = Instant::now();
        wrap::compute_fft_cm_wrap(args);
        let duration = start.elapsed();
        timings.get_mut("compute_fft_wrap").unwrap().push(duration);
    }

    // Time vec_to_mat
    timings.insert("vec_to_mat", vec![]);
    for _ in 0..warmup {
        let mut v = generate_vector(buf_size);
        compute_fft(fft_planner.clone(), &mut v);
        let _ = vec_to_mat(&v);
    }
    for _ in 0..repeat {
        let mut v = generate_vector(buf_size);
        compute_fft(fft_planner.clone(), &mut v);
        let start = Instant::now();
        let _ = vec_to_mat(&v);
        let duration = start.elapsed();
        timings.get_mut("vec_to_mat").unwrap().push(duration);
    }

    // Time wrap vec_to_mat
    timings.insert("vec_to_mat_wrap", vec![]);
    for _ in 0..warmup {
        let fft_buf_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
        wrap::compute_fft_cm_wrap(vec![fft_planner_cm.clone(), fft_buf_cm.clone()]);
        let args = vec![fft_buf_cm.clone()];
        let _ = wrap::vec_to_mat_cm_wrap(args);
    }
    for _ in 0..repeat {
        let fft_buf_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
        wrap::compute_fft_cm_wrap(vec![fft_planner_cm.clone(), fft_buf_cm.clone()]);
        let args = vec![fft_buf_cm.clone()];
        let start = Instant::now();
        let _ = wrap::vec_to_mat_cm_wrap(args);
        let duration = start.elapsed();
        timings.get_mut("vec_to_mat_wrap").unwrap().push(duration);
    }

    // Time mat_mul
    timings.insert("mat_mul", vec![]);
    let mut orig_vector = generate_vector(buf_size);
    compute_fft(fft_planner.clone(), &mut orig_vector);
    let orig_matrix = vec_to_mat(&orig_vector);
    for _ in 0..warmup {
        let a = orig_matrix.clone();
        let b = orig_matrix.clone();
        let _ = mat_mul(&a, &b);
    }
    for _ in 0..repeat {
        let a = orig_matrix.clone();
        let b = orig_matrix.clone();
        let start = Instant::now();
        let _ = mat_mul(&a, &b);
        let duration = start.elapsed();
        timings.get_mut("mat_mul").unwrap().push(duration);
    }
    // Time wrap mat_mul
    timings.insert("mat_mul_wrap", vec![]);
    for _ in 0..warmup {
        let fft_buf_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
        wrap::compute_fft_cm_wrap(vec![fft_planner_cm.clone(), fft_buf_cm.clone()]);
        let mat_cm = wrap::vec_to_mat_cm_wrap(vec![fft_buf_cm.clone()]);
        let args = vec![mat_cm.clone(), mat_cm.clone()];
        let _ = wrap::mat_mul_cm_wrap(args);
    }
    for _ in 0..repeat {
        let fft_buf_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
        wrap::compute_fft_cm_wrap(vec![fft_planner_cm.clone(), fft_buf_cm.clone()]);
        let mat_cm = wrap::vec_to_mat_cm_wrap(vec![fft_buf_cm.clone()]);
        let args = vec![mat_cm.clone(), mat_cm.clone()];
        let start = Instant::now();
        let _ = wrap::mat_mul_cm_wrap(args);
        let duration = start.elapsed();
        timings.get_mut("mat_mul_wrap").unwrap().push(duration);
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
