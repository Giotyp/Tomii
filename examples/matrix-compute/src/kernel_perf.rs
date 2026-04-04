use crate::functions::*;
use crate::wrap;
use std::time::{Duration, Instant};
use synstream_types::CmTypes;

/// Buffer sizes to sweep — all perfect squares (vec_to_mat constraint).
const SIZES: &[usize] = &[64, 256, 1024, 4096];

/// Minimum samples before the time cap kicks in.
const MIN_ITERS: usize = 200;
/// Maximum wall time per (raw, wrap) pair.
const MAX_SECS: f64 = 2.0;
/// Warmup iterations for each function.
const WARMUP: usize = 100;

fn stats(times: &[Duration]) -> (f64, f64) {
    // Returns (avg_ns, stddev_ns).
    let n = times.len() as f64;
    let avg = times.iter().map(|d| d.as_nanos() as f64).sum::<f64>() / n;
    let var = times
        .iter()
        .map(|d| (d.as_nanos() as f64 - avg).powi(2))
        .sum::<f64>()
        / n;
    (avg, var.sqrt())
}

/// Run raw and wrap closures interleaved until MIN_ITERS samples collected
/// and MAX_SECS elapsed.  Interleaving ensures both measurements share the
/// same cache/thermal state each iteration, preventing sequential bias.
fn bench_pair<F, G>(mut raw: F, mut wrap: G) -> ((f64, f64), (f64, f64), usize)
where
    F: FnMut(),
    G: FnMut(),
{
    // Warmup both together.
    for _ in 0..WARMUP {
        raw();
        wrap();
    }

    let mut raw_times: Vec<Duration> = Vec::with_capacity(MIN_ITERS * 2);
    let mut wrap_times: Vec<Duration> = Vec::with_capacity(MIN_ITERS * 2);
    let deadline = Instant::now() + Duration::from_secs_f64(MAX_SECS);

    loop {
        let t = Instant::now();
        raw();
        raw_times.push(t.elapsed());

        let t = Instant::now();
        wrap();
        wrap_times.push(t.elapsed());

        if raw_times.len() >= MIN_ITERS && Instant::now() >= deadline {
            break;
        }
    }

    let n = raw_times.len();
    (stats(&raw_times), stats(&wrap_times), n)
}

fn row(kernel: &str, size: &str, r: (f64, f64), w: (f64, f64), n: usize) {
    let (r_avg, r_sd) = r;
    let (w_avg, w_sd) = w;
    let ovhd = w_avg - r_avg;
    let pct = if r_avg > 1.0 {
        format!("{:>6.1}%", ovhd / r_avg * 100.0)
    } else {
        "   n/a".to_string()
    };
    println!(
        "{:<20} {:>6}  {:>10.1} {:>8.1}  {:>10.1} {:>8.1}  {:>10.1}  {}  n={}",
        kernel, size, r_avg, r_sd, w_avg, w_sd, ovhd, pct, n
    );
}

pub fn measure_performance(_buf_size: usize, _repeat: usize, _warmup: usize) {
    let sep = "-".repeat(100);
    println!(
        "\n=== SynStream Plugin Adapter Overhead  \
         (min_iters={MIN_ITERS}, max_secs={MAX_SECS}s, warmup={WARMUP}) ===\n"
    );
    println!(
        "{:<20} {:>6}  {:>10} {:>8}  {:>10} {:>8}  {:>10}  {:>7}",
        "Kernel", "Size", "Raw(ns)", "Std", "Wrap(ns)", "Std", "Ovhd(ns)", "Ovhd%"
    );
    println!("{sep}");

    // Zero-compute baseline: isolates Vec<CmTypes> alloc + fn-ptr dispatch.
    {
        let (r, w, n) = bench_pair(|| {}, || {
            wrap::noop_cm_wrap(vec![]);
        });
        row("noop (0 args)", "-", r, w, n);
    }
    println!("{sep}");

    for &size in SIZES {
        let size_str = size.to_string();
        let buf_size_cm = CmTypes::Usize(size);
        let fft_plan = fft_planner(size);
        let fft_plan_cm = wrap::fft_planner_cm_wrap(vec![buf_size_cm.clone()]);

        // gen_vec
        {
            let (r, w, n) = bench_pair(
                || {
                    let _ = generate_vector(size);
                },
                || {
                    let _ = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
                },
            );
            row("gen_vec", &size_str, r, w, n);
        }

        // compute_fft — reuse same buffer across iterations (no O(n) clone per call).
        // Both raw and wrap operate on their own reused buffer; interleaving keeps
        // both hot in cache simultaneously.
        {
            let mut v = generate_vector(size);
            let buf_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
            let (r, w, n) = bench_pair(
                || {
                    compute_fft(fft_plan.clone(), &mut v);
                },
                || {
                    wrap::compute_fft_cm_wrap(vec![fft_plan_cm.clone(), buf_cm.clone()]);
                },
            );
            row("compute_fft", &size_str, r, w, n);
        }

        // vec_to_mat — pre-apply FFT so input is in the correct state.
        {
            let mut v2 = generate_vector(size);
            compute_fft(fft_plan.clone(), &mut v2);
            let buf2_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
            wrap::compute_fft_cm_wrap(vec![fft_plan_cm.clone(), buf2_cm.clone()]);
            let (r, w, n) = bench_pair(
                || {
                    let _ = vec_to_mat(&v2);
                },
                || {
                    let _ = wrap::vec_to_mat_cm_wrap(vec![buf2_cm.clone()]);
                },
            );
            row("vec_to_mat", &size_str, r, w, n);
        }

        // mat_mul
        {
            let mut v3 = generate_vector(size);
            compute_fft(fft_plan.clone(), &mut v3);
            let mat = vec_to_mat(&v3);
            let buf3_cm = wrap::generate_vector_cm_wrap(vec![buf_size_cm.clone()]);
            wrap::compute_fft_cm_wrap(vec![fft_plan_cm.clone(), buf3_cm.clone()]);
            let mat_cm = wrap::vec_to_mat_cm_wrap(vec![buf3_cm]);
            let (r, w, n) = bench_pair(
                || {
                    let _ = mat_mul(&mat, &mat);
                },
                || {
                    let _ = wrap::mat_mul_cm_wrap(vec![mat_cm.clone(), mat_cm.clone()]);
                },
            );
            row("mat_mul", &size_str, r, w, n);
        }

        println!();
    }
}
