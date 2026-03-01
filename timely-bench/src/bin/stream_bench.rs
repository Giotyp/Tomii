//! Timely Dataflow STREAM benchmark.
//!
//! Each Timely worker independently allocates and operates on its own copy of
//! the arrays (total memory = workers * array_size * bytes_per_element).
//! This matches the SynStream STREAM setup: each worker runs one instance of
//! the kernel on an independent array.
//!
//! Usage:
//!   cargo run -r -p timely-bench --bin stream_bench -- \
//!     --kernel triad --array-size 268435456 --workers 4 --output timely_stream.csv

use clap::Parser;
use std::sync::{Arc, Barrier};
use std::time::Instant;
use timely_bench::append_csv;

#[derive(Parser, Debug)]
#[command(name = "stream_bench", about = "Timely Dataflow STREAM benchmark")]
struct Cli {
    /// STREAM kernel: copy | scale | add | triad
    #[arg(long, value_parser = ["copy", "scale", "add", "triad"], default_value = "triad")]
    kernel: String,

    /// Array size per worker (number of f64 elements)
    #[arg(long, default_value_t = 268_435_456usize)]
    array_size: usize,

    /// Number of Timely worker threads
    #[arg(long, default_value_t = 1usize)]
    workers: usize,

    /// Scalar for scale/triad kernels
    #[arg(long, default_value_t = 3.0f64)]
    scalar: f64,

    /// Output CSV file
    #[arg(long, default_value = "timely_stream.csv")]
    output: String,

    /// Measurement repetitions (warm-up excluded from stats)
    #[arg(long, default_value_t = 20usize)]
    reps: usize,

    /// Warm-up repetitions
    #[arg(long, default_value_t = 3usize)]
    warmup: usize,
}

fn main() {
    let cli = Cli::parse();
    let kernel = cli.kernel.clone();
    let array_size = cli.array_size;
    let num_workers = cli.workers;
    let scalar = cli.scalar;
    let reps = cli.reps;
    let warmup = cli.warmup;
    let output = cli.output.clone();

    let arrays = match kernel.as_str() {
        "copy" | "scale" => 2usize,
        "add" | "triad" => 3usize,
        _ => unreachable!(),
    };
    // bytes per repetition = workers * arrays * array_size * 8 bytes
    let bytes_total = num_workers * arrays * array_size * 8;

    // Use a barrier so all workers start the timed section simultaneously
    let barrier = Arc::new(Barrier::new(num_workers));
    // Shared elapsed time: max across workers (wall-clock of last worker)
    let elapsed_vec: Arc<std::sync::Mutex<Vec<f64>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut all_elapsed: Vec<f64> = Vec::with_capacity(warmup + reps);

    for _rep in 0..(warmup + reps) {
        let barrier_rep = barrier.clone();
        let elapsed_rep = elapsed_vec.clone();
        let kernel_rep = kernel.clone();

        timely::execute_from_args(
            vec![String::from("-w"), num_workers.to_string()].into_iter(),
            move |worker| {
                let _wi = worker.index();
                let n = array_size;

                // Allocate arrays local to each worker
                let mut a: Vec<f64> = vec![0.0; n];
                let b: Vec<f64> = vec![2.0; n];
                let c: Vec<f64> = vec![1.0; n];

                // All workers wait at barrier before starting timed section
                barrier_rep.wait();
                let t0 = Instant::now();

                match kernel_rep.as_str() {
                    "copy" => {
                        for i in 0..n { a[i] = b[i]; }
                    }
                    "scale" => {
                        for i in 0..n { a[i] = scalar * b[i]; }
                    }
                    "add" => {
                        for i in 0..n { a[i] = b[i] + c[i]; }
                    }
                    "triad" => {
                        for i in 0..n { a[i] = b[i] + scalar * c[i]; }
                    }
                    _ => unreachable!(),
                }

                // Prevent dead-code elimination
                std::hint::black_box(&a);

                let elapsed = t0.elapsed().as_secs_f64();

                // Record max elapsed across workers (last worker to finish = total wall-clock)
                let mut guard = elapsed_rep.lock().unwrap();
                guard.push(elapsed);
            },
        )
        .expect("Timely execute failed");

        // Max elapsed across all workers for this rep
        let max_elapsed = {
            let mut guard = elapsed_vec.lock().unwrap();
            let v = guard.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            guard.clear();
            v
        };

        all_elapsed.push(max_elapsed);
    }

    // Exclude warm-up
    let timed = &all_elapsed[warmup..];
    let mean_elapsed = timed.iter().sum::<f64>() / timed.len() as f64;
    let gb_s = bytes_total as f64 / mean_elapsed / 1e9;

    println!(
        "Timely STREAM {} | workers={} | array_size={} | mean={:.4}s | {:.2} GB/s",
        kernel, num_workers, array_size, mean_elapsed, gb_s
    );

    append_csv(&output, "timely", &kernel, array_size, num_workers, mean_elapsed, gb_s);
}
