use crate::functions::*;
use std::fs::OpenOptions;
use std::io::Write;

pub fn validate(buf_size: usize, num_nodes: usize, script_dir: &str) {
    println!(
        "--- Validating with buf_size: {}, num_nodes: {} ---\n",
        buf_size, num_nodes
    );

    let fft_planner = fft_planner(buf_size);
    let mut orig_vector = generate_vector(buf_size);
    compute_fft(fft_planner.clone(), &mut orig_vector);
    let orig_matrix = vec_to_mat(&orig_vector);
    let mat_mul_res = mat_mul(&orig_matrix, &orig_matrix);

    let validation_file = "validation.txt";
    let out_file_path = format!("{}/{}", script_dir, validation_file);
    std::fs::File::create(&out_file_path)
        .unwrap_or_else(|_| panic!("Failed to create or open file: {}", out_file_path));

    let mut file = OpenOptions::new()
        .create(false)
        .append(true)
        .open(&out_file_path)
        .expect("Failed to open or create output file");

    for idx in 0..num_nodes {
        writeln!(file, "Buffer-{}:", idx).expect("Failed to write buffer header");
        write!(file, "{{").expect("Failed to write opening brace");

        // Write each element of the matrix
        for (i, elem) in mat_mul_res.iter().enumerate() {
            if i > 0 {
                write!(file, ", ").expect("Failed to write separator");
            }
            write!(file, "{}+{}i", elem.re, elem.im).expect("Failed to write element");
        }

        writeln!(file, "}}").expect("Failed to write closing brace");
    }
}
