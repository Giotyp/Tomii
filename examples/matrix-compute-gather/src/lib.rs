pub mod functions;

/// Functions that Return CmTypes and will be wrapped
use functions::*;
use nalgebra::*;
use num_complex::Complex32;
use rustfft::Fft;
use std::sync::Arc;
use synstream_types::CmTypes;

#[no_mangle]
pub fn generate_vector_cm(n: usize) -> CmTypes {
    let vector = generate_vector(n);
    CmTypes::from_any(vector)
}

#[no_mangle]
pub fn fft_planner_cm(buf_size: usize) -> CmTypes {
    CmTypes::from_any(fft_planner(buf_size))
}

#[no_mangle]
pub fn compute_fft_cm(fft_planner: &CmTypes, buffer: &CmTypes) {
    fft_planner
        .with_any(|fft_planner_ref: &Arc<dyn Fft<f32>>| {
            buffer
                .with_any_mut(|buffer_mut: &mut Vec<Complex32>| {
                    compute_fft(fft_planner_ref.clone(), buffer_mut);
                })
                .expect("Failed to access buffer struct or wrong type")
        })
        .expect("Failed to access fft_planner struct or wrong type")
}

#[no_mangle]
pub fn vec_to_mat_cm(vector: &CmTypes) -> CmTypes {
    vector
        .with_any(|vector_ref: &Vec<Complex32>| CmTypes::from_any(vec_to_mat(vector_ref)))
        .expect("Failed to access vector or wrong type")
}

#[no_mangle]
pub fn mat_mul_cm(vectors: Vec<CmTypes>) -> CmTypes {
    CmTypes::from_any(mat_mul(vectors))
}

#[no_mangle]
pub fn get_out_file(env_var: &str, out_file: &str) -> String {
    let curr_dir = std::env::var(env_var).unwrap_or_else(|_| {
        panic!(
            "Environment variable '{}' not found or could not be read",
            env_var
        )
    });
    let out_file_path = format!("{}/{}", curr_dir, out_file);
    // Create file if it doesn't exist
    std::fs::File::create(&out_file_path)
        .unwrap_or_else(|_| panic!("Failed to create or open file: {}", out_file_path));
    out_file_path
}

#[no_mangle]
pub fn write_to_file(file_path: &str, buffers: &Vec<CmTypes>) {
    use std::fs::OpenOptions;
    use std::io::Write;

    let mut file = OpenOptions::new()
        .create(false)
        .append(true)
        .open(file_path)
        .expect("Failed to open or create output file");

    for (idx, buffer) in buffers.iter().enumerate() {
        writeln!(file, "Buffer-{}:", idx).expect("Failed to write buffer header");

        buffer
            .with_any(|matrix_ref: &DMatrix<Complex32>| {
                write!(file, "{{").expect("Failed to write opening brace");

                // Write each element of the matrix
                for (i, elem) in matrix_ref.iter().enumerate() {
                    if i > 0 {
                        write!(file, ", ").expect("Failed to write separator");
                    }
                    write!(file, "{}+{}i", elem.re, elem.im).expect("Failed to write element");
                }

                writeln!(file, "}}").expect("Failed to write closing brace");
            })
            .unwrap_or_else(|| {
                eprintln!("Failed to access buffer as DMatrix<Complex32> or wrong type");
                writeln!(file, "{{Error: Unable to read buffer}}").expect("Failed to write error");
            });
    }
}
