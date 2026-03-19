use crate::wrappers::*;
use synstream_types::*;

pub fn get_func(func_name: &str) -> Option<CmPtr> {
    match func_name {
        "generate_vector" => Some(generate_vector_cm_wrap),
        "fft_planner" => Some(fft_planner_cm_wrap),
        "compute_fft" => Some(compute_fft_cm_wrap),
        "vec_to_mat" => Some(vec_to_mat_cm_wrap),
        "mat_mul" => Some(mat_mul_cm_wrap),
        "get_out_file" => Some(get_out_file_cm_wrap),
        "write_to_file" => Some(write_to_file_cm_wrap),
        _ => {
            println!("Function {} not found in registry", func_name);
            panic!("Function not found: {}", func_name);
        }
    }
}
