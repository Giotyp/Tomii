use crate::wrappers::*;
use synstream_types::*;

pub fn get_func(func_name: &str) -> Option<CmPtr> {
    match func_name {
        "init_bench_state"      => Some(init_bench_state_cm_wrap),
        "decompose_tiles"       => Some(decompose_tiles_cm_wrap),
        "bilateral_filter_tile" => Some(bilateral_filter_tile_cm_wrap),
        "reassemble_tiles"      => Some(reassemble_tiles_cm_wrap),
        "compute_psnr"          => Some(compute_psnr_cm_wrap),
        _ => {
            println!("Function {} not found", func_name);
            panic!("Panicking...");
        }
    }
}
