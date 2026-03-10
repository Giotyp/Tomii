use crate::wrappers::*;
use synstream_types::*;

pub fn get_func(func_name: &str) -> Option<CmPtr> {
    match func_name {
        "init_grid" => Some(init_grid_cm_wrap),
        "wf_cell"   => Some(wf_cell_cm_wrap),
        "wf_tile"   => Some(wf_tile_cm_wrap),
        _ => {
            println!("Function {} not found", func_name);
            panic!("Panicking...");
        }
    }
}
