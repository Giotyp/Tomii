use crate::wrappers::*;
use synstream_types::*;

pub fn get_func(func_name: &str) -> Option<CmPtr> {
    match func_name {
        "generate_array" => Some(generate_array_cm_wrap),
        "stream_copy" => Some(stream_copy_cm_wrap),
        "stream_scale" => Some(stream_scale_cm_wrap),
        "stream_add" => Some(stream_add_cm_wrap),
        "stream_triad" => Some(stream_triad_cm_wrap),
        "sink" => Some(sink_cm_wrap),
        _ => {
            println!("Function {} not found", func_name);
            panic!("Panicking...");
        }
    }
}
