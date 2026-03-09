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
        "create_buffer_pool" => Some(create_buffer_pool_cm_wrap),
        "create_mutable_buffer_pool" => Some(create_mutable_buffer_pool_cm_wrap),
        "generate_mut_array" => Some(generate_mut_array_cm_wrap),
        "stream_copy_init_pooled" => Some(stream_copy_init_pooled_cm_wrap),
        "stream_scale_init_pooled" => Some(stream_scale_init_pooled_cm_wrap),
        "stream_add_init_pooled" => Some(stream_add_init_pooled_cm_wrap),
        "stream_triad_init_pooled" => Some(stream_triad_init_pooled_cm_wrap),
        "stream_copy_pooled" => Some(stream_copy_pooled_cm_wrap),
        "stream_scale_pooled" => Some(stream_scale_pooled_cm_wrap),
        "stream_add_pooled" => Some(stream_add_pooled_cm_wrap),
        "stream_triad_pooled" => Some(stream_triad_pooled_cm_wrap),
        _ => {
            println!("Function {} not found", func_name);
            panic!("Panicking...");
        }
    }
}
