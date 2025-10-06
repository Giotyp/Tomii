pub mod buffers;
pub mod debug;
pub mod graph;
pub mod graph_gen;
pub mod graph_struct;
pub mod json_structs;
pub mod obj_gen;
pub mod runtime;
pub mod scheduler;
pub mod time_buffer;
pub mod utils_rdtsc;

pub mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/wrappers.rs"));
}
pub mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/func_reg.rs"));
}
pub mod funcs {
    include!(concat!(env!("OUT_DIR"), "/funcs.rs"));
}
