pub mod cmtypes;
pub mod graph_gen;
pub mod graph_struct;
pub mod obj_gen;
pub mod scheduler;
pub mod time_buffer;
pub mod utils_rdtsc;

pub mod funcs {
    include!(concat!(env!("OUT_DIR"), "/funcs.rs"));
}

pub mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/wrappers.rs"));
}

pub mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/func_reg.rs"));
}

// Define the Init module with all the necessary initialization functions/structs
pub mod init_funcs {
    include!(concat!(env!("OUT_DIR"), "/init_funcs.rs"));
}
