pub mod graph_gen;
mod graph_struct;
pub mod executor;
mod python_api;

pub mod funcs {
    include!(concat!(env!("OUT_DIR"), "/funcs.rs"));
}

pub mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/wrappers.rs"));
}

pub mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/func_reg.rs"));
}