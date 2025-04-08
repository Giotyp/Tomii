use build_print::info;
use std::path::PathBuf;
use std::{env, fs};

fn main() {
    let func_file = env::var("INIT_PATH").expect("INIT_PATH environment variable is not set");
    println!("cargo:rerun-if-changed={}", func_file);

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let name_funcs = "init_funcs.rs";
    let copied_file = out_dir.join(name_funcs);

    info!("Generating init functions for {}", func_file);
    fs::copy(&func_file, &copied_file)
            .unwrap_or_else(|err| panic!("Failed to copy init_func.rs to OUT_DIR: {}", err));
}