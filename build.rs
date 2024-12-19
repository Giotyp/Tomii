use std::process::Command;
use std::path::PathBuf;
use std::{env, fs};
use build_print::info;

fn main() {

    let func_file = env::var("FUNC_PATH").expect("FUNC_PATH environment variable is not set");
    let path = PathBuf::from(func_file.clone());

    // Extract the path to function file
    let func_path = path.parent().unwrap().to_str().unwrap_or("");
    info!("Generating wrappers for functions in {}", func_path);


    let file_name = path.file_name().unwrap().to_str().unwrap();
    // remove the extension
    let file_name = file_name.split(".").collect::<Vec<&str>>()[0];
    let file_extension = path.extension().unwrap().to_str().unwrap();

    info!("File name: {}.{}", file_name, file_extension);

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // copy func_file to OUT_DIR
    let name_funcs = "funcs.rs";
    let dest_path = out_dir.join(name_funcs); // Destination in OUT_DIR

    fs::copy(&func_file, &dest_path)
        .unwrap_or_else(|err| panic!("Failed to copy func.rs to OUT_DIR: {}", err));

    let wrapper_path = out_dir.join("wrappers.rs");
    let registry_path = out_dir.join("func_reg.rs");


    // Call the Python script to generate the wrappers
    let status = Command::new("python3")
        .arg("translator.py")
        .arg(dest_path)
        .arg(wrapper_path.to_str().unwrap())
        .arg(registry_path.to_str().unwrap())
        .status()
        .expect("Failed to execute Python script");

    if !status.success() {
        panic!("Python script failed");
    }

    // If given function file is a .h header
    // linkage with lib<file>.so is required

    if file_extension == "h" {
        info!("Linking with {}/{}", func_path, file_name);
        println!("cargo:rustc-link-lib=dylib=stdc++");
        println!("cargo:rustc-link-search=native={}", func_path);
        println!("cargo:rustc-link-lib=dylib={}", file_name);
        // Add RPATH to the build output
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", func_path);
    }
}