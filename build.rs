use std::process::Command;
use std::path::PathBuf;
use std::env;
use build_print::info;

fn main() {

    let func_file = env::var("FUNC_PATH").expect("FUNC_PATH environment variable is not set");
    let path = PathBuf::from(func_file.clone());

    // Extract the path to function file
    let func_path = path.parent().unwrap().to_str().unwrap_or("");
    info!("Generating wrappers for functions in {}", func_path);

    let dest_path = PathBuf::from(func_path).join("wrappers.rs");

    let file_name = path.file_name().unwrap().to_str().unwrap();
    // remove the extension
    let file_name = file_name.split(".").collect::<Vec<&str>>()[0];
    let file_extension = path.extension().unwrap().to_str().unwrap();

    info!("File name: {}.{}", file_name, file_extension);


    // Call the Python script to generate the wrappers
    let status = Command::new("python3")
        .arg("translator.py")
        .arg(func_file)
        .arg(dest_path.to_str().unwrap())
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