use std::process::Command;
use std::path::PathBuf;
use std::env;

fn main() {

    let func_file = env::var("FUNC_PATH").expect("FUNC_PATH environment variable is not set");
    let path = PathBuf::from(func_file.clone());

    // Extract the path to function file
    let func_path = path.parent().unwrap().to_str().unwrap_or("");
    println!("Generating wrappers for functions in {}", func_path);

    let dest_path = PathBuf::from(func_path).join("wrappers.rs");


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
}