use build_print::info;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn main() {
    let func_file = env::var("FUNC_PATH").expect("FUNC_PATH environment variable is not set");
    println!("cargo:rerun-if-changed={}", func_file);
    let path = PathBuf::from(func_file.clone());

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let name_funcs = "funcs.rs";
    let copied_file = out_dir.join(name_funcs);
    let wrapper_file = out_dir.join("wrappers.rs");
    let registry_file = out_dir.join("func_reg.rs");

    if func_file == "python" {
        info!("Python API");
        let output = Command::new("python3")
            .arg("translator.py")
            .arg("a")
            .arg("b")
            .arg(registry_file.to_str().unwrap())
            .arg("True")
            .output()
            .expect("Failed to execute Python script");

        if !output.status.success() {
            panic!("Python script failed");
        } else {
            let python_output = std::str::from_utf8(&output.stdout).unwrap();
            info!("Python output:\n{}", python_output);
        }
        fs::write(&wrapper_file, "").expect("Failed to create empty wrappers.rs file");
        fs::write(&copied_file, "").expect("Failed to create empty function file");
        return;
    }

    // Extract the path to function file
    let func_path = path.parent().unwrap().to_str().unwrap_or("");
    info!("Generating wrappers for functions in {}", func_path);

    let mut file_name = path.file_name().unwrap().to_str().unwrap();
    // remove the extension
    file_name = file_name.split(".").collect::<Vec<&str>>()[0];
    let file_extension = path.extension().unwrap().to_str().unwrap();

    info!("File name: {}.{}", file_name, file_extension);

    if file_extension == "rs" {
        // copy func_file to OUT_DIR for easy linking
        fs::copy(&func_file, &copied_file)
            .unwrap_or_else(|err| panic!("Failed to copy func.rs to OUT_DIR: {}", err));
    } else if file_extension == "h" {
        // Generate an empty module
        let empty_module = r#"
        pub mod funcs {
            // Placeholder for .h files
        }
        "#;

        fs::write(&copied_file, empty_module)
            .unwrap_or_else(|err| panic!("Failed to write empty module to OUT_DIR: {}", err));
    } else {
        panic!("Unsupported file extension: {}", file_extension);
    }

    let input_path = if file_extension == "rs" {
        copied_file
    } else {
        PathBuf::from(func_file.clone())
    };

    // Call the translator script to generate the wrappers
    let output = Command::new("python3")
        .arg("translator.py")
        .arg(input_path)
        .arg(wrapper_file.to_str().unwrap())
        .arg(registry_file.to_str().unwrap())
        .arg("False")
        .output()
        .expect("Failed to execute Python script");

    if !output.status.success() {
        let error_output = std::str::from_utf8(&output.stderr).unwrap();
        panic!("Python script failed: {}", error_output);
    } else {
        let python_output = std::str::from_utf8(&output.stdout).unwrap();
        info!("Python output:\n{}", python_output);
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

    // link with libraries meant to be done by project
    // Link against the MKL library
    println!("cargo:rerun-if-env-changed=MKLROOT");
    println!("cargo:rustc-link-search=native=/opt/intel/oneapi/mkl/2024.0/lib/");
    println!("cargo:rustc-link-lib=static=mkl_intel_lp64");
    println!("cargo:rustc-link-lib=dylib=mkl_core");
    println!("cargo:rustc-link-lib=dylib=mkl_sequential");
    println!("cargo:rustc-link-search=native=/lib/x86_64-linux-gnu/");
    println!("cargo:rustc-link-lib=dylib=pthread");
    println!("cargo:rustc-link-lib=dylib=m");
    println!("cargo:rustc-link-lib=dylib=dl");
    println!("cargo:rustc-link-lib=dylib=stdc++");
}
