use build_print::info;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};

fn main() {
    let func_file = env::var("FUNC_PATH").expect("FUNC_PATH environment variable is not set");
    // println!("cargo:rerun-if-changed={}", func_file);
    let path = PathBuf::from(func_file.clone());

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let name_funcs = "funcs.rs";
    let copied_file = out_dir.join(name_funcs);
    let wrapper_file = out_dir.join("wrappers.rs");
    let registry_file = out_dir.join("func_reg.rs");

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

    let output = {
        // Call the transformer script to generate the wrappers
        Command::new("python3")
            .arg("transformer.py")
            .arg(input_path.clone())
            .arg(wrapper_file.to_str().unwrap())
            .arg(registry_file.to_str().unwrap())
            .output()
            .expect("Failed to execute Python script")
    };

    if !output.status.success() {
        let out = String::from_utf8_lossy(&output.stdout);
        let err = String::from_utf8_lossy(&output.stderr);
        panic!(
            "Python script failed (exit {:?})\n--- STDOUT ---\n{}\n--- STDERR ---\n{}",
            output.status.code(),
            out,
            err
        );
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
