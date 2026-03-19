use build_print::info;
use std::path::PathBuf;
use std::process::Command;
use std::{env, fs};
use synstream_converter::generate_from_file;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let name_funcs = "funcs.rs";
    let copied_file = out_dir.join(name_funcs);
    let wrapper_file = out_dir.join("wrappers.rs");
    let registry_file = out_dir.join("func_reg.rs");

    // Check if environment variables are set to bypass transformer
    let wrap_env = env::var("WRAP_PATH").ok();
    let reg_env = env::var("REG_PATH").ok();

    let mut bypass_transformer = false;

    if let Some(wrap_path) = wrap_env {
        let wrap_path = PathBuf::from(wrap_path);
        fs::copy(&wrap_path, &wrapper_file).unwrap_or_else(|err| {
            panic!(
                "Failed to copy wrapper file from {}: {}",
                wrap_path.display(),
                err
            )
        });
        info!("Copied wrapper file from {}", wrap_path.display());

        if let Some(reg_path) = reg_env {
            let reg_path = PathBuf::from(reg_path);
            fs::copy(&reg_path, &registry_file).unwrap_or_else(|err| {
                panic!(
                    "Failed to copy registry file from {}: {}",
                    reg_path.display(),
                    err
                )
            });
            info!("Copied registry file from {}", reg_path.display());
        } else {
            // Create an empty registry file
            fs::write(&registry_file, "// Empty registry file")
                .unwrap_or_else(|err| panic!("Failed to write empty registry file: {}", err));
            info!("Created empty registry file at {}", registry_file.display());
        }

        // Write empty funcs.rs to satisfy dependencies
        fs::write(&copied_file, "// Empty funcs.rs file")
            .unwrap_or_else(|err| panic!("Failed to write empty funcs.rs file: {}", err));
        info!("Created empty funcs.rs file at {}", copied_file.display());

        bypass_transformer = true;
    }

    if bypass_transformer {
        info!("Bypassing transformer script as per environment variables.");
        return;
    }

    let func_file = env::var("FUNC_PATH").expect("FUNC_PATH environment variable is not set");
    // println!("cargo:rerun-if-changed={}", func_file);
    let path = PathBuf::from(func_file.clone());

    // Extract the path to function file
    let func_path = path.parent().unwrap().to_str().unwrap_or("");
    info!("Generating wrappers for functions in {}", func_path);

    let file_name_full = path.file_name().unwrap().to_str().unwrap();
    let file_name = file_name_full.split('.').next().unwrap_or(file_name_full);
    let file_extension = path.extension().unwrap().to_str().unwrap();

    info!("File name: {}.{}", file_name, file_extension);

    if file_extension == "rs" {
        // Write an empty funcs.rs — functions are loaded from the dylib at runtime
        fs::write(&copied_file, "// Functions are loaded from the plugin dylib via wrappers.rs\n")
            .unwrap_or_else(|err| panic!("Failed to write funcs.rs to OUT_DIR: {}", err));

        // Generate wrappers and registry using the Rust converter
        generate_from_file(&path, &wrapper_file, &registry_file)
            .unwrap_or_else(|e| panic!("Converter failed: {}", e));

        info!(
            "Generated wrappers at {} and registry at {}",
            wrapper_file.display(),
            registry_file.display()
        );
    } else if file_extension == "h" {
        // Generate an empty funcs module placeholder for .h files
        let empty_module = r#"
        pub mod funcs {
            // Placeholder for .h files
        }
        "#;

        fs::write(&copied_file, empty_module)
            .unwrap_or_else(|err| panic!("Failed to write empty module to OUT_DIR: {}", err));

        // Call the Python transformer for .h files (C++ interop path)
        let output = Command::new("python3")
            .arg("transformer.py")
            .arg(&path)
            .arg(wrapper_file.to_str().unwrap())
            .arg(registry_file.to_str().unwrap())
            .output()
            .expect("Failed to execute Python script");

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

        // .h files require linking with lib<file>.so
        info!("Linking with {}/{}", func_path, file_name);
        println!("cargo:rustc-link-lib=dylib=stdc++");
        println!("cargo:rustc-link-search=native={}", func_path);
        println!("cargo:rustc-link-lib=dylib={}", file_name);
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", func_path);
    } else {
        panic!("Unsupported file extension: {}", file_extension);
    }
}
