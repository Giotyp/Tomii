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

    // Initialization file
    let init_file = match env::var("INIT_PATH") {
        Ok(path) => path,
        Err(_) => {
            info!("INIT_PATH environment variable is not set. Skipping initialization.");
            "".to_string()
        }
    };
    println!("cargo:rerun-if-changed={}", init_file);

    let init_funcs = "init_funcs.rs";
    let init_path = out_dir.join(init_funcs);

    let output = {
        if !init_file.is_empty() {
            info!("Generating init functions for {}", init_file);
            fs::copy(&init_file, &init_path)
                .unwrap_or_else(|err| panic!("Failed to copy init_func.rs to OUT_DIR: {}", err));

            // Call the translator script to generate the wrappers
            Command::new("python3")
                .arg("translator.py")
                .arg(input_path.clone())
                .arg(wrapper_file.to_str().unwrap())
                .arg(registry_file.to_str().unwrap())
                .arg("False")
                .arg(init_path.clone())
                .output()
                .expect("Failed to execute Python script")
        } else {
            // write empty module
            let empty_module = r#"
                pub mod init_funcs {
                    // Placeholder for .h files
                }
                "#;

            fs::write(&init_path, empty_module)
                .unwrap_or_else(|err| panic!("Failed to write empty module to OUT_DIR: {}", err));

            // Call translator without init file
            Command::new("python3")
                .arg("translator.py")
                .arg(input_path.clone())
                .arg(wrapper_file.to_str().unwrap())
                .arg(registry_file.to_str().unwrap())
                .arg("False")
                .output()
                .expect("Failed to execute Python script")
        }
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

    let project_src = env::var("SRC_PATH").expect("SRC_PATH environment variable is not set");
    println!("cargo:rerun-if-changed={}", project_src);

    let user_include = PathBuf::from(project_src).join("generated_jraph.rs");
    info!("Writing generated_jraph.rs to {}", user_include.display());

    let content = format!(
        r#"
pub mod funcs {{
    include!("{}");
}}
pub mod init_funcs {{
    include!("{}");
}}
pub mod wrappers {{
    include!("{}");
}}
pub mod func_reg {{
    include!("{}");
}}
"#,
        input_path.display(),
        init_path.display(),
        wrapper_file.display(),
        registry_file.display()
    );

    fs::write(&user_include, content).expect("Failed to write generated_jraph.rs");

    println!(
        "cargo:warning=Generated include file at {:?}",
        user_include.display()
    );
}
