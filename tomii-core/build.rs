use build_print::info;
use std::path::PathBuf;
use std::{env, fs};
use tomii_converter::generate_from_file;

fn main() {
    // Signal to lib.rs that the build script ran (used for cfg-gating OUT_DIR includes
    // so rust-analyzer, which skips build scripts, sees empty stubs instead of errors).
    println!("cargo::rustc-check-cfg=cfg(build_rs_ran)");
    println!("cargo:rustc-cfg=build_rs_ran");

    // Declare Py_GIL_DISABLED as a valid cfg key so that code gated with
    // #[cfg(Py_GIL_DISABLED)] in tomii-core compiles without warnings.
    // The actual flag is emitted by pyo3's build script when embed-python is active.
    println!("cargo::rustc-check-cfg=cfg(Py_GIL_DISABLED)");

    // Re-run the build script whenever the wrapper/registry/func env vars change
    // so that cargo detects the new paths and recompiles with the correct functions.
    println!("cargo:rerun-if-env-changed=WRAP_PATH");
    println!("cargo:rerun-if-env-changed=REG_PATH");
    println!("cargo:rerun-if-env-changed=FUNC_PATH");

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

    // No WRAP_PATH and no FUNC_PATH: write no-op stubs so `cargo test` works
    // without requiring a plugin source tree.
    let func_env = env::var("FUNC_PATH").ok();
    if func_env.is_none() {
        fs::write(&copied_file, "// No plugin — stub for cargo test\n")
            .expect("write funcs.rs stub");
        fs::write(&wrapper_file, "pub fn init_wrappers() {}\n").expect("write wrappers.rs stub");
        fs::write(
            &registry_file,
            concat!(
                "use tomii_types::*;\n",
                "fn noop(_: &[CmTypes]) -> CmTypes { CmTypes::None }\n",
                "pub fn get_func(name: &str) -> Option<CmPtr> { if name == \"noop\" { Some(noop) } else { None } }\n",
                "pub fn get_bulk_func(_name: &str) -> Option<CmBulkPtr> { None }\n",
            ),
        )
        .expect("write func_reg.rs stub");
        info!("No FUNC_PATH set — wrote empty stubs for cargo test");
        return;
    }

    let func_file = func_env.unwrap();
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
        fs::write(
            &copied_file,
            "// Functions are loaded from the plugin dylib via wrappers.rs\n",
        )
        .unwrap_or_else(|err| panic!("Failed to write funcs.rs to OUT_DIR: {}", err));

        // Generate wrappers and registry using the Rust converter
        generate_from_file(&path, &wrapper_file, &registry_file)
            .unwrap_or_else(|e| panic!("Converter failed: {}", e));

        info!(
            "Generated wrappers at {} and registry at {}",
            wrapper_file.display(),
            registry_file.display()
        );
    } else if file_extension == "h" || file_extension == "hpp" {
        // Write a placeholder funcs.rs — C functions are loaded from the dylib
        // at runtime via wrappers.rs (libloading), not via static linking.
        fs::write(
            &copied_file,
            "// Functions are loaded from the C dylib via wrappers.rs\n",
        )
        .unwrap_or_else(|err| panic!("Failed to write funcs.rs to OUT_DIR: {}", err));

        // Generate wrappers and registry using the Rust converter (C header path)
        generate_from_file(&path, &wrapper_file, &registry_file)
            .unwrap_or_else(|e| panic!("Converter failed for C header: {}", e));

        info!(
            "Generated wrappers at {} and registry at {}",
            wrapper_file.display(),
            registry_file.display()
        );
    } else {
        panic!("Unsupported file extension: {}", file_extension);
    }
}
