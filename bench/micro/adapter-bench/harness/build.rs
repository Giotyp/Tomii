//! Runs the real `tomii-converter` codegen pipeline (the same
//! `generate_from_file` used by `tomii-core`'s own `build.rs` when
//! `FUNC_PATH` is set) against the two adapter-bench kernels, so the
//! harness measures the exact wrapper/registry code the runtime would
//! generate for a real plugin — not a hand-rolled approximation.
//!
//! Also compiles `kernel_c.c` directly into the `c-bench` binary (as a
//! statically linked object, no `dlopen`/CmTypes involved) to give that
//! binary a true "no boundary" native-call baseline.

use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());

    let rust_kernel = manifest_dir.join("../kernel-rust/src/lib.rs");
    let c_kernel_header = manifest_dir.join("../kernel-c/include/kernel_c.h");
    let c_kernel_src = manifest_dir.join("../kernel-c/src/kernel_c.c");

    println!("cargo:rerun-if-changed={}", rust_kernel.display());
    println!("cargo:rerun-if-changed={}", c_kernel_header.display());
    println!("cargo:rerun-if-changed={}", c_kernel_src.display());

    tomii_converter::generate_from_file(
        &rust_kernel,
        &out_dir.join("rust_wrappers.rs"),
        &out_dir.join("rust_registry.rs"),
    )
    .expect("failed to generate Rust kernel wrappers");

    tomii_converter::generate_from_file(
        &c_kernel_header,
        &out_dir.join("c_wrappers.rs"),
        &out_dir.join("c_registry.rs"),
    )
    .expect("failed to generate C kernel wrappers");

    // Native-call baseline for c-bench: statically link the same C kernel
    // (no dlopen, no CmTypes) so "native" means exactly that.
    cc::Build::new()
        .file(&c_kernel_src)
        .include(manifest_dir.join("../kernel-c/include"))
        .opt_level(3)
        .flag_if_supported("-march=native")
        .compile("kernel_c_native");
}
