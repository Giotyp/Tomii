fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let kernels_dir = format!("{}/kernels", crate_dir);

    // Dynamic-registration PoC: when the `dynamic-registration` feature is on,
    // generate a self-contained `__tomii_exports` table (per-kernel marshalling
    // wrappers that call the `#[tomii_export]` companions directly, plus the
    // export descriptor table) into OUT_DIR, `include!`d by lib.rs. OFF by
    // default, so the normal plugin build is unchanged.
    if std::env::var_os("CARGO_FEATURE_DYNAMIC_REGISTRATION").is_some() {
        let out_dir = std::env::var("OUT_DIR").unwrap();
        let src = std::path::Path::new(&crate_dir).join("src/lib.rs");
        let out = std::path::Path::new(&out_dir).join("tomii_exports.rs");
        println!("cargo:rerun-if-changed=src/lib.rs");
        tomii_converter::generate_self_contained_file(&src, &out)
            .expect("failed to generate __tomii_exports table");
    }

    // libradar_kernels.so (CPU/FFTW) — swapped for the CUDA twin by pointing
    // RADAR_KERNELS_DIR at a directory holding a GPU build of the same soname.
    println!("cargo:rerun-if-env-changed=RADAR_KERNELS_DIR");
    let link_dir = std::env::var("RADAR_KERNELS_DIR").unwrap_or(kernels_dir);
    println!("cargo:rustc-link-search=native={}", link_dir);
    println!("cargo:rustc-link-lib=dylib=radar_kernels");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", link_dir);
}
