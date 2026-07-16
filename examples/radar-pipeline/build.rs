fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let kernels_dir = format!("{}/kernels", crate_dir);

    // libradar_kernels.so (CPU/FFTW) — swapped for the CUDA twin by pointing
    // RADAR_KERNELS_DIR at a directory holding a GPU build of the same soname.
    println!("cargo:rerun-if-env-changed=RADAR_KERNELS_DIR");
    let link_dir = std::env::var("RADAR_KERNELS_DIR").unwrap_or(kernels_dir);
    println!("cargo:rustc-link-search=native={}", link_dir);
    println!("cargo:rustc-link-lib=dylib=radar_kernels");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", link_dir);
}
