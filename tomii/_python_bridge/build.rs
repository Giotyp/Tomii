fn main() {
    // Declare Py_GIL_DISABLED as a valid cfg key. This flag is set by
    // pyo3-build-config when the linked Python is a free-threaded (3.13t) build.
    println!("cargo::rustc-check-cfg=cfg(Py_GIL_DISABLED)");
}
