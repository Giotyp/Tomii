use std::{env, fs, path::PathBuf};

fn main() {
    // Declare Py_GIL_DISABLED as a valid cfg key. This flag is set by
    // pyo3-build-config when the linked Python is a free-threaded (3.13t) build.
    println!("cargo::rustc-check-cfg=cfg(Py_GIL_DISABLED)");

    // Write python_abi.rs into OUT_DIR so lib.rs can include it.
    // Only the version bits (major/minor) are baked here; the GIL_DISABLED bit
    // is set at compile time via cfg!(Py_GIL_DISABLED) in lib.rs so that the
    // build script does not need to access the `gil_disabled` field, which was
    // not present in pyo3-build-config < 0.23.
    let info = pyo3_build_config::get();
    let major = info.version.major as u32;
    let minor = info.version.minor as u32;
    let abi_base = (major << 24) | (minor << 16);

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::write(
        out_dir.join("python_abi.rs"),
        format!(
            "/// Base ABI (bits[31:24]=major, bits[23:16]=minor); \
             bit[15] (GIL_DISABLED) is ORed in by tomii_python_bridge_abi().\n\
             const PYTHON_BRIDGE_ABI_BASE: u32 = {abi_base};\n"
        ),
    )
    .expect("failed to write python_abi.rs");

    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");
}
