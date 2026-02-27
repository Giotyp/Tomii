fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let lib_dir = format!("{}/lib", crate_dir);

    // Add search path for build-time linking
    println!("cargo:rustc-link-search=native={}", lib_dir);

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

    // Link with demod library
    println!("cargo:rustc-link-lib=dylib=demod");

    // Link with armadillo library
    println!("cargo:rustc-link-search=native=/usr/include");
    println!("cargo:rustc-link-lib=dylib=armadillo");

    // Link with beamfuncs library
    println!("cargo:rustc-link-lib=dylib=beamfuncs");

    // Link with fftfuncs library
    println!("cargo:rustc-link-lib=dylib=fftfuncs");

    // Link with scrambler library
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rustc-link-search=native={}", lib_dir);
    println!("cargo:rustc-link-lib=dylib=scrambler");

    // Link against the phy_ldpc_decoder library

    let flexran_sdk_path = "/opt/FlexRAN-FEC-SDK-19-04/sdk";
    // Path to the build output directory
    let build_output_path = format!("{}/build-avx512-icc/install", flexran_sdk_path);
    let lib_common_path = format!("{}/lib_common", build_output_path);

    // Path to the header file and library
    let lib_ldpc_decoder_path = format!("{}/lib_ldpc_decoder_5gnr", build_output_path);

    println!("cargo:rustc-link-search=native={}", lib_ldpc_decoder_path);
    println!("cargo:rustc-link-lib=static=ldpc_decoder_5gnr");

    // Link against the common library
    println!("cargo:rustc-link-search=native={}", lib_common_path);
    println!("cargo:rustc-link-lib=dylib=common");

    // Add RPATH to the build output
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
    println!("cargo:rustc-link-arg=-Wl,-z,notext");
}
