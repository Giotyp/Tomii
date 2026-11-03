# Dynamic registration (experimental, off by default)

By default `app.build()` regenerates the runtime's wrappers from your source
(`FUNC_PATH`) and rebuilds `tomii-core`, so adding a kernel rebuilds the runtime. With
the `dynamic-registration` feature, the plugin `.so` describes its own kernels in a
`__tomii_exports` table and the runtime loads them at startup, so adding a kernel
rebuilds only the plugin. To opt in (the repository's `examples/` contain a plugin with this feature):

1. Plugin `Cargo.toml`: `[features] dynamic-registration = []` and
   `[build-dependencies] tomii-converter = { path = "../../tomii-converter" }`.
2. Plugin `build.rs`: when `CARGO_FEATURE_DYNAMIC_REGISTRATION` is set, call
   `tomii_converter::generate_self_contained_file(src/lib.rs, $OUT_DIR/tomii_exports.rs)`.
3. Plugin `src/lib.rs`: `#[cfg(feature = "dynamic-registration")] include!(concat!(env!("OUT_DIR"), "/tomii_exports.rs"));`
4. Build the plugin with `--features dynamic-registration` and the runtime
   (`tomii-core`) with `--features dynamic-registration`; run with `--dylib <plugin.so>`.
   The runtime exits with an error if the table is missing or its ABI version differs.
