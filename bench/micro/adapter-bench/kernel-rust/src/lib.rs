//! Rust kernel for the adapter-overhead microbenchmark (item 3).
//!
//! `#[tomii_export]` emits `buf_sum_cm(buf: &CmTypes) -> CmTypes` alongside
//! the original `buf_sum`. Built as a `dylib` (same crate-type as every
//! real Tomii plugin, see `bench/anti-diag-bench/tomii/Cargo.toml`) so the
//! harness can `dlopen` it via `libloading` exactly as `tomii-core`'s
//! generated `wrappers.rs` does, and also linked as an `rlib` so the
//! harness can call `buf_sum` directly for the native baseline.

#![allow(improper_ctypes_definitions)]

use tomii_macro::tomii_export;

/// O(1) kernel (first + last element) so the per-call cost is flat in the
/// buffer size and isolates dispatch/marshalling overhead from compute —
/// same methodology as `bench/micro/cmtypes-vs-protobuf`'s `kernel()`.
#[tomii_export]
pub fn buf_sum(buf: &Vec<f32>) -> f32 {
    buf[0] + buf[buf.len() - 1]
}
