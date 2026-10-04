//! Rust `#[tomii_export]` kernel: adapter dispatch vs. native call.
//!
//! `wrappers`/`func_reg` below are the *actual* code `tomii-converter`
//! generates for a real plugin (`generate_from_file` is invoked from
//! `build.rs`, unmodified) — this is not a re-implementation, it is the
//! real generated dispatch path, `dlopen`+`dlsym`'d via `libloading` exactly
//! as `tomii-core`'s own `wrappers.rs` does.
//!
//! Run with `PLUGIN_LIB=<path to libkernel_rust.so>` set (mirrors the real
//! runtime's `PLUGIN_LIB` contract, set by `--dylib` in `tomii-core`'s CLI).

mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/rust_wrappers.rs"));
}
mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/rust_registry.rs"));
}

use std::hint::black_box;
use std::time::Instant;
use tomii_types::CmTypes;

/// Same logic as `kernel-rust::buf_sum`, compiled natively into this binary
/// (no CmTypes, no dlopen) — the "no boundary" baseline.
#[inline(never)]
fn buf_sum_native(buf: &[f32]) -> f32 {
    buf[0] + buf[buf.len() - 1]
}

fn bench<F: FnMut() -> f32>(iters: u32, mut f: F) -> f64 {
    for _ in 0..2_000 {
        black_box(f());
    }
    let start = Instant::now();
    for _ in 0..iters {
        black_box(f());
    }
    start.elapsed().as_nanos() as f64 / iters as f64
}

fn main() {
    assert!(
        std::env::var("PLUGIN_LIB").is_ok(),
        "set PLUGIN_LIB=<path to libkernel_rust.so>"
    );
    wrappers::init_wrappers();
    let adapter_fn = func_reg::get_func("buf_sum").expect("buf_sum missing from registry");

    // Correctness: adapter and native must agree before we trust the timing.
    let probe: Vec<f32> = (0..37).map(|i| i as f32).collect();
    let native_v = buf_sum_native(&probe);
    let adapter_v = match adapter_fn(&[CmTypes::from_any(probe.clone())]) {
        CmTypes::F32(v) => v,
        other => panic!("unexpected adapter return: {other:?}"),
    };
    assert!(
        (native_v - adapter_v).abs() < 1e-3,
        "native/adapter mismatch: {native_v} vs {adapter_v}"
    );

    const ITERS: u32 = 200_000;
    println!("elems,bytes,native_ns,adapter_ns,adapter_overhead_ns");
    for &n in &[64usize, 128, 256, 512, 1024, 2048, 4096] {
        let payload: Vec<f32> = (0..n).map(|i| i as f32 * 0.5).collect();
        let bytes = n * 4;

        let native_ref = payload.clone();
        let t_native = bench(ITERS, || buf_sum_native(black_box(&native_ref)));

        // Marshalled once per size (like the runtime handing an Arc'd result
        // buffer to the adapter); the per-call cost measured is dispatch +
        // with_any downcast, not the Arc construction.
        let args = [CmTypes::from_any(payload.clone())];
        let t_adapter = bench(ITERS, || match adapter_fn(black_box(&args)) {
            CmTypes::F32(v) => v,
            _ => unreachable!(),
        });

        println!(
            "{},{},{:.2},{:.2},{:.2}",
            n,
            bytes,
            t_native,
            t_adapter,
            t_adapter - t_native
        );
    }
}
