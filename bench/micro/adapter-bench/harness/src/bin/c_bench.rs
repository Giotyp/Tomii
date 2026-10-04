//! C kernel wrapped by `tomii-converter`'s C-header path: adapter dispatch
//! (libloading dynamic dispatch, per the `c_header.rs` doc comment) vs. a
//! statically-linked native FFI call.
//!
//! `wrappers`/`func_reg` are the actual generated code (`generate_from_file`
//! run unmodified from `build.rs` against `kernel-c/include/kernel_c.h`).
//!
//! Run with `PLUGIN_LIB=<path to libkernel_c.so>` set.

mod wrappers {
    include!(concat!(env!("OUT_DIR"), "/c_wrappers.rs"));
}
mod func_reg {
    include!(concat!(env!("OUT_DIR"), "/c_registry.rs"));
}

use std::hint::black_box;
use std::time::Instant;
use tomii_types::CmTypes;

// Statically linked (via build.rs `cc::Build`) — no dlopen, no CmTypes.
extern "C" {
    fn buf_sum_c(buf: *const f32, buf_len: usize) -> f32;
}

#[inline(never)]
fn buf_sum_native(buf: &[f32]) -> f32 {
    unsafe { buf_sum_c(buf.as_ptr(), buf.len()) }
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
        "set PLUGIN_LIB=<path to libkernel_c.so>"
    );
    wrappers::init_wrappers();
    let adapter_fn = func_reg::get_func("buf_sum_c").expect("buf_sum_c missing from registry");

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
