//! CmTypes adapter dispatch vs. Protocol Buffers serialization.
//!
//! Measures the per-call composition-boundary overhead for passing an f32
//! buffer to a kernel and returning a scalar, across payload sizes:
//!   - native:   direct call, no boundary (baseline)
//!   - adapter:  CmTypes universal-interface path (Arc passing + downcast),
//!               mirroring the generated Plugin Adapter code
//!   - protobuf: prost encode -> decode -> call -> encode result,
//!               mirroring a serialization-based boundary
//!
//! Overhead(path) = t(path) - t(native), reported in ns/call.

use prost::Message;
use std::any::Any;
use std::hint::black_box;
use std::time::Instant;
use tomii_types::CmTypes;

#[derive(Clone, PartialEq, Message)]
struct FloatVec {
    #[prost(float, repeated, tag = "1")]
    data: Vec<f32>,
}

#[derive(Clone, PartialEq, Message)]
struct Scalar {
    #[prost(float, tag = "1")]
    value: f32,
}

/// The kernel: cheap on purpose so the boundary dominates.
#[inline(never)]
fn kernel(data: &[f32]) -> f32 {
    data[0] + data[data.len() - 1]
}

/// Generated-adapter shape: extract native types from &[CmTypes], call, wrap.
#[inline(never)]
fn adapter_call(args: &[CmTypes]) -> CmTypes {
    let CmTypes::Any(cell) = &args[0] else {
        panic!("type mismatch")
    };
    let guard = cell.read();
    let data = (&**guard as &dyn Any)
        .downcast_ref::<Vec<f32>>()
        .expect("downcast");
    CmTypes::F32(kernel(data))
}

/// Serialization boundary: bytes in, bytes out.
#[inline(never)]
fn protobuf_call(encoded: &[u8]) -> Vec<u8> {
    let msg = FloatVec::decode(encoded).expect("decode");
    let out = Scalar {
        value: kernel(&msg.data),
    };
    out.encode_to_vec()
}

fn bench<F: FnMut() -> f32>(iters: u32, mut f: F) -> f64 {
    // warm-up
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
    const ITERS: u32 = 100_000;
    println!("elems,bytes,native_ns,adapter_ns,protobuf_ns,adapter_overhead_ns,protobuf_overhead_ns");
    for &n in &[16usize, 64, 256, 1024, 4096, 16384] {
        let payload: Vec<f32> = (0..n).map(|i| i as f32 * 0.5).collect();
        let bytes = n * 4;

        // native baseline
        let native_ref = payload.clone();
        let t_native = bench(ITERS, || kernel(black_box(&native_ref)));

        // adapter path: argument marshalled once per call, like the runtime
        // handing an Arc'd result buffer to the adapter.
        let args = [CmTypes::from_any(payload.clone())];
        let t_adapter = bench(ITERS, || {
            let CmTypes::F32(v) = adapter_call(black_box(&args)) else {
                unreachable!()
            };
            v
        });

        // protobuf path: sender-side encode + receiver-side decode + result encode.
        let msg = FloatVec {
            data: payload.clone(),
        };
        let t_protobuf = bench(ITERS, || {
            let encoded = black_box(&msg).encode_to_vec();
            let out = protobuf_call(black_box(&encoded));
            Scalar::decode(&out[..]).unwrap().value
        });

        println!(
            "{},{},{:.1},{:.1},{:.1},{:.1},{:.1}",
            n,
            bytes,
            t_native,
            t_adapter,
            t_protobuf,
            t_adapter - t_native,
            t_protobuf - t_native
        );
    }
}
