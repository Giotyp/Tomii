pub mod functions;

use functions::*;
use synstream_types::CmTypes;

#[no_mangle]
pub fn generate_array_cm(n: usize, fill: f64) -> CmTypes {
    CmTypes::from_any(generate_array(n, fill))
}

#[no_mangle]
pub fn stream_copy_cm(b: &CmTypes) -> CmTypes {
    b.with_any(|v: &Vec<f64>| CmTypes::from_any(stream_copy(v)))
        .expect("stream_copy_cm: expected Vec<f64>")
}

#[no_mangle]
pub fn stream_scale_cm(b: &CmTypes, scalar: f64) -> CmTypes {
    b.with_any(|v: &Vec<f64>| CmTypes::from_any(stream_scale(v, scalar)))
        .expect("stream_scale_cm: expected Vec<f64>")
}

#[no_mangle]
pub fn stream_add_cm(b: &CmTypes, c: &CmTypes) -> CmTypes {
    b.with_any(|bv: &Vec<f64>| {
        c.with_any(|cv: &Vec<f64>| CmTypes::from_any(stream_add(bv, cv)))
            .expect("stream_add_cm: expected Vec<f64> for c")
    })
    .expect("stream_add_cm: expected Vec<f64> for b")
}

#[no_mangle]
pub fn stream_triad_cm(b: &CmTypes, c: &CmTypes, scalar: f64) -> CmTypes {
    b.with_any(|bv: &Vec<f64>| {
        c.with_any(|cv: &Vec<f64>| CmTypes::from_any(stream_triad(bv, cv, scalar)))
            .expect("stream_triad_cm: expected Vec<f64> for c")
    })
    .expect("stream_triad_cm: expected Vec<f64> for b")
}

#[no_mangle]
pub fn sink_cm(result: &CmTypes) -> CmTypes {
    match result {
        CmTypes::None => CmTypes::None,
        _ => {
            result
                .with_any(|v: &Vec<f64>| CmTypes::Usize(sink(v)))
                .unwrap_or(CmTypes::None)
        }
    }
}
