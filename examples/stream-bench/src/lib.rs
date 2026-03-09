pub mod functions;

use functions::*;
#[allow(unused_imports)]
use std::sync::Mutex;
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

// ---------------------------------------------------------------------------
// Init-pooled _cm wrappers (per-worker buffers via init factor, no locks)
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn generate_mut_array_cm(n: usize) -> CmTypes {
    CmTypes::from_any(generate_mut_array(n))
}

#[no_mangle]
pub fn stream_copy_init_pooled_cm(a: &CmTypes, b: &CmTypes) -> CmTypes {
    a.with_any_mut(|av: &mut Vec<f64>| {
        b.with_any(|bv: &Vec<f64>| {
            stream_copy_pooled(av, bv);
        })
        .expect("stream_copy_init_pooled_cm: expected Vec<f64> for b");
    })
    .expect("stream_copy_init_pooled_cm: expected Vec<f64> for a");
    CmTypes::None
}

#[no_mangle]
pub fn stream_scale_init_pooled_cm(a: &CmTypes, b: &CmTypes, scalar: f64) -> CmTypes {
    a.with_any_mut(|av: &mut Vec<f64>| {
        b.with_any(|bv: &Vec<f64>| {
            stream_scale_pooled(av, bv, scalar);
        })
        .expect("stream_scale_init_pooled_cm: expected Vec<f64> for b");
    })
    .expect("stream_scale_init_pooled_cm: expected Vec<f64> for a");
    CmTypes::None
}

#[no_mangle]
pub fn stream_add_init_pooled_cm(a: &CmTypes, b: &CmTypes, c: &CmTypes) -> CmTypes {
    a.with_any_mut(|av: &mut Vec<f64>| {
        b.with_any(|bv: &Vec<f64>| {
            c.with_any(|cv: &Vec<f64>| {
                stream_add_pooled(av, bv, cv);
            })
            .expect("stream_add_init_pooled_cm: expected Vec<f64> for c");
        })
        .expect("stream_add_init_pooled_cm: expected Vec<f64> for b");
    })
    .expect("stream_add_init_pooled_cm: expected Vec<f64> for a");
    CmTypes::None
}

#[no_mangle]
pub fn stream_triad_init_pooled_cm(
    a: &CmTypes,
    b: &CmTypes,
    c: &CmTypes,
    scalar: f64,
) -> CmTypes {
    a.with_any_mut(|av: &mut Vec<f64>| {
        b.with_any(|bv: &Vec<f64>| {
            c.with_any(|cv: &Vec<f64>| {
                stream_triad_pooled(av, bv, cv, scalar);
            })
            .expect("stream_triad_init_pooled_cm: expected Vec<f64> for c");
        })
        .expect("stream_triad_init_pooled_cm: expected Vec<f64> for b");
    })
    .expect("stream_triad_init_pooled_cm: expected Vec<f64> for a");
    CmTypes::None
}

// ---------------------------------------------------------------------------
// Buffer-pool _cm wrappers
// ---------------------------------------------------------------------------

#[no_mangle]
pub fn create_buffer_pool_cm(n_workers: usize, array_size: usize, fill: f64) -> CmTypes {
    CmTypes::from_any(create_buffer_pool(n_workers, array_size, fill))
}

#[no_mangle]
pub fn create_mutable_buffer_pool_cm(n_workers: usize, array_size: usize) -> CmTypes {
    CmTypes::from_any(create_mutable_buffer_pool(n_workers, array_size))
}

#[no_mangle]
pub fn stream_copy_pooled_cm(all_a: &CmTypes, all_b: &CmTypes, idx: usize) -> CmTypes {
    all_a
        .with_any(|a_pool: &Vec<Mutex<Vec<f64>>>| {
            all_b
                .with_any(|b_pool: &Vec<Vec<f64>>| {
                    let mut a = a_pool[idx].lock().unwrap();
                    stream_copy_pooled(&mut a, &b_pool[idx]);
                })
                .unwrap()
        })
        .expect("stream_copy_pooled_cm: type error");
    CmTypes::None
}

#[no_mangle]
pub fn stream_scale_pooled_cm(
    all_a: &CmTypes,
    all_b: &CmTypes,
    idx: usize,
    scalar: f64,
) -> CmTypes {
    all_a
        .with_any(|a_pool: &Vec<Mutex<Vec<f64>>>| {
            all_b
                .with_any(|b_pool: &Vec<Vec<f64>>| {
                    let mut a = a_pool[idx].lock().unwrap();
                    stream_scale_pooled(&mut a, &b_pool[idx], scalar);
                })
                .unwrap()
        })
        .expect("stream_scale_pooled_cm: type error");
    CmTypes::None
}

#[no_mangle]
pub fn stream_add_pooled_cm(
    all_a: &CmTypes,
    all_b: &CmTypes,
    all_c: &CmTypes,
    idx: usize,
) -> CmTypes {
    all_a
        .with_any(|a_pool: &Vec<Mutex<Vec<f64>>>| {
            all_b
                .with_any(|b_pool: &Vec<Vec<f64>>| {
                    all_c
                        .with_any(|c_pool: &Vec<Vec<f64>>| {
                            let mut a = a_pool[idx].lock().unwrap();
                            stream_add_pooled(&mut a, &b_pool[idx], &c_pool[idx]);
                        })
                        .unwrap()
                })
                .unwrap()
        })
        .expect("stream_add_pooled_cm: type error");
    CmTypes::None
}

#[no_mangle]
pub fn stream_triad_pooled_cm(
    all_a: &CmTypes,
    all_b: &CmTypes,
    all_c: &CmTypes,
    idx: usize,
    scalar: f64,
) -> CmTypes {
    all_a
        .with_any(|a_pool: &Vec<Mutex<Vec<f64>>>| {
            all_b
                .with_any(|b_pool: &Vec<Vec<f64>>| {
                    all_c
                        .with_any(|c_pool: &Vec<Vec<f64>>| {
                            let mut a = a_pool[idx].lock().unwrap();
                            stream_triad_pooled(&mut a, &b_pool[idx], &c_pool[idx], scalar);
                        })
                        .unwrap()
                })
                .unwrap()
        })
        .expect("stream_triad_pooled_cm: type error");
    CmTypes::None
}
