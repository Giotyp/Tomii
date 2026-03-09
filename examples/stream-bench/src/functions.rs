/// Generate a Vec<f64> of length n filled with fill value
pub fn generate_array(n: usize, fill: f64) -> Vec<f64> {
    vec![fill; n]
}

/// STREAM Copy: a[i] = b[i]
// b.to_vec() compiles to ptr::copy_nonoverlapping — already optimal.
pub fn stream_copy(b: &[f64]) -> Vec<f64> {
    b.to_vec()
}

/// STREAM Scale: a[i] = scalar * b[i]
// Unsafe raw-pointer loop: lets LLVM prove no-alias and emit AVX-512.
pub fn stream_scale(b: &[f64], scalar: f64) -> Vec<f64> {
    let n = b.len();
    let mut a = Vec::with_capacity(n);
    unsafe {
        let ap = a.as_mut_ptr() as *mut f64;
        let bp = b.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read() * scalar);
        }
        a.set_len(n);
    }
    a
}

/// STREAM Add: a[i] = b[i] + c[i]
pub fn stream_add(b: &[f64], c: &[f64]) -> Vec<f64> {
    let n = b.len();
    let mut a = Vec::with_capacity(n);
    unsafe {
        let ap = a.as_mut_ptr() as *mut f64;
        let bp = b.as_ptr();
        let cp = c.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read() + cp.add(i).read());
        }
        a.set_len(n);
    }
    a
}

/// STREAM Triad: a[i] = b[i] + scalar * c[i]
pub fn stream_triad(b: &[f64], c: &[f64], scalar: f64) -> Vec<f64> {
    let n = b.len();
    let mut a = Vec::with_capacity(n);
    unsafe {
        let ap = a.as_mut_ptr() as *mut f64;
        let bp = b.as_ptr();
        let cp = c.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read() + scalar * cp.add(i).read());
        }
        a.set_len(n);
    }
    a
}

/// Sink: consume result, return byte count processed
pub fn sink(result: &[f64]) -> usize {
    result.len() * std::mem::size_of::<f64>()
}

// ---------------------------------------------------------------------------
// Buffer-pool helpers for pre-allocated STREAM benchmarks
// ---------------------------------------------------------------------------

use std::sync::Mutex;

/// Create N pre-allocated read-only arrays, each filled with `fill`.
pub fn create_buffer_pool(n_workers: usize, array_size: usize, fill: f64) -> Vec<Vec<f64>> {
    (0..n_workers).map(|_| vec![fill; array_size]).collect()
}

/// Create N pre-allocated mutable arrays (zero-initialised), wrapped in Mutex.
pub fn create_mutable_buffer_pool(
    n_workers: usize,
    array_size: usize,
) -> Vec<Mutex<Vec<f64>>> {
    (0..n_workers)
        .map(|_| Mutex::new(vec![0.0f64; array_size]))
        .collect()
}

/// Generate a mutable Vec<f64> of length n, zero-initialised.
/// Used as an init-factor element: each worker gets its own buffer.
pub fn generate_mut_array(n: usize) -> Vec<f64> {
    vec![0.0f64; n]
}

/// STREAM Copy in-place: a[i] = b[i]
pub fn stream_copy_pooled(a: &mut Vec<f64>, b: &Vec<f64>) {
    let n = b.len();
    unsafe {
        let ap = a.as_mut_ptr();
        let bp = b.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read());
        }
    }
}

/// STREAM Scale in-place: a[i] = scalar * b[i]
pub fn stream_scale_pooled(a: &mut Vec<f64>, b: &Vec<f64>, scalar: f64) {
    let n = b.len();
    unsafe {
        let ap = a.as_mut_ptr();
        let bp = b.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read() * scalar);
        }
    }
}

/// STREAM Add in-place: a[i] = b[i] + c[i]
pub fn stream_add_pooled(a: &mut Vec<f64>, b: &Vec<f64>, c: &Vec<f64>) {
    let n = b.len();
    unsafe {
        let ap = a.as_mut_ptr();
        let bp = b.as_ptr();
        let cp = c.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read() + cp.add(i).read());
        }
    }
}

/// STREAM Triad in-place: a[i] = b[i] + scalar * c[i]
pub fn stream_triad_pooled(a: &mut Vec<f64>, b: &Vec<f64>, c: &Vec<f64>, scalar: f64) {
    let n = b.len();
    unsafe {
        let ap = a.as_mut_ptr();
        let bp = b.as_ptr();
        let cp = c.as_ptr();
        for i in 0..n {
            ap.add(i).write(bp.add(i).read() + scalar * cp.add(i).read());
        }
    }
}
