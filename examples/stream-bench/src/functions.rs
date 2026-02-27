/// Generate a Vec<f64> of length n filled with fill value
pub fn generate_array(n: usize, fill: f64) -> Vec<f64> {
    vec![fill; n]
}

/// STREAM Copy: a[i] = b[i]
pub fn stream_copy(b: &[f64]) -> Vec<f64> {
    b.to_vec()
}

/// STREAM Scale: a[i] = scalar * b[i]
pub fn stream_scale(b: &[f64], scalar: f64) -> Vec<f64> {
    b.iter().map(|x| scalar * x).collect()
}

/// STREAM Add: a[i] = b[i] + c[i]
pub fn stream_add(b: &[f64], c: &[f64]) -> Vec<f64> {
    b.iter().zip(c.iter()).map(|(x, y)| x + y).collect()
}

/// STREAM Triad: a[i] = b[i] + scalar * c[i]
pub fn stream_triad(b: &[f64], c: &[f64], scalar: f64) -> Vec<f64> {
    b.iter().zip(c.iter()).map(|(x, y)| x + scalar * y).collect()
}

/// Sink: consume result, return byte count processed
pub fn sink(result: &[f64]) -> usize {
    result.len() * std::mem::size_of::<f64>()
}
