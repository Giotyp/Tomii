#![allow(improper_ctypes_definitions)]

use tomii_macro::tomii_export;

/// Initialise an N×N wavefront grid with boundary values.
#[tomii_export]
pub fn init_grid(n: usize) -> Vec<f64> {
    let mut grid = vec![0.0f64; n * n];
    for j in 0..n {
        grid[j] = (j + 1) as f64;
    }
    for i in 1..n {
        grid[i * n] = (i + 1) as f64;
    }
    grid
}

/// One cell on anti-diagonal `diag` at position `idx` — per-cell dispatch path.
///
/// grid[i][j] = 0.5*(grid[i-1][j] + grid[i][j-1])
///
/// The `$barrier` arg from the previous diagonal lands as the last element
/// of the args slice and is simply not extracted here.
#[tomii_export]
pub fn wf_cell(grid: &Vec<f64>, n: usize, diag: usize, idx: usize) {
    let ptr = grid.as_ptr() as *mut f64;
    let i = diag.min(n - 1) - idx;
    let j = diag - i;
    if i > 0 && j > 0 {
        // SAFETY: non-overlapping writes within a diagonal; $barrier ensures
        // all (diag-1) writes complete before any instance of diag runs.
        unsafe {
            let left = *ptr.add(i * n + (j - 1));
            let top = *ptr.add((i - 1) * n + j);
            *ptr.add(i * n + j) = 0.5 * (left + top);
        }
    }
}

/// Bulk variant of wf_cell — uses the Tier 4 single-call fast path.
///
/// Identical compute to wf_cell; `#[tomii_export(bulk)]` causes the macro to
/// emit a `wf_cell_bulk_bulk_cm` companion that is called once per diagonal
/// task with the full `start..end` range instead of once per cell.
#[tomii_export(bulk)]
pub fn wf_cell_bulk(grid: &Vec<f64>, n: usize, diag: usize, idx: usize) {
    let ptr = grid.as_ptr() as *mut f64;
    let i = diag.min(n - 1) - idx;
    let j = diag - i;
    if i > 0 && j > 0 {
        unsafe {
            let left = *ptr.add(i * n + (j - 1));
            let top = *ptr.add((i - 1) * n + j);
            *ptr.add(i * n + j) = 0.5 * (left + top);
        }
    }
}
