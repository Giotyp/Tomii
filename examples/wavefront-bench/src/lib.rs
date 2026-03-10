pub mod wavefront;

use wavefront::{compute_cell, init_grid};
use synstream_types::CmTypes;

/// Compute a tile of consecutive cells on anti-diagonal `diag` starting at `tile_idx`.
///
/// Replaces `wf_cell_cm` when tile coarsening is enabled.  Each task covers
/// `tile_size` cells: indices `tile_idx * tile_size` through
/// `min((tile_idx + 1) * tile_size, width) - 1`.
///
/// Arguments (via wrapper args array):
///   args[0] = grid      (CmTypes::Any(Vec<f64>))
///   args[1] = n         (CmTypes::Usize)
///   args[2] = diag      (CmTypes::Usize)
///   args[3] = tile_idx  (CmTypes::Usize — resolved from $index at runtime)
///   args[4] = tile_size (CmTypes::Usize — compile-time constant from graph)
///   args[5] = barrier   (CmTypes::None  — ignored, sync only)
///
/// Returns: CmTypes::None
#[no_mangle]
pub fn wf_tile_cm(grid: &CmTypes, n: usize, diag: usize, tile_idx: usize, tile_size: usize) -> CmTypes {
    let width = (diag + 1).min(n).min(2 * n - 1 - diag);
    let start = tile_idx * tile_size;
    let end   = (start + tile_size).min(width);

    let data_ptr = grid
        .with_any(|v: &Vec<f64>| v.as_ptr() as *mut f64)
        .expect("wf_tile_cm: expected CmTypes::Any(Vec<f64>) for grid");

    // SAFETY: same invariants as wf_cell_cm — non-overlapping writes within a
    // diagonal, $barrier guarantees all (diag-1) writes are complete first.
    for idx in start..end {
        unsafe { compute_cell(data_ptr, n, diag, idx) };
    }

    CmTypes::None
}

/// Initialise the N×N wavefront grid (called once at graph initialisation).
///
/// Argument: n_cm = CmTypes::Usize(N)
/// Returns:  CmTypes::Any(Vec<f64>) — the N×N grid with boundary values.
#[no_mangle]
pub fn init_grid_cm(n_cm: &CmTypes) -> CmTypes {
    let n = match n_cm {
        CmTypes::Usize(x) => *x,
        _ => panic!("init_grid_cm: expected CmTypes::Usize for n"),
    };
    CmTypes::from_any(init_grid(n))
}

/// Compute one cell on anti-diagonal `diag` at position `idx` (= $index).
///
/// Writes to `grid[i*n + j]` in-place via a raw pointer.  Non-overlapping
/// writes within a diagonal are safe; the $barrier on the previous diagonal
/// guarantees all (diag-1) cells are complete before any diag cell starts.
///
/// Arguments (via wrapper args array):
///   args[0] = grid (CmTypes::Any(Vec<f64>))
///   args[1] = n    (CmTypes::Usize)
///   args[2] = diag (CmTypes::Usize)
///   args[3] = idx  (CmTypes::Usize — resolved from $index at runtime)
///   args[4] = barrier value from previous diagonal (CmTypes::None — ignored)
///
/// Returns: CmTypes::None  (in-place update; no result to propagate as data)
#[no_mangle]
pub fn wf_cell_cm(grid: &CmTypes, n: usize, diag: usize, idx: usize) -> CmTypes {
    // Acquire a brief read lock to get the raw pointer; release before writing.
    // The read lock is compatible with concurrent callers on the same diagonal
    // (all writers target non-overlapping cells).
    let data_ptr = grid
        .with_any(|v: &Vec<f64>| v.as_ptr() as *mut f64)
        .expect("wf_cell_cm: expected CmTypes::Any(Vec<f64>) for grid");

    // SAFETY:
    // 1. Instances on the same diagonal write to non-overlapping (i,j) cells.
    // 2. The $barrier dependency on the previous diagonal ensures all writes
    //    from diagonal (diag-1) are visible before any instance of diag runs.
    // 3. The Vec<f64> allocation is stable for the lifetime of the run
    //    (grid is a $ref init object, never reallocated).
    unsafe { compute_cell(data_ptr, n, diag, idx) };

    CmTypes::None
}
