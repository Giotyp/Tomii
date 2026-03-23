pub mod wavefront;

use wavefront::compute_cell;
use synstream_macro::synstream_export;
use synstream_types::CmTypes;

/// Write grid[N-1][N-1] to "wf_corner.txt" in the current working directory.
///
/// Called as a single terminal node in the wavefront graph after the last
/// diagonal barrier.  The Python wrapper reads this file and passes the value
/// to the verifier with `--corner VALUE`.
///
/// Arguments (via wrapper args array):
///   args[0] = grid    (CmTypes::Any(Vec<f64>))
///   args[1] = n       (CmTypes::Usize)
///   args[2] = barrier from last diagonal (CmTypes::None — ignored, sync only)
///
/// Returns: CmTypes::None
#[no_mangle]
pub fn print_corner_cm(grid: &CmTypes, n: usize, _barrier: &CmTypes) -> CmTypes {
    let corner = grid
        .with_any(|v: &Vec<f64>| v[(n - 1) * n + (n - 1)])
        .expect("print_corner_cm: expected CmTypes::Any(Vec<f64>) for grid");
    std::fs::write("wf_corner.txt", format!("{:.15e}\n", corner))
        .expect("print_corner_cm: failed to write wf_corner.txt");
    CmTypes::None
}

/// Initialise the N×N wavefront grid (called once at graph initialisation).
///
/// Returns the N×N grid with boundary values.
#[synstream_export]
pub fn init_grid(n: usize) -> Vec<f64> {
    wavefront::init_grid(n)
}

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

/// Compute a T×T block of cells starting at grid row `block_row*tile_size`,
/// col `block_col*tile_size`.  Used for the 2D block DAG wavefront variant.
///
/// Arguments (via wrapper args array):
///   args[0] = grid       (CmTypes::Any(Vec<f64>))
///   args[1] = n          (CmTypes::Usize)
///   args[2] = block_row  (CmTypes::Usize — compile-time constant from graph)
///   args[3] = block_col  (CmTypes::Usize — compile-time constant from graph)
///   args[4] = tile_size  (CmTypes::Usize — compile-time constant from graph)
///   args[5..] = $res sync signals from left/top neighbours (CmTypes::None, ignored)
///
/// Returns: CmTypes::None
#[no_mangle]
pub fn wf_block_cm(grid: &CmTypes, n: usize, block_row: usize, block_col: usize, tile_size: usize) -> CmTypes {
    let data_ptr = grid
        .with_any(|v: &Vec<f64>| v.as_ptr() as *mut f64)
        .expect("wf_block_cm: expected CmTypes::Any(Vec<f64>) for grid");

    let row_start = block_row * tile_size;
    let col_start = block_col * tile_size;
    let row_end   = (row_start + tile_size).min(n);
    let col_end   = (col_start + tile_size).min(n);

    for i in row_start..row_end {
        for j in col_start..col_end {
            if i == 0 || j == 0 {
                continue; // boundary cells — pre-initialised, skip
            }
            unsafe {
                let left = *data_ptr.add(i * n + (j - 1));
                let top  = *data_ptr.add((i - 1) * n + j);
                *data_ptr.add(i * n + j) = 0.5 * (left + top);
            }
        }
    }

    CmTypes::None
}
