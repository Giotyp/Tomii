/// Initialise an N×N wavefront grid with boundary values.
///
/// Boundary conditions:
///   grid[0][j] = (j + 1) as f64   (top row)
///   grid[i][0] = (i + 1) as f64   (left column)
///   interior   = 0.0
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

/// Compute one interior cell on anti-diagonal `diag` at position `idx`.
///
/// Anti-diagonal d contains all cells (i, j) with i + j = d.
/// Position `idx` within diagonal d maps to:
///   i = min(d, n-1) - idx
///   j = d - i
///
/// Boundary cells (i == 0 or j == 0) are pre-initialised and skipped.
///
/// Cell update: grid[i][j] = 0.5 * (grid[i-1][j] + grid[i][j-1])
///
/// # Safety
/// Caller must ensure:
/// - All instances on diagonal `diag` write to non-overlapping cells.
/// - All instances of diagonal `diag - 1` have fully completed before any
///   instance of `diag` executes (guaranteed by the $barrier dependency).
pub unsafe fn compute_cell(grid: *mut f64, n: usize, diag: usize, idx: usize) {
    let i = diag.min(n - 1) - idx;
    let j = diag - i;
    if i == 0 || j == 0 {
        return; // boundary — pre-initialised, no computation needed
    }
    let left = *grid.add(i * n + (j - 1));
    let top  = *grid.add((i - 1) * n + j);
    *grid.add(i * n + j) = 0.5 * (left + top);
}

/// Verify correctness for a completed sweep (used with small N for sanity checks).
///
/// Returns true if every interior cell satisfies the wavefront recurrence
/// within a relative tolerance of 1e-9.
#[allow(dead_code)]
pub fn verify_grid(grid: &[f64], n: usize) -> bool {
    for i in 1..n {
        for j in 1..n {
            let expected = 0.5 * (grid[i * n + (j - 1)] + grid[(i - 1) * n + j]);
            let actual = grid[i * n + j];
            let rel_err = (expected - actual).abs() / expected.abs().max(1e-30);
            if rel_err > 1e-9 {
                eprintln!("verify_grid: mismatch at ({},{}) expected={} actual={}", i, j, expected, actual);
                return false;
            }
        }
    }
    true
}
