/// Initialise an N×N wavefront grid with boundary values.
/// Matches the reference kernel in agent-bench/references/synstream/src/wavefront.rs.
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

/// Compute one cell. Boundary cells (i==0 or j==0) are pre-initialised; this is a no-op for them.
///
/// # Safety
/// Caller guarantees that no two tasks for the same sweep share a cell,
/// and that all predecessors of (i,j) on diagonal d-1 have completed before this runs.
#[inline(always)]
pub unsafe fn compute_cell(grid: *mut f64, n: usize, task_id: usize) {
    let i = task_id / n;
    let j = task_id % n;
    if i == 0 || j == 0 {
        return;
    }
    let left = *grid.add(i * n + (j - 1));
    let top = *grid.add((i - 1) * n + j);
    *grid.add(i * n + j) = 0.5 * (left + top);
}

pub fn verify_grid(grid: &[f64], n: usize) -> bool {
    for i in 1..n {
        for j in 1..n {
            let expected = 0.5 * (grid[i * n + (j - 1)] + grid[(i - 1) * n + j]);
            let actual = grid[i * n + j];
            let rel_err = (expected - actual).abs() / expected.abs().max(1e-30);
            if rel_err > 1e-9 {
                eprintln!(
                    "verify_grid: mismatch at ({i},{j}) expected={expected} actual={actual}"
                );
                return false;
            }
        }
    }
    true
}
