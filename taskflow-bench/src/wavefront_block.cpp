//! Taskflow 2D Block-DAG wavefront benchmark.
//!
//! Same computation as wavefront.cpp but uses a per-block dependency graph:
//!   block(i,j) precede block(i+1,j)   (top → bottom)
//!   block(i,j) precede block(i,j+1)   (left → right)
//!
//! This matches the Taskflow reference implementation at
//! https://taskflow.github.io/taskflow/wavefront.html
//! and avoids global anti-diagonal barrier synchronisation points.
//!
//! Each B×B block processes a tile_size × tile_size region of the N×N grid.
//! For N=512, tile_size=32: B=16, 256 nodes total.
//!
//! Usage:
//!   ./wavefront_block --n 512 --tile-size 32 --workers 4 --iterations 20 \
//!                     --output tf_block.csv [--pin]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cmath>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>

#include <sched.h>

#include <taskflow/taskflow.hpp>

// ---------------------------------------------------------------------------
// CSV helper
// ---------------------------------------------------------------------------

static void append_wavefront_csv(const std::string& path,
                                  const std::string& system,
                                  int                n,
                                  int                workers,
                                  int                iterations,
                                  double             total_s,
                                  double             s_per_iter)
{
    bool write_header = false;
    {
        std::ifstream f(path);
        write_header = !f.good();
    }
    std::ofstream f(path, std::ios::app);
    if (write_header)
        f << "system,n,workers,iterations,total_s,s_per_iter\n";
    f << system << ',' << n << ',' << workers << ','
      << iterations << ',' << total_s << ',' << s_per_iter << '\n';
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cli {
    int         n          = 512;
    int         tile_size  = 32;
    int         workers    = 1;
    int         iterations = 20;
    int         warmup     = 3;
    std::string output     = "tf_block_wavefront.csv";
    bool        pin        = false;
};

static Cli parse_args(int argc, char** argv) {
    Cli c;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto val = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + a);
            return argv[++i];
        };
        if      (a == "--n")          c.n          = std::stoi(val());
        else if (a == "--tile-size")  c.tile_size  = std::stoi(val());
        else if (a == "--workers")    c.workers    = std::stoi(val());
        else if (a == "--iterations") c.iterations = std::stoi(val());
        else if (a == "--warmup")     c.warmup     = std::stoi(val());
        else if (a == "--output")     c.output     = val();
        else if (a == "--pin")        c.pin        = true;
        else throw std::runtime_error("Unknown flag: " + a);
    }
    return c;
}

// ---------------------------------------------------------------------------
// CPU pinning for Taskflow worker threads
// ---------------------------------------------------------------------------

static void pin_workers(tf::Executor& executor, int base_core) {
    const size_t nw = executor.num_workers();
    for (size_t i = 0; i < nw; ++i) {
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(base_core + static_cast<int>(i), &cs);
        auto handle = executor.async([cs]() mutable {
            sched_setaffinity(0, sizeof(cs), &cs);
        });
        handle.get();
    }
}

// ---------------------------------------------------------------------------
// Block kernel: compute one tile_size × tile_size block of the grid.
// Intra-block dependency order: row-major (top-left to bottom-right).
// Cross-block dependencies guaranteed by the DAG $res edges.
// ---------------------------------------------------------------------------

static void wf_block_kernel(double* grid, int n, int block_row, int block_col,
                             int tile_size)
{
    const int row_start = block_row * tile_size;
    const int col_start = block_col * tile_size;
    const int row_end   = std::min(row_start + tile_size, n);
    const int col_end   = std::min(col_start + tile_size, n);

    for (int i = row_start; i < row_end; ++i) {
        for (int j = col_start; j < col_end; ++j) {
            if (i == 0 || j == 0) continue;  // boundary — skip
            grid[i * n + j] = 0.5 * (grid[i * n + (j - 1)]
                                    + grid[(i - 1) * n + j]);
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Cli cli = parse_args(argc, argv);

    const int N     = cli.n;
    const int T     = cli.tile_size;
    const int W     = cli.workers;
    const int ITERS = cli.iterations;
    const int WARM  = cli.warmup;
    const int B     = (N + T - 1) / T;   // number of blocks per dimension

    std::printf("Taskflow Block-DAG Wavefront N=%d tile=%d B=%dx%d workers=%d\n",
                N, T, B, B, W);

    // Pre-allocate N×N grid with boundary values
    std::vector<double> grid(static_cast<std::size_t>(N) * N, 0.0);
    for (int j = 0; j < N; ++j) grid[j]        = static_cast<double>(j + 1);  // row 0
    for (int i = 1; i < N; ++i) grid[i * N]     = static_cast<double>(i + 1); // col 0

    tf::Executor executor(W);
    if (cli.pin) pin_workers(executor, 1);

    // Build 2D block-DAG once — reused across iterations.
    // tiles[i][j] precede tiles[i+1][j] (top→bottom) and tiles[i][j+1] (left→right).
    tf::Taskflow taskflow;
    double* grid_ptr = grid.data();

    std::vector<std::vector<tf::Task>> tiles(B, std::vector<tf::Task>(B));
    for (int i = 0; i < B; ++i) {
        for (int j = 0; j < B; ++j) {
            tiles[i][j] = taskflow.emplace([grid_ptr, N, T, i, j] {
                wf_block_kernel(grid_ptr, N, i, j, T);
            });
            if (i > 0) tiles[i-1][j].precede(tiles[i][j]);   // top neighbour
            if (j > 0) tiles[i][j-1].precede(tiles[i][j]);   // left neighbour
        }
    }

    auto run_sweep = [&]() {
        executor.run(taskflow).get();
    };

    // Warm-up sweeps (untimed)
    for (int w = 0; w < WARM; ++w) run_sweep();

    // Timed sweeps
    double total_s = 0.0;
    for (int iter = 0; iter < ITERS; ++iter) {
        auto t0 = std::chrono::high_resolution_clock::now();
        run_sweep();
        auto t1 = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(t1 - t0).count();
        total_s += elapsed;
        std::printf("  iter %2d: %.4fs\n", iter + 1, elapsed);
    }

    double s_per_iter = total_s / ITERS;
    const std::string system_label =
        cli.pin ? "taskflow_block_pinned" : "taskflow_block";
    std::printf(
        "%s | n=%d tile=%d | workers=%d | iters=%d | total=%.4fs | %.4fs/iter\n",
        system_label.c_str(), N, T, W, ITERS, total_s, s_per_iter);

    append_wavefront_csv(cli.output, system_label, N, W, ITERS, total_s, s_per_iter);
    return 0;
}
