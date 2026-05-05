//! Intel TBB flow_graph 2D Block-DAG Wavefront benchmark.
//!
//! Uses tbb::flow::continue_node per tile with explicit left/top dependencies:
//!   tile(i,j) -> tile(i+1,j)  (top → bottom)
//!   tile(i,j) -> tile(i,j+1)  (left → right)
//!
//! The DAG topology matches the Taskflow wavefront_block reference implementation.
//! Graph is built once and reset between sweeps via graph::reset().
//!
//! Worker count is controlled via tbb::global_control (limits TBB's global pool
//! to W threads total including the main thread), which is the native mechanism
//! for flow_graph since its tasks bypass task_arena and go to the global pool.
//! Pinning is omitted: arena-bound PinningObserver does not fire for global-pool
//! threads, and a global observer cannot selectively pin exactly W threads.
//!
//! Usage:
//!   ./wavefront_flow --n 512 --tile-size 32 --workers 4 --iterations 20 \
//!                   --output tbb_flow_wavefront.csv

#include "common.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>

#include <oneapi/tbb/flow_graph.h>
#include <oneapi/tbb/global_control.h>

namespace flow = tbb::flow;

// ---------------------------------------------------------------------------
// CSV helper — same schema as wavefront.cpp
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
// Block kernel — identical to taskflow-bench/src/wavefront_block.cpp
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
            if (i == 0 || j == 0) continue;
            grid[i * n + j] = 0.5 * (grid[i * n + (j - 1)]
                                    + grid[(i - 1) * n + j]);
        }
    }
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
    std::string output     = "tbb_flow_wavefront.csv";
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
        else throw std::runtime_error("Unknown flag: " + a);
    }
    return c;
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
    const int B     = (N + T - 1) / T;

    std::printf("TBB flow_graph Block-DAG Wavefront N=%d tile=%d B=%dx%d workers=%d\n",
                N, T, B, B, W);

    std::vector<double> grid(static_cast<std::size_t>(N) * N, 0.0);
    for (int j = 0; j < N; ++j) grid[j]        = static_cast<double>(j + 1);
    for (int i = 1; i < N; ++i) grid[i * N]     = static_cast<double>(i + 1);
    double* grid_ptr = grid.data();

    // Limit TBB's global pool to W threads (main thread counts as 1,
    // so this gives W-1 worker threads + 1 main = W total, matching
    // the task_arena(W) parallelism used by parallel_for and Taskflow).
    tbb::global_control gc(tbb::global_control::max_allowed_parallelism, W);

    // Build the flow graph once — reused across all sweeps via g.reset().
    using cnode = flow::continue_node<flow::continue_msg>;
    flow::graph g;

    flow::broadcast_node<flow::continue_msg> start(g);

    // Allocate all B×B nodes before wiring edges.
    std::vector<std::vector<std::unique_ptr<cnode>>> tiles(B);
    for (int i = 0; i < B; ++i) {
        tiles[i].resize(B);
        for (int j = 0; j < B; ++j) {
            tiles[i][j] = std::make_unique<cnode>(
                g,
                [grid_ptr, N, T, i, j](flow::continue_msg) {
                    wf_block_kernel(grid_ptr, N, i, j, T);
                }
            );
        }
    }

    flow::make_edge(start, *tiles[0][0]);
    for (int i = 0; i < B; ++i) {
        for (int j = 0; j < B; ++j) {
            if (i > 0) flow::make_edge(*tiles[i-1][j], *tiles[i][j]);
            if (j > 0) flow::make_edge(*tiles[i][j-1], *tiles[i][j]);
        }
    }

    auto run_sweep = [&]() {
        g.reset();
        start.try_put(flow::continue_msg{});
        g.wait_for_all();
    };

    for (int w = 0; w < WARM; ++w) run_sweep();

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
    std::printf(
        "tbb_flow | n=%d tile=%d | workers=%d | iters=%d | total=%.4fs | %.4fs/iter\n",
        N, T, W, ITERS, total_s, s_per_iter);

    append_wavefront_csv(cli.output, "tbb_flow", N, W, ITERS, total_s, s_per_iter);
    return 0;
}
