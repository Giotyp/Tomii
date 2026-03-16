//! Intel TBB Wavefront benchmark.
//!
//! Computes iterative anti-diagonal wavefront on an N×N grid:
//!   grid[i][j] = 0.5 * (grid[i-1][j] + grid[i][j-1])
//!
//! Boundary conditions (pre-allocated, never overwritten):
//!   grid[0][j] = (j+1)   (top row)
//!   grid[i][0] = (i+1)   (left column)
//!
//! Parallelism: tbb::parallel_for per anti-diagonal.  The parallel_for
//! completes before the next diagonal begins (implicit barrier).
//!
//! Usage:
//!   ./wavefront --n 512 --workers 4 --iterations 20 --output tbb_wavefront.csv [--pin]

#include "common.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include <stdexcept>

#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/task_arena.h>

// ---------------------------------------------------------------------------
// Wavefront CSV helper — appended to common.h schema
// ---------------------------------------------------------------------------

inline void append_wavefront_csv(const std::string& path,
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
    int         workers    = 1;
    int         iterations = 20;
    int         warmup     = 3;
    std::string output     = "tbb_wavefront.csv";
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
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Cli cli = parse_args(argc, argv);

    const int N     = cli.n;
    const int W     = cli.workers;
    const int ITERS = cli.iterations;
    const int WARM  = cli.warmup;

    std::printf("Wavefront N=%d workers=%d\n", N, W);

    // Pre-allocate N×N grid with boundary values
    std::vector<double> grid(static_cast<std::size_t>(N) * N, 0.0);
    for (int j = 0; j < N; ++j) grid[j]           = static_cast<double>(j + 1);  // row 0
    for (int i = 1; i < N; ++i) grid[i * N]        = static_cast<double>(i + 1);  // col 0

    tbb::task_arena arena(W);
    std::unique_ptr<PinningObserver> obs;
    if (cli.pin) {
        // Pin the calling (main) thread to core 0 so it stays schedulable
        // across all arena.execute() calls without OS migration.
        // Worker threads are pinned to cores 1..W via PinningObserver.
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(0, &cs);
        sched_setaffinity(0, sizeof(cs), &cs);
        obs = std::make_unique<PinningObserver>(arena, 1);
    }

    // One full sweep: iterate all 2N-1 anti-diagonals
    auto run_sweep = [&]() {
        for (int d = 1; d < 2 * N - 1; ++d) {
            // Anti-diagonal d: cells (i,j) with i+j=d, i>=1, j>=1
            const int i_start = std::min(d, N - 1);
            const int width   = std::min({d + 1, N, 2 * N - 1 - d});

            arena.execute([&] {
                tbb::parallel_for(
                    tbb::blocked_range<int>(0, width),
                    [&](const tbb::blocked_range<int>& r) {
                        for (int p = r.begin(); p < r.end(); ++p) {
                            const int i = i_start - p;
                            const int j = d - i;
                            if (i == 0 || j == 0) continue; // boundary
                            grid[i * N + j] = 0.5 * (grid[i * N + (j - 1)]
                                                    + grid[(i - 1) * N + j]);
                        }
                    },
                    tbb::static_partitioner{}
                );
            });
        }
    };

    // Warm-up sweeps (untimed)
    for (int w = 0; w < WARM; ++w)
        run_sweep();

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
    const std::string system_label = cli.pin ? "tbb_pinned" : "tbb";
    std::printf(
        "%s Wavefront | n=%d | workers=%d | iters=%d | total=%.4fs | %.4fs/iter\n",
        system_label.c_str(), N, W, ITERS, total_s, s_per_iter);

    append_wavefront_csv(cli.output, system_label, N, W, ITERS, total_s, s_per_iter);
    return 0;
}
