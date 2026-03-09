//! Taskflow Wavefront benchmark.
//!
//! Computes iterative anti-diagonal wavefront on an N×N grid:
//!   grid[i][j] = 0.5 * (grid[i-1][j] + grid[i][j-1])
//!
//! Boundary conditions (pre-allocated, never overwritten):
//!   grid[0][j] = (j+1)   (top row)
//!   grid[i][0] = (i+1)   (left column)
//!
//! Parallelism: Anti-diagonal parallel-for using tf::Taskflow::for_each_index().
//! One for_each_index per anti-diagonal, chained sequentially via precede().
//! The for_each_index completion acts as an implicit barrier (matching TBB's
//! parallel_for approach).
//!
//! Usage:
//!   ./wavefront --n 512 --workers 4 --iterations 20 --output tf_wavefront.csv [--pin]

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <functional>

#include <sched.h>

#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/for_each.hpp>

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
    int         workers    = 1;
    int         iterations = 20;
    int         warmup     = 3;
    std::string output     = "tf_wavefront.csv";
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
// File-scope callable for for_each_index (GCC 11 can't link local lambdas
// used as template args in Taskflow's for_each_index).
// ---------------------------------------------------------------------------

struct CellFn {
    double* grid;
    int     n;
    int     d;
    int     i_start;

    void operator()(int p) const {
        const int i = i_start - p;
        const int j = d - i;
        if (i == 0 || j == 0) return;
        grid[i * n + j] = 0.5 * (grid[i * n + (j - 1)]
                                + grid[(i - 1) * n + j]);
    }
};

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Cli cli = parse_args(argc, argv);

    const int N     = cli.n;
    const int W     = cli.workers;
    const int ITERS = cli.iterations;
    const int WARM  = cli.warmup;

    std::printf("Taskflow Wavefront N=%d workers=%d\n", N, W);

    // Pre-allocate N×N grid with boundary values
    std::vector<double> grid(static_cast<std::size_t>(N) * N, 0.0);
    for (int j = 0; j < N; ++j) grid[j]           = static_cast<double>(j + 1);  // row 0
    for (int i = 1; i < N; ++i) grid[i * N]        = static_cast<double>(i + 1);  // col 0

    tf::Executor executor(W);

    if (cli.pin) {
        pin_workers(executor, 1);
    }

    // Build a Taskflow DAG once: one for_each_index per anti-diagonal,
    // chained sequentially.  Each for_each_index distributes cells within
    // a diagonal across workers (matching TBB's parallel_for approach).
    // The for_each_index completion acts as an implicit barrier.
    tf::Taskflow taskflow;

    double* grid_ptr = grid.data();

    struct DiagInfo { int d; int i_start; int width; };
    std::vector<DiagInfo> diags;
    diags.reserve(2 * N - 2);
    for (int d = 1; d < 2 * N - 1; ++d) {
        int i_start = std::min(d, N - 1);
        int width   = std::min({d + 1, N, 2 * N - 1 - d});
        diags.push_back({d, i_start, width});
    }

    // Use CellFn (file-scope type) to avoid GCC 11 local-type linker issue
    // with tf::for_each_index template instantiation.
    std::vector<CellFn> fns;
    fns.reserve(diags.size());
    for (auto& info : diags) {
        fns.push_back(CellFn{grid_ptr, N, info.d, info.i_start});
    }

    tf::Task prev;
    for (size_t idx = 0; idx < diags.size(); ++idx) {
        tf::Task cur = taskflow.for_each_index(
            0, diags[idx].width, 1, fns[idx]
        );
        if (idx > 0) {
            prev.precede(cur);
        }
        prev = cur;
    }

    auto run_sweep = [&]() {
        executor.run(taskflow).get();
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
    const std::string system_label = cli.pin ? "taskflow_pinned" : "taskflow";
    std::printf(
        "%s Wavefront | n=%d | workers=%d | iters=%d | total=%.4fs | %.4fs/iter\n",
        system_label.c_str(), N, W, ITERS, total_s, s_per_iter);

    append_wavefront_csv(cli.output, system_label, N, W, ITERS, total_s, s_per_iter);
    return 0;
}
