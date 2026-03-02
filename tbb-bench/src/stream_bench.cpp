//! Intel TBB STREAM benchmark.
//!
//! Each TBB worker independently allocates and operates on its own array
//! (total memory = workers * array_size * bytes_per_element), matching the
//! SynStream and Timely STREAM setups exactly.
//!
//! Usage:
//!   ./stream_bench --kernel triad --array-size 268435456 --workers 4 \
//!                  --output tbb_stream.csv [--pin]

#include "common.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/task_arena.h>

// ---------------------------------------------------------------------------
// CLI (hand-rolled to avoid external deps)
// ---------------------------------------------------------------------------

struct Cli {
    std::string kernel     = "triad";
    std::size_t array_size = 268'435'456UL;
    int         workers    = 1;
    double      scalar     = 3.0;
    int         reps       = 20;
    int         warmup     = 3;
    std::string output     = "tbb_stream.csv";
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
        if      (a == "--kernel")     c.kernel     = val();
        else if (a == "--array-size") c.array_size = std::stoull(val());
        else if (a == "--workers")    c.workers    = std::stoi(val());
        else if (a == "--scalar")     c.scalar     = std::stod(val());
        else if (a == "--reps")       c.reps       = std::stoi(val());
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

    const std::size_t N  = cli.array_size;
    const int         W  = cli.workers;
    const double      sc = cli.scalar;

    // Number of arrays touched per rep (for GB/s calculation)
    std::size_t n_arrays = 0;
    if (cli.kernel == "copy" || cli.kernel == "scale") n_arrays = 2;
    else if (cli.kernel == "add" || cli.kernel == "triad") n_arrays = 3;
    else throw std::runtime_error("Unknown kernel: " + cli.kernel);

    const std::size_t bytes_total =
        static_cast<std::size_t>(W) * n_arrays * N * sizeof(double);

    // Allocate per-worker arrays
    std::vector<std::vector<double>> a(W, std::vector<double>(N, 0.0));
    std::vector<std::vector<double>> b(W, std::vector<double>(N, 2.0));
    std::vector<std::vector<double>> c(W, std::vector<double>(N, 1.0));

    tbb::task_arena arena(W);
    std::unique_ptr<PinningObserver> obs;
    if (cli.pin) obs = std::make_unique<PinningObserver>(arena, 1);

    std::vector<double> all_elapsed;
    all_elapsed.reserve(cli.warmup + cli.reps);

    const std::string& kernel = cli.kernel;

    for (int rep = 0; rep < cli.warmup + cli.reps; ++rep) {
        auto t0 = std::chrono::high_resolution_clock::now();

        arena.execute([&] {
            tbb::parallel_for(
                0, W,
                [&](int w) {
                    double* __restrict__ av = a[w].data();
                    const double* __restrict__ bv = b[w].data();
                    const double* __restrict__ cv = c[w].data();

                    if (kernel == "copy") {
                        for (std::size_t i = 0; i < N; ++i) av[i] = bv[i];
                    } else if (kernel == "scale") {
                        for (std::size_t i = 0; i < N; ++i) av[i] = sc * bv[i];
                    } else if (kernel == "add") {
                        for (std::size_t i = 0; i < N; ++i) av[i] = bv[i] + cv[i];
                    } else { // triad
                        for (std::size_t i = 0; i < N; ++i) av[i] = bv[i] + sc * cv[i];
                    }
                },
                tbb::static_partitioner{}
            );
        });

        auto t1 = std::chrono::high_resolution_clock::now();
        all_elapsed.push_back(
            std::chrono::duration<double>(t1 - t0).count());
    }

    // Prevent dead-code elimination
    volatile double sink = a[0][0];
    (void)sink;

    // Exclude warmup
    double sum = 0.0;
    for (int i = cli.warmup; i < cli.warmup + cli.reps; ++i)
        sum += all_elapsed[i];
    double mean = sum / cli.reps;
    double gb_s = static_cast<double>(bytes_total) / mean / 1e9;

    std::printf(
        "TBB STREAM %s | workers=%d | array_size=%zu | mean=%.4fs | %.2f GB/s\n",
        cli.kernel.c_str(), W, N, mean, gb_s);

    append_csv(cli.output, "tbb", cli.kernel, N, W, mean, gb_s);
    return 0;
}
