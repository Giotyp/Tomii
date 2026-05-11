#pragma once

#include <taskflow/taskflow.hpp>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <fstream>
#include <vector>
#include <sched.h>

// ---------------------------------------------------------------------------
// CSV helper
// ---------------------------------------------------------------------------

static void append_wavefront_csv(const std::string &path,
                                 const std::string &system,
                                 int n,
                                 int workers,
                                 int iterations,
                                 double total_ms,
                                 double ms_per_iter)
{
    bool write_header = false;
    {
        std::ifstream f(path);
        write_header = !f.good();
    }
    std::ofstream f(path, std::ios::app);
    if (write_header)
        f << "system,n,workers,iterations,total_ms,ms_per_iter\n";
    f << system << ',' << n << ',' << workers << ','
      << iterations << ',' << total_ms << ',' << ms_per_iter << '\n';
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cli
{
    int n = 512;
    int workers = 1;
    int iterations = 20;
    int warmup = 3;
    std::string output = "tf_wavefront.csv";
    std::string partitioner = "static";
    bool pin = false;
};

static Cli parse_args(int argc, char **argv)
{
    Cli c;
    for (int i = 1; i < argc; ++i)
    {
        std::string a = argv[i];
        auto val = [&]() -> std::string
        {
            if (i + 1 >= argc)
                throw std::runtime_error("Missing value for " + a);
            return argv[++i];
        };
        if (a == "--n")
            c.n = std::stoi(val());
        else if (a == "--workers")
            c.workers = std::stoi(val());
        else if (a == "--iterations")
            c.iterations = std::stoi(val());
        else if (a == "--warmup")
            c.warmup = std::stoi(val());
        else if (a == "--output")
            c.output = val();
        else if (a == "--partitioner")
        {
            c.partitioner = val();
            if (c.partitioner != "static" && c.partitioner != "dynamic" &&
                c.partitioner != "guided" && c.partitioner != "random")
                throw std::runtime_error(
                    "Unknown partitioner: " + c.partitioner +
                    " (choose: static, dynamic, guided, random)");
        }
        else if (a == "--pin")
            c.pin = true;
        else
            throw std::runtime_error("Unknown flag: " + a);
    }
    return c;
}

// ---------------------------------------------------------------------------
// CPU pinning for Taskflow worker threads
// ---------------------------------------------------------------------------

static void pin_workers(tf::Executor &executor, int base_core)
{
    const size_t nw = executor.num_workers();
    for (size_t i = 0; i < nw; ++i)
    {
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(base_core + static_cast<int>(i), &cs);
        auto handle = executor.async([cs]() mutable
                                     { sched_setaffinity(0, sizeof(cs), &cs); });
        handle.get();
    }
}

// ---------------------------------------------------------------------------
// File-scope callable for for_each_index (GCC 11 can't link local lambdas
// used as template args in Taskflow's for_each_index).
// ---------------------------------------------------------------------------

struct CellFn
{
    double *grid;
    int n;
    int d;
    int i_start;

    void operator()(int p) const
    {
        const int i = i_start - p;
        const int j = d - i;
        if (i == 0 || j == 0)
            return;
        grid[i * n + j] = 0.5 * (grid[i * n + (j - 1)] + grid[(i - 1) * n + j]);
    }
};

// ---------------------------------------------------------------------------
// Wavefront DAG builder — templated on partitioner type so each partitioner
// variant compiles to its own optimised for_each_index instantiation.
// CellFn callables are copied into the task graph at build time, so the
// local fns vector does not need to outlive this function.
// ---------------------------------------------------------------------------

template <typename P>
void build_wavefront_dag(tf::Taskflow &taskflow, int N, double *grid_ptr, P partitioner)
{
    const int ndiags = 2 * N - 2;
    std::vector<CellFn> fns;
    std::vector<int> widths(ndiags);
    fns.reserve(ndiags);

    for (int d = 1; d < 2 * N - 1; ++d)
    {
        int idx = d - 1;
        int i_start = std::min(d, N - 1);
        widths[idx] = std::min({d + 1, N, 2 * N - 1 - d});
        fns.push_back(CellFn{grid_ptr, N, d, i_start});
    }

    tf::Task prev;
    for (int idx = 0; idx < ndiags; ++idx)
    {
        tf::Task cur = taskflow.for_each_index(0, widths[idx], 1, fns[idx], partitioner);
        if (idx > 0)
            prev.precede(cur);
        prev = cur;
    }
}
