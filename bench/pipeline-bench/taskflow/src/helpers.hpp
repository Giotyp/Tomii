#pragma once

#include <taskflow/taskflow.hpp>
#include <algorithm>
#include <cmath>
#include <fstream>
#include <sched.h>
#include <stdexcept>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Peak RSS from /proc/self/status (Linux only)
// ---------------------------------------------------------------------------

static long peak_rss_kb()
{
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("VmHWM:", 0) == 0) {
            long kb = 0;
            sscanf(line.c_str(), "VmHWM: %ld kB", &kb);
            return kb;
        }
    }
    return -1;
}

// ---------------------------------------------------------------------------
// CSV helper
// ---------------------------------------------------------------------------

static void append_pipeline_csv(
    const std::string &path,
    const std::string &system,
    int n,
    int items_per_stream,
    int slots,
    int workers,
    int streams,
    double ms_per_stream,
    int transform_iters)
{
    bool write_header = false;
    {
        std::ifstream f(path);
        write_header = !f.good();
    }
    std::ofstream f(path, std::ios::app);
    if (write_header)
        f << "system,n,items_per_stream,slots,workers,streams,ms_per_stream,transform_iters\n";
    f << system << ','
      << n << ','
      << items_per_stream << ','
      << slots << ','
      << workers << ','
      << streams << ','
      << ms_per_stream << ','
      << transform_iters << '\n';
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cli
{
    int n        = 256;   // items per stream (pipeline width)
    int slots    = 1;     // concurrent streams (S)
    int streams  = 2000;  // total streams (T)
    int warmup   = 200;   // warmup streams
    int workers  = 4;     // executor thread count (W)
    std::string mode   = "clone";     // clone | sequential
    std::string output = "tf_pipeline.csv";
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
        if      (a == "--n")       c.n       = std::stoi(val());
        else if (a == "--slots")   c.slots   = std::stoi(val());
        else if (a == "--streams") c.streams = std::stoi(val());
        else if (a == "--warmup")  c.warmup  = std::stoi(val());
        else if (a == "--workers") c.workers = std::stoi(val());
        else if (a == "--mode")
        {
            c.mode = val();
            if (c.mode != "clone" && c.mode != "sequential")
                throw std::runtime_error(
                    "Unknown mode: " + c.mode + " (choose: clone, sequential)");
        }
        else if (a == "--output")  c.output  = val();
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
        auto handle = executor.async([cs]() mutable {
            sched_setaffinity(0, sizeof(cs), &cs);
        });
        handle.get();
    }
}

// ---------------------------------------------------------------------------
// Expected mean: mean of transform((i+1)/n) for i in [0, n)
// where transform(x) = sqrt(x) + x * 0.5
// ---------------------------------------------------------------------------

static double expected_mean(int n)
{
    double sum = 0.0;
    for (int i = 0; i < n; ++i)
    {
        double x = static_cast<double>(i + 1) / n;
        sum += std::sqrt(x) + x * 0.5;
    }
    return sum / n;
}
