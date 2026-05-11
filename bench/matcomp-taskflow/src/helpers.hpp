#pragma once

#include <algorithm>
#include <fstream>
#include <sched.h>
#include <stdexcept>
#include <string>
#include <taskflow/taskflow.hpp>
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
// CSV helper — appends one result row
// ---------------------------------------------------------------------------

static void append_matcomp_csv(
    const std::string &path,
    int n, int buf_size, int slots, int workers, int streams,
    double ms_per_stream,
    double gv_kern,  double fft_kern, double vtm_kern, double mm_kern,
    double gv_disp,  double fft_disp, double vtm_disp, double mm_disp)
{
    bool write_header = false;
    {
        std::ifstream f(path);
        write_header = !f.good();
    }
    std::ofstream f(path, std::ios::app);
    if (write_header)
        f << "system,n,buf_size,slots,workers,streams,ms_per_stream,"
          << "gv_kern_us,fft_kern_us,vtm_kern_us,mm_kern_us,"
          << "gv_disp_us,fft_disp_us,vtm_disp_us,mm_disp_us\n";
    f << "taskflow_matcomp"
      << ',' << n << ',' << buf_size << ',' << slots << ',' << workers << ',' << streams
      << ',' << ms_per_stream
      << ',' << gv_kern  << ',' << fft_kern << ',' << vtm_kern << ',' << mm_kern
      << ',' << gv_disp  << ',' << fft_disp << ',' << vtm_disp << ',' << mm_disp
      << '\n';
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cli
{
    int n        = 200;   // DAG fan-out (items per stream)
    int buf_size = 100;   // complex vector length per item
    int slots    = 4;     // concurrent streams (S)
    int streams  = 30;    // measured streams
    int warmup   = 10;    // warmup streams
    int workers  = 4;     // executor threads (W)
    int pin_core = 3;     // first worker core (0 = no pinning)
    bool pin     = false;
    std::string output = "tf_matcomp.csv";
};

static Cli parse_args(int argc, char **argv)
{
    Cli c;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto val = [&]() -> std::string {
            if (i + 1 >= argc)
                throw std::runtime_error("Missing value for " + a);
            return argv[++i];
        };
        if      (a == "--n")        c.n        = std::stoi(val());
        else if (a == "--buf")      c.buf_size = std::stoi(val());
        else if (a == "--slots")    c.slots    = std::stoi(val());
        else if (a == "--streams")  c.streams  = std::stoi(val());
        else if (a == "--warmup")   c.warmup   = std::stoi(val());
        else if (a == "--workers")  c.workers  = std::stoi(val());
        else if (a == "--pin")      { c.pin_core = std::stoi(val()); c.pin = true; }
        else if (a == "--output")   c.output   = val();
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
