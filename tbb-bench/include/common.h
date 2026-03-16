#pragma once

#include <cstddef>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <utility>
#include <atomic>

#include <sched.h>
#include <oneapi/tbb/task_scheduler_observer.h>
#include <oneapi/tbb/task_arena.h>

// ---------------------------------------------------------------------------
// CSV helpers — schema matches timely-bench/src/lib.rs
// ---------------------------------------------------------------------------

inline void append_csv(const std::string& path,
                       const std::string& system,
                       const std::string& kernel,
                       std::size_t        array_size,
                       int                workers,
                       double             elapsed_s,
                       double             gb_s)
{
    bool write_header = false;
    {
        std::ifstream f(path);
        write_header = !f.good();
    }
    std::ofstream f(path, std::ios::app);
    if (write_header)
        f << "system,kernel,array_size,workers,elapsed_s,gb_s\n";
    f << system << ',' << kernel << ',' << array_size << ','
      << workers << ',' << elapsed_s << ',' << gb_s << '\n';
}

inline void append_graph_csv(const std::string& path,
                              const std::string& system,
                              const std::string& dataset,
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
        f << "system,dataset,workers,iterations,total_s,s_per_iter\n";
    f << system << ',' << dataset << ',' << workers << ','
      << iterations << ',' << total_s << ',' << s_per_iter << '\n';
}

// ---------------------------------------------------------------------------
// SNAP edge-list parser — same semantics as timely-bench/src/lib.rs::parse_snap
// Lines starting with '#' are skipped. Returns (num_nodes, edges).
// ---------------------------------------------------------------------------

inline std::pair<std::size_t, std::vector<std::pair<uint32_t,uint32_t>>>
parse_snap(const std::string& path)
{
    std::ifstream f(path);
    if (!f) throw std::runtime_error("Cannot open graph file: " + path);

    std::vector<std::pair<uint32_t,uint32_t>> edges;
    uint32_t max_node = 0;
    std::string line;

    while (std::getline(f, line)) {
        if (line.empty() || line[0] == '#') continue;
        std::istringstream ss(line);
        uint32_t src, dst;
        if (!(ss >> src >> dst)) continue;
        edges.emplace_back(src, dst);
        if (src > max_node) max_node = src;
        if (dst > max_node) max_node = dst;
    }

    return {static_cast<std::size_t>(max_node) + 1, std::move(edges)};
}

// ---------------------------------------------------------------------------
// CPU-pinning observer for tbb::task_arena
// Pins each arena worker thread to core (base_core + sequential_id).
// ---------------------------------------------------------------------------

class PinningObserver : public tbb::task_scheduler_observer {
    int              base_core_;
    std::atomic<int> next_id_{0};
public:
    PinningObserver(tbb::task_arena& arena, int base_core)
        : tbb::task_scheduler_observer(arena), base_core_(base_core)
    {
        observe(true);
    }

    void on_scheduler_entry(bool is_worker) override {
        if (!is_worker) return;  // don't re-pin the external calling thread
        int id = next_id_.fetch_add(1, std::memory_order_relaxed);
        cpu_set_t cs;
        CPU_ZERO(&cs);
        CPU_SET(base_core_ + id, &cs);
        sched_setaffinity(0, sizeof(cs), &cs);
    }
};
