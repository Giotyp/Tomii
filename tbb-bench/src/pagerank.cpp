//! Intel TBB PageRank benchmark (COST-style).
//!
//! Algorithm mirrors SynStream cost-bench scatter/gather pattern:
//!   - Linear edge partitioning (matching SynStream's PartitionedEdges)
//!   - Per-worker contribution buffers (no races)
//!   - BSP: scatter phase → gather phase, repeated for --iterations rounds
//!
//! Usage:
//!   ./pagerank --graph /path/to/snap.txt --dataset livejournal \
//!              --workers 4 --iterations 20 --output tbb_pagerank.csv [--pin]

#include "common.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/task_arena.h>

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Cli {
    std::string graph      = "";
    std::string dataset    = "unknown";
    int         workers    = 1;
    int         iterations = 20;
    double      damping    = 0.85;
    std::string output     = "tbb_pagerank.csv";
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
        if      (a == "--graph")      c.graph      = val();
        else if (a == "--dataset")    c.dataset    = val();
        else if (a == "--workers")    c.workers    = std::stoi(val());
        else if (a == "--iterations") c.iterations = std::stoi(val());
        else if (a == "--damping")    c.damping    = std::stod(val());
        else if (a == "--output")     c.output     = val();
        else if (a == "--pin")        c.pin        = true;
        else throw std::runtime_error("Unknown flag: " + a);
    }
    if (c.graph.empty()) throw std::runtime_error("--graph is required");
    return c;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    Cli cli = parse_args(argc, argv);

    const int    W       = cli.workers;
    const int    ITERS   = cli.iterations;
    const double damping = cli.damping;

    // Load graph
    std::printf("Loading graph %s ...\n", cli.graph.c_str());
    auto [num_nodes, edges] = parse_snap(cli.graph);
    const std::size_t E = edges.size();
    std::printf("  nodes=%zu  edges=%zu\n", num_nodes, E);

    // Out-degrees
    std::vector<float> out_degrees(num_nodes, 0.0f);
    for (auto [src, dst] : edges)
        out_degrees[src] += 1.0f;

    // Linear edge partitioning — matches SynStream's PartitionedEdges
    std::size_t chunk = (E + static_cast<std::size_t>(W) - 1)
                        / static_cast<std::size_t>(W);
    // partitions[w] = {begin_edge_idx, end_edge_idx}
    std::vector<std::pair<std::size_t,std::size_t>> partitions(W);
    for (int w = 0; w < W; ++w) {
        std::size_t beg = static_cast<std::size_t>(w) * chunk;
        std::size_t end = std::min(beg + chunk, E);
        partitions[w] = {beg, end};
    }

    // Initial ranks: uniform 1/N (float, matching SynStream)
    std::vector<float> ranks(num_nodes, 1.0f / static_cast<float>(num_nodes));

    // Per-worker contribution buffers (no races in scatter)
    std::vector<std::vector<float>> pw_contrib(
        W, std::vector<float>(num_nodes, 0.0f));

    tbb::task_arena arena(W);
    std::unique_ptr<PinningObserver> obs;
    if (cli.pin) obs = std::make_unique<PinningObserver>(arena, 1);

    const double base = (1.0 - damping) / static_cast<double>(num_nodes);

    // Warm up with one iteration (not timed)
    arena.execute([&] {
        tbb::parallel_for(0, W, [&](int w) {
            auto& contrib = pw_contrib[w];
            std::fill(contrib.begin(), contrib.end(), 0.0f);
            auto [beg, end] = partitions[w];
            for (std::size_t ei = beg; ei < end; ++ei) {
                auto [src, dst] = edges[ei];
                if (out_degrees[src] > 0.0f)
                    contrib[dst] += ranks[src] / out_degrees[src];
            }
        }, tbb::static_partitioner{});
    });
    arena.execute([&] {
        tbb::parallel_for(
            tbb::blocked_range<std::size_t>(0, num_nodes),
            [&](const tbb::blocked_range<std::size_t>& r) {
                for (std::size_t v = r.begin(); v < r.end(); ++v) {
                    float total = 0.0f;
                    for (int w = 0; w < W; ++w) total += pw_contrib[w][v];
                    ranks[v] = static_cast<float>(base + damping * total);
                }
            }
        );
    });

    // Timed iterations
    double total_s = 0.0;

    for (int iter = 0; iter < ITERS; ++iter) {
        auto t0 = std::chrono::high_resolution_clock::now();

        // Scatter: each worker processes its edge partition into its contrib buffer
        arena.execute([&] {
            tbb::parallel_for(0, W, [&](int w) {
                auto& contrib = pw_contrib[w];
                std::fill(contrib.begin(), contrib.end(), 0.0f);
                auto [beg, end] = partitions[w];
                for (std::size_t ei = beg; ei < end; ++ei) {
                    auto [src, dst] = edges[ei];
                    if (out_degrees[src] > 0.0f)
                        contrib[dst] += ranks[src] / out_degrees[src];
                }
            }, tbb::static_partitioner{});
        });

        // Gather: reduce per-worker contributions and update ranks
        arena.execute([&] {
            tbb::parallel_for(
                tbb::blocked_range<std::size_t>(0, num_nodes),
                [&](const tbb::blocked_range<std::size_t>& r) {
                    for (std::size_t v = r.begin(); v < r.end(); ++v) {
                        float total = 0.0f;
                        for (int w = 0; w < W; ++w) total += pw_contrib[w][v];
                        ranks[v] = static_cast<float>(base + damping * total);
                    }
                }
            );
        });

        auto t1 = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double>(t1 - t0).count();
        total_s += elapsed;
        std::printf("  iter %2d: %.4fs\n", iter + 1, elapsed);
    }

    double s_per_iter = total_s / ITERS;
    std::printf(
        "TBB PageRank | dataset=%s | workers=%d | iters=%d | "
        "total=%.4fs | %.4fs/iter\n",
        cli.dataset.c_str(), W, ITERS, total_s, s_per_iter);

    append_graph_csv(cli.output, "tbb", cli.dataset, W, ITERS,
                     total_s, s_per_iter);
    return 0;
}
