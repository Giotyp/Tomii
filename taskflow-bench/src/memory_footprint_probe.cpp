// Q1: Memory footprint of S tf::Taskflow linear-chain instances vs the theoretical O(S×N) model.
// Prints per-node and per-slot byte counts to support the analytical comparison in the paper.
//
// Usage: ./memory_footprint_probe [--n 128] [--max-slots 16]
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <malloc.h>
#include <string>
#include "taskflow/taskflow.hpp"

static size_t heap_bytes() {
    struct mallinfo2 mi = mallinfo2();
    return mi.uordblks;  // bytes currently allocated
}

// Build a linear chain of n nodes in a fresh tf::Taskflow.
// Returns the taskflow; caller owns it.
static tf::Taskflow* build_chain(int n) {
    auto* tf = new tf::Taskflow();
    tf::Task prev;
    for (int i = 0; i < n; i++) {
        tf::Task t = tf->emplace([](){});
        if (i > 0) prev.precede(t);
        prev = t;
    }
    return tf;
}

int main(int argc, char** argv) {
    int n = 128;
    int max_slots = 16;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--n") == 0 && i+1 < argc)       n = atoi(argv[++i]);
        if (strcmp(argv[i], "--max-slots") == 0 && i+1 < argc) max_slots = atoi(argv[++i]);
    }

    printf("sizeof(tf::Node)     = %zu bytes\n", sizeof(tf::Node));
    printf("sizeof(tf::Taskflow) = %zu bytes\n", sizeof(tf::Taskflow));
    printf("N = %d nodes, max_slots = %d\n\n", n, max_slots);

    // Warm up the allocator so baseline is stable
    {
        auto* warmup = build_chain(n);
        delete warmup;
    }

    printf("%-8s %-14s %-18s %-18s %-14s\n",
           "S", "heap_bytes", "expected(S×N×248)", "overhead/node", "bytes/extra_slot");

    size_t prev_heap = 0;
    std::vector<tf::Taskflow*> instances;

    for (int s = 1; s <= max_slots; s *= 2) {
        // Build to target slot count
        while ((int)instances.size() < s) {
            instances.push_back(build_chain(n));
        }

        size_t h = heap_bytes();
        size_t expected = (size_t)s * n * sizeof(tf::Node);
        size_t extra_slot_cost = (s > 1 && prev_heap > 0) ? (h - prev_heap) / (s/2) : 0;

        printf("%-8d %-14zu %-18zu %-18.1f %-14zu\n",
               s, h, expected, (double)h / (s * n), extra_slot_cost);

        prev_heap = h;
    }

    // Cleanup
    for (auto* p : instances) delete p;

    printf("\n=== Key ratios ===\n");
    printf("tf::Node state fields: _state(4) + _join_counter(8) = 12 bytes\n");
    printf("tf::Node topology: %zu - 12 = %zu bytes (%.1f%% of total)\n",
           sizeof(tf::Node), sizeof(tf::Node) - 12,
           100.0 * (sizeof(tf::Node) - 12) / sizeof(tf::Node));
    printf("\nFor S concurrent streams of N nodes:\n");
    printf("  Taskflow replication cost: S × N × %zu bytes\n", sizeof(tf::Node));
    printf("  Of that, redundant topology: (S-1) × N × %zu bytes\n", sizeof(tf::Node) - 12);
    printf("  For S=8, N=1024: redundant = %zu bytes (%.1f MB)\n",
           (size_t)7 * 1024 * (sizeof(tf::Node) - 12),
           7.0 * 1024.0 * (sizeof(tf::Node) - 12) / (1024.0 * 1024.0));

    return 0;
}
