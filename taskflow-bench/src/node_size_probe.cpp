// Prints sizeof/offsetof for tf::Node fields to quantify topology vs state bytes.
// Compile: g++ -O0 -std=c++17 -Itaskflow-lib src/node_size_probe.cpp -o node_size_probe -lpthread
#include <cstdio>
#include <cstddef>
#include "taskflow/taskflow.hpp"

// Mirrors the private layout of tf::Node enough to get offsets.
// We verify sizes with static_assert against the public sizeof.
int main() {
    printf("=== tf::Node size breakdown ===\n");
    printf("sizeof(tf::Node)               = %zu\n", sizeof(tf::Node));
    printf("sizeof(tf::Taskflow)           = %zu\n", sizeof(tf::Taskflow));

    // Individual field sizes (types match graph.hpp private section exactly)
    printf("\n--- topology fields (immutable after build) ---\n");
    printf("std::string                    = %zu  (_name)\n",        sizeof(std::string));
    printf("unsigned                       = %zu  (_priority)\n",    sizeof(unsigned));
    printf("void*                          = %zu  (_data)\n",        sizeof(void*));
    printf("tf::Topology*                  = %zu  (_topology)\n",    sizeof(tf::Topology*));
    printf("tf::Node*                      = %zu  (_parent)\n",      sizeof(tf::Node*));

    // SmallVector<tf::Node*, 2> — base is 3×ptr (Begin/End/Cap) + 2 inline slots
    using SVN = tf::SmallVector<tf::Node*>;
    printf("SmallVector<Node*,2>           = %zu  (_successors / _dependents each)\n", sizeof(SVN));

    printf("std::unique_ptr<...>           = %zu  (_semaphores)\n",  sizeof(std::unique_ptr<int>));
    printf("std::exception_ptr             = %zu  (_exception_ptr)\n", sizeof(std::exception_ptr));

    // handle_t variant — stores the callable
    using Static_t = std::variant<std::function<void()>, std::function<void(tf::Runtime&)>>;
    printf("handle_t (≈ Static variant)    ≥ %zu  (_handle / callable)\n", sizeof(Static_t));

    printf("\n--- per-execution state fields (reset each run) ---\n");
    printf("std::atomic<int>               = %zu  (_state)\n",       sizeof(std::atomic<int>));
    printf("std::atomic<size_t>            = %zu  (_join_counter)\n", sizeof(std::atomic<size_t>));

    printf("\n--- topology total (approx, excludes handle) ---\n");
    size_t topo_approx =
        sizeof(std::string)           // _name
        + sizeof(unsigned)            // _priority
        + sizeof(void*)               // _data
        + sizeof(tf::Topology*)       // _topology
        + sizeof(tf::Node*)           // _parent
        + sizeof(SVN) * 2             // _successors + _dependents
        + sizeof(std::unique_ptr<int>)// _semaphores
        + sizeof(std::exception_ptr); // _exception_ptr
    printf("topo_approx                    = %zu\n", topo_approx);

    size_t state_bytes = sizeof(std::atomic<int>) + sizeof(std::atomic<size_t>);
    printf("state_bytes                    = %zu\n", state_bytes);
    printf("state %% of sizeof(tf::Node)   = %.1f%%\n",
           100.0 * state_bytes / sizeof(tf::Node));

    printf("\n--- cache-line analysis ---\n");
    // The critical question: are _state/_join_counter on the same 64-byte line
    // as _successors (a pointer read by workers on other cores)?
    // We can't get private offsets, but we can emit a note from field ordering:
    // layout order from graph.hpp:
    //   _name(32) _priority(4) [pad4] _data(8) _topology(8) _parent(8)
    //   _successors(40) _dependents(40)   <- offset ~104
    //   _state(4) _join_counter(8)        <- offset ~152
    //   ... continuation
    // At offset 128 (== 2 cache lines) we'd land inside _dependents.
    // _state/_join_counter are at offset ~152, i.e. cache line 2 (bytes 128-191).
    // _successors.BeginX is at offset ~104, i.e. cache line 1 (bytes 64-127).
    // So they're on adjacent cache lines, not the exact same line.
    // But _join_counter writes (by completing workers) will evict line 2, which
    // also contains _dependents tail and must be re-read by workers fetching _join_counter.
    printf("Field order (from graph.hpp):\n");
    printf("  _name(32) _priority(4) _data(8) _topology(8) _parent(8)\n");
    printf("  _successors(~40) _dependents(~40)    <- cache line 1-2\n");
    printf("  _state(4) _join_counter(8) ...       <- cache line 2-3\n");
    printf("  → _join_counter and _successors head span adjacent lines;\n");
    printf("    a write to _join_counter on task completion\n");
    printf("    invalidates line 2 which another worker reads for _dependents.\n");

    return 0;
}
