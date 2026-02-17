# SynStream

### Task-Graph Framework for Streaming Applications

SynStream automates the process of describing a computational graph and executing it in a specified environment. It focuses on streaming applications, which require low-latency and data-reuse between computation stages in a consumer-producer MIMO pattern.

## How to Use

1. Describe the application using a JSON file (or use SynStream-Visualizer).

2. Obtain (or create) a plugin library compatible with SynStream (dynamic `.so` file and header file or Rust source file).

3. If the application functions use only Rust standard types, skip step 2 and have the source code available.

4. Set **FUNC_PATH** environment variable to the header or Rust source path.

5. Execute SynStream. See available arguments with `cargo run -- --help`:

```
Usage: main [OPTIONS] --json <FILE> --dylib <FILE>

Options:
      --json <FILE>                         Task graph definition (required)
      --dylib <FILE>                        Plugin library (.so) (required)
      --workers <CORES>                     Worker thread count [default: 1]
      --core-offset <CORE_OFFSET>           CPU affinity start index [default: 1]
      --system-threads <SYSTEM_THREADS>     Resolution threads [default: 1]
      --receiver-threads <RECEIVER_THREADS> Network receiver threads [default: 1]
      --slots <SLOTS>                       Concurrent stream slots [default: 1]
      --max-streams <MAX_STREAMS>           Streams to process [default: 1]
      --max-runtime <MAX_RUNTIME>           Timeout in seconds, 0 = unlimited [default: 0]
      --batching-size <BATCHING_SIZE>       Tasks per batch [default: 1]
      --batching-limit <BATCHING_LIMIT>     Max batch wait time in microseconds [default: 10]
      --timing <FILE>                       CSV timing output file
      --record                              Enable scheduler event recording
      --record-stream <STREAM_ID>           Record only a specific stream
      --exclude-streams <EXCLUDE_STREAMS>   Exclude N initial streams from timing statistics [default: 0]
      --slot-priority                       Sequential slot processing with round-robin for cache locality
      --fifo                                Enable FIFO scheduler
      --custom                              Enable custom lock-free priority scheduler
      --use-rdtsc                           Use hardware RDTSC for timing
      --inits                               Print initializations to stdout
      --debug                               Enable debug printing
      --output <FILE>                       Redirect stdout [default: stdout]
  -h, --help                                Print help
  -V, --version                             Print version
```

## Architecture

**Workspace crates:**
- `synstream-core` — Runtime, scheduler, graph engine, and network receiver infrastructure
- `synstream-types` — `CmTypes` enum for type-erased value passing across plugin boundaries
- `synstream-macro` — Procedural macros for plugin wrapping (WIP)
- `examples/matrix-compute` — FFT and matrix computation benchmark (under refactoring)

**Core modules in `synstream-core/src`:**
- `runtime.rs` / `runtime_funcs.rs` — Main execution orchestration, slot and stream management
- `scheduler.rs` — Task scheduling strategies: work-stealing (default), FIFO, and lock-free custom
- `graph.rs` / `graph_gen.rs` — DAG representation, JSON parsing, dependency resolution
- `network.rs` / `network_funcs.rs` — Dedicated receiver threads for UDP/TCP packet injection
- `buffers.rs` — Per-slot, per-node result storage with lock-free dependency tracking
- `async_recorder.rs` — Lock-free timing and event recording

**Threading model:**
- Worker threads (Rayon or custom pool) with CPU affinity for kernel execution
- System (resolution) threads for dependency checking and task scheduling
- Dedicated network receiver threads for low-latency packet ingestion
- Async recorder thread for non-blocking timing output

## JSON Graph Format

Graphs define `initializations` (pre-computed objects) and `nodes` (tasks). Key argument types:
- `$ref` — Reference to an initialized object
- `$res` — Result from a predecessor node (data dependency)
- `$barrier` — Wait for all predecessor instances
- `$network` — Network packet injection marker

The `factor` field creates parallel node instances. Network nodes are handled by receiver threads and excluded from the task scheduler's dependency counters.

## Environment Variables

- `FUNC_PATH` — Path to plugin header or Rust source (required)
- `WRAP_PATH` — Wrapper functions file (optional set to bypass converter, auto-generated)
- `REG_PATH` — Function registry file (optional set to bypass converter, auto-generated)

## Build

```bash
# Build entire workspace
cargo build

# Optimized build
cargo build --release

# Quick compilation check
cargo check --lib
```
