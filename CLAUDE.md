# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SynStream is a Rust task-graph framework for streaming applications, focusing on low-latency execution and data-reuse in consumer-producer MIMO (Multiple-Input Multiple-Output) patterns. It executes computational graphs defined in JSON with dynamically-loaded plugin libraries.

## Git Commits
When commiting, keep the commit message short and avoid adding ' Co-Authored-By: Claude Sonnet 4.6      
   <noreply@anthropic.com>'

## Instructions
Never generate markdown files without getting explicit permission from the user via verification.

## Plan Mode On
When in plan mode, always store the plans generated for this project under ~/SynStream/.claude/plans and use a descriptive name describing the feature you are about to implement.

## Analysis Generated Markdowns
When generating a markdown analysis file, always follow these steps:
1. Ask the user permission to create the file
2. Store the generated file under .claude/analysis with a descriptive name

## Build Commands

```bash
# Setup environment (required before building)
source examples/mimolib/scripts/export.sh

# Build entire workspace
cargo build

# Build release (optimized)
cargo build --release

# Quick compilation check
cargo check --lib

# Linting
cargo clippy --lib

# Format
cargo fmt
```

## Testing

```bash
# Run all tests
cargo test

# With debug logging
RUST_LOG=debug cargo test

# MIMO streaming test
examples/mimolib/scripts/run_mimo_ptr.sh
# Validates: ~/mimolib/timing_ptr.txt contains all nodes from graph
```

## Running

```bash
cargo run -- \
  --json /path/to/graph.json \
  --dylib /path/to/plugin.so \
  --workers 4 \
  --core-offset 1 \
  --max-runtime 60
```

Key CLI flags: `--json` (graph file), `--dylib` (plugin library), `--workers` (thread count), `--core-offset` (CPU affinity start), `--nrx` (network receiver threads), `--slots` (concurrent streams), `--timing` (CSV output), `--record`, `--debug`, `--fifo`, `--use-rdtsc`.

## Architecture

**Workspace crates:**
- `synstream-core` - Runtime, scheduler, graph engine, network receiver infrastructure
- `synstream-types` - `CmTypes` enum for type-erased values across plugins
- `synstream-macro` - Procedural macros for plugin wrapping (WIP)
- `examples/matrix-compute` - FFT+matrix benchmark example
- `examples/mimolib` - MIMO benchmark example

**Core modules in synstream-core/src:**
- `runtime.rs` / `runtime_funcs.rs` - Main execution orchestration, worker pools
- `scheduler.rs` - Task scheduling (default, FIFO, slot-priority strategies)
- `graph.rs` / `graph_gen.rs` - DAG representation, JSON parsing, dependency resolution
- `network.rs` / `network_funcs.rs` - Dedicated receiver threads, packet injection
- `buffers.rs` - Per-slot, per-node result storage
- `async_recorder.rs` - Lock-free timing/event recording

**Threading model:**
- System thread(s) for resolution/state machine
- Worker threads (Rayon pool) with CPU affinity
- Dedicated network receiver threads (UDP/TCP)
- Async recorder thread for timing output

## JSON Graph Format

Graphs define `initializations` (pre-computed objects) and `nodes` (tasks). Key argument types:
- `$ref` - Reference to initialized object
- `$res` - Result from predecessor node (data dependency)
- `$barrier` - Wait for all predecessor instances
- `$network` - Network injection marker

`factor` field creates parallel node instances. Network nodes are handled by receiver threads, not the task scheduler.

## Environment Variables

- `FUNC_PATH` - Path to plugin header or Rust source (required)
- `WRAP_PATH` - Wrapper functions file (optional, auto-generated)
- `REG_PATH` - Function registry file (optional, auto-generated)

## Documentation

Additional specifications in `.github/`:
- `copilot-instructions.md` - Developer workflows
- `journal.md` - Development progress log
- `skills/mimo-testing/SKILL.md` - MIMO test runbook
