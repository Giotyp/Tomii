# scheduler-plugin example

Demonstrates the stable `TaskScheduler` plugin API introduced in `tomii-core`.

`FifoScheduler` is a minimal scheduler backed by a single shared FIFO work
queue (`Mutex<VecDeque>`). Worker threads block on a `Condvar` when idle and
wake one-at-a-time as tasks arrive. The implementation depends only on
`tomii-core` and `tomii-types` public APIs — no internal Tomii types are used.

## Build

```sh
cargo build -p scheduler-plugin-example
```

## Test

```sh
cargo test -p scheduler-plugin-example
```

## Integrating with Tomii

```rust
use std::sync::Arc;
use tomii_core::runtime::TomiiRtBuilder;
use scheduler_plugin_example::FifoScheduler;

// Build a placeholder Rayon scheduler so GraphSpec::compile has core metadata.
let placeholder = tomii_core::scheduler::create_scheduler(tomii_core::scheduler::SchedulerConfig {
    scheduler_type: tomii_core::scheduler::SchedulerType::Fifo,
    core_offset: 0,
    num_workers: 4,
    ..Default::default()
});
let compiled = spec.compile(&placeholder);

// Replace the placeholder with the FIFO plugin.
let fifo: Arc<dyn tomii_core::scheduler::TaskScheduler> = FifoScheduler::new(4, 0);
let rt = TomiiRtBuilder::new_with_plugin(compiled, fifo)
    .slots(4)
    .max_streams(1000)
    .build();

rt.run();
```

## Limitations vs built-in schedulers

| Feature | FifoScheduler | Custom (built-in) |
|---|---|---|
| CPU affinity pinning | No | Yes |
| Work-stealing | No | No (per-group queues) |
| Per-task allocation | `Box` per task | Lock-free, no extra alloc |
| Priority dispatch | Ignored | Yes (High/Normal/Low) |
| Timing CSV output | No | Yes (via AsyncRecorder) |

For latency-sensitive production workloads use `SchedulerImpl::Custom`
(`--custom` on the CLI). Use this plugin path when you need a custom dispatch
policy that the built-in schedulers cannot express.

## API reference

See `tomii-core/PLUGIN_SCHEDULER_API.md` for the full stability contract,
method descriptions, and semver policy.
