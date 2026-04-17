use clap::Parser;
use core_affinity;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::time::Instant;
use tomii_core::debug::init_debug;
use tomii_core::graph_gen::{from_json, GraphSpec}; // GraphCompiled produced via spec.compile()
use tomii_core::runtime::{BatchConfig, SpinWaitConfig, TomiiRt, TomiiRtBuilder};
use tomii_core::scheduler::{create_scheduler, SchedulerConfig, SchedulerType};
use tomii_core::utils_rdtsc;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[clap(author = "George Typaldos", version, about)]
struct Args {
    #[clap(long, value_name = "FILE", required = true)]
    json: String,
    #[clap(long, value_name = "FILE", required = true)]
    dylib: String,
    #[clap(long, value_name = "CORES", required = false, default_value = "1")]
    workers: usize,
    #[clap(
        long,
        value_name = "CORE_OFFSET",
        required = false,
        default_value = "1"
    )]
    core_offset: usize,
    #[clap(
        long,
        value_name = "SYSTEM_THREADS",
        required = false,
        default_value = "1",
        help = "Number of threads for resolution operation"
    )]
    system_threads: usize,
    #[clap(
        long,
        value_name = "RECEIVER_THREADS",
        required = false,
        default_value = "1",
        help = "Number of threads for resolution operation"
    )]
    receiver_threads: usize,
    #[clap(
        long,
        value_name = "MAX_RUNTIME",
        required = false,
        default_value = "0"
    )]
    max_runtime: u64,
    #[clap(long, value_name = "FILE", required = false, default_value = "stdout")]
    output: String,
    #[clap(long, help = "Enable fifo scheduler")]
    fifo: bool,
    #[clap(long, help = "Enable custom lock-free priority scheduler")]
    custom: bool,
    #[clap(long, help = "Print Initializations to stdout")]
    inits: bool,
    #[clap(long, help = "Enable Debug Printing")]
    debug: bool,
    #[clap(long, value_name = "SLOTS", required = false, default_value = "1")]
    slots: usize,
    #[clap(
        long,
        value_name = "MAX_STREAMS",
        required = false,
        default_value = "1"
    )]
    max_streams: usize,
    #[clap(long, value_name = "FILE", required = false, help = "Enable timing")]
    timing: Option<String>,
    #[clap(long, help = "Enable scheduler recording", required = false)]
    record: bool,
    #[clap(
        long,
        value_name = "STREAM_ID",
        required = false,
        help = "Record only a specific stream (memory optimization)"
    )]
    record_stream: Option<usize>,
    #[clap(long, help = "Use rdtsc for timing")]
    use_rdtsc: bool,
    #[clap(
        long,
        value_name = "BATCHING_SIZE",
        required = false,
        default_value = "1",
        help = "Number of completed tasks to batch before processing"
    )]
    batching_size: usize,
    #[clap(
        long,
        value_name = "BATCHING_LIMIT",
        required = false,
        default_value = "10",
        help = "Maximum time to wait for batch in microseconds"
    )]
    batching_limit: u64,
    #[clap(
        long,
        help = "Enable slot-priority processing (process slots sequentially with automatic round-robin for better cache locality)"
    )]
    slot_priority: bool,
    #[clap(
        long,
        help = "Coalesce barrier fan-outs into bulk tasks (min(N, workers) tasks instead of N). Only enable for fine-grained workloads where per-task compute << spawn overhead (e.g. wavefront). Disabled by default to avoid serialising coarse-grained tasks (MIMO, PageRank)."
    )]
    coalesce_barriers: bool,
    #[clap(
        long,
        help = "Reserve one ready successor for inline execution on the completing worker thread instead of spawning via the scheduler. Eliminates scheduler round-trip for chain-dominant graphs (factor=1 chains). Disabled by default; must NOT enable for coarse-grained workloads (MIMO) where serialising a successor increases latency."
    )]
    inline_continuation: bool,
    #[clap(
        long,
        value_name = "EXCLUDE_STREAMS",
        required = false,
        default_value = "0",
        help = "Number of initial streams to exclude from timing statistics (for steady-state measurement)"
    )]
    exclude_streams: usize,
    #[clap(
        long,
        value_name = "FILE",
        required = false,
        help = "Write JSON performance report to FILE"
    )]
    report: Option<String>,
    #[clap(
        long,
        value_name = "N",
        required = false,
        default_value = "65536",
        help = "Capacity of the lock-free task-completion ring-buffer"
    )]
    batch_queue_capacity: usize,
    #[clap(
        long,
        value_name = "N",
        required = false,
        default_value = "32",
        help = "Spin iterations before blocking recv in resolution threads"
    )]
    spin_iterations: u32,
    #[clap(
        long,
        value_name = "N",
        required = false,
        default_value = "32",
        help = "Flush accumulated successors to workers every N items during batch processing"
    )]
    sched_flush_threshold: usize,
    #[clap(
        long,
        value_name = "BYTES",
        required = false,
        default_value = "16777216",
        help = "UDP socket kernel receive buffer size in bytes"
    )]
    socket_recv_buf_bytes: usize,
    #[clap(
        long,
        value_name = "N",
        required = false,
        default_value = "1024",
        help = "Pre-allocated packet buffer pool size per receiver thread"
    )]
    recv_pool_size: usize,
    #[clap(
        long,
        value_name = "N",
        required = false,
        default_value = "64",
        help = "spin_wait: iterations of spin_loop() before switching to yield_now()"
    )]
    spin_wait_spin_iters: u32,
    #[clap(
        long,
        value_name = "N",
        required = false,
        default_value = "256",
        help = "spin_wait: iterations of yield_now() before switching to park_timeout()"
    )]
    spin_wait_yield_iters: u32,
    #[clap(
        long,
        value_name = "NS",
        required = false,
        default_value = "100",
        help = "spin_wait: park_timeout duration in nanoseconds"
    )]
    spin_wait_park_ns: u64,
}

fn main() {
    let args = Args::parse();

    // Initialize tracing subscriber. RUST_LOG overrides the --debug flag.
    let default_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level)),
        )
        .init();

    // init_debug is now a no-op; kept for source compatibility.
    init_debug(args.debug);

    let _stdout_guard = if args.output != "stdout" {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&args.output)
            .expect("Failed to create output file");
        tracing::debug!("output file opened");

        // Redirect stdout to file (captures any remaining println! from plugins etc.)
        Some(gag::Redirect::stdout(file).expect("Failed to redirect stdout"))
    } else {
        None
    };

    {
        // set PLUGIN_LIB environment variable
        unsafe { std::env::set_var("PLUGIN_LIB", args.dylib) };
        tomii_core::wrappers::init_wrappers();
    }

    let runtime = match args.max_runtime {
        0 => None,
        _ => Some(args.max_runtime),
    };

    let scheduler_type = if args.fifo {
        SchedulerType::Fifo
    } else if args.custom {
        SchedulerType::Custom
    } else {
        SchedulerType::WorkStealing
    };

    tracing::debug!("starting graph initialization");
    let spec = from_json(&args.json, args.workers).expect("Failed to parse graph from JSON file");
    tracing::debug!("graph initialized");
    // check if inits flag is set
    if args.inits {
        println!();
        spec.graph.print_graph();
        println!();
        println!("Initialized Objects:");
        for (id, obj) in spec.init_objects.iter().enumerate() {
            println!("  {}: {:?}", id, obj);
        }
        println!();
    }
    tracing::debug!("objects initialized");

    // Eagerly calibrate RDTSC frequency once at startup (avoids 1M-iteration loop on hot path)
    if args.use_rdtsc {
        utils_rdtsc::init_rdtsc_freq();
    }

    let timing_enabled = args.timing.is_some();

    let synrt = run_graph(
        spec,
        RunGraphConfig {
            scheduler_type,
            workers: args.workers,
            core_offset: args.core_offset,
            system_threads: args.system_threads,
            receiver_threads: args.receiver_threads,
            slots: args.slots,
            max_streams: args.max_streams,
            max_runtime: runtime,
            record: args.record,
            record_stream: args.record_stream,
            use_rdtsc: args.use_rdtsc,
            timing_enabled,
            slot_priority_enabled: args.slot_priority,
            coalesce_barriers: args.coalesce_barriers,
            inline_continuation: args.inline_continuation,
            batch_queue_capacity: args.batch_queue_capacity,
            socket_recv_buf_bytes: args.socket_recv_buf_bytes,
            recv_pool_size: args.recv_pool_size,
            spin_wait: SpinWaitConfig {
                spin_iters: args.spin_wait_spin_iters,
                yield_iters: args.spin_wait_yield_iters,
                park_ns: args.spin_wait_park_ns,
            },
            batch: BatchConfig {
                target_size: args.batching_size,
                timeout_us: args.batching_limit,
                poll_spin_iters: args.spin_iterations,
                flush_threshold: args.sched_flush_threshold,
            },
        },
    );

    let time_file = args.timing;
    if let Some(time_file) = &time_file {
        let time_name = time_file.split('/').last().unwrap_or_default();
        synrt.print_statistics(&time_name, Some(&time_file), args.exclude_streams);

        if args.record {
            // remove  extension if present
            let time_name = time_name.split('.').next().unwrap_or_default();
            let path = PathBuf::from(&time_file);
            let dir = path.parent().unwrap();
            let csv_file = dir.join(format!("{}_sched.csv", time_name));
            synrt.write_record(csv_file.to_str().unwrap());
        }
    } else {
        if args.record {
            synrt.write_record("scheduler_record.csv");
        }
    }

    if let Some(report_path) = &args.report {
        synrt.write_json_report(report_path, args.exclude_streams);
    }
}

pub struct RunGraphConfig {
    pub scheduler_type: SchedulerType,
    pub workers: usize,
    pub core_offset: usize,
    pub system_threads: usize,
    pub receiver_threads: usize,
    pub slots: usize,
    pub max_streams: usize,
    pub max_runtime: Option<u64>,
    pub record: bool,
    pub record_stream: Option<usize>,
    pub use_rdtsc: bool,
    pub timing_enabled: bool,
    pub slot_priority_enabled: bool,
    pub coalesce_barriers: bool,
    pub inline_continuation: bool,
    pub batch_queue_capacity: usize,
    pub socket_recv_buf_bytes: usize,
    pub recv_pool_size: usize,
    pub spin_wait: SpinWaitConfig,
    pub batch: BatchConfig,
}

pub fn run_graph(spec: GraphSpec, cfg: RunGraphConfig) -> TomiiRt {
    let RunGraphConfig {
        scheduler_type,
        workers,
        core_offset,
        system_threads,
        receiver_threads,
        slots,
        max_streams,
        max_runtime,
        record,
        record_stream,
        use_rdtsc,
        timing_enabled,
        slot_priority_enabled,
        coalesce_barriers,
        inline_continuation,
        batch_queue_capacity,
        socket_recv_buf_bytes,
        recv_pool_size,
        spin_wait,
        batch,
    } = cfg;
    let receiver_threads = if spec.graph.network_config().is_some() {
        receiver_threads
    } else {
        0
    };

    // Create a single AsyncRecorder sized for all threads: workers + network + system
    let total_recorders = workers + system_threads + receiver_threads;
    let shared_recorder = if record {
        Some(std::sync::Arc::new(
            tomii_core::async_recorder::AsyncRecorder::new(total_recorders, 1000),
        ))
    } else {
        None
    };

    let base_instant = Instant::now();

    // Scan graph for unique use_workers specs to create worker affinity configuration
    let worker_affinity = {
        use std::collections::HashSet;
        use tomii_core::scheduler::WorkerAffinityConfig;
        use tomii_core::WorkerRangeSpec;

        let mut unique_worker_specs: HashSet<WorkerRangeSpec> = HashSet::new();
        for node in &spec.graph.nodes {
            if let Some(ref ws) = node.use_workers {
                unique_worker_specs.insert(ws.clone());
            }
        }
        if let Some(ref post_nodes) = spec.graph.post_nodes {
            for node in post_nodes {
                if let Some(ref ws) = node.use_workers {
                    unique_worker_specs.insert(ws.clone());
                }
            }
        }

        if !unique_worker_specs.is_empty() {
            tracing::info!(
                count = unique_worker_specs.len(),
                "detected unique worker specs"
            );
            for ws in &unique_worker_specs {
                tracing::debug!(%ws, "worker spec");
            }
            Some(WorkerAffinityConfig::from_worker_specs(
                &unique_worker_specs,
                workers,
            ))
        } else {
            None
        }
    };

    let scheduler = create_scheduler(SchedulerConfig {
        scheduler_type,
        core_offset,
        num_workers: workers,
        record,
        external_recorder: shared_recorder.clone(),
        base_instant,
        system_threads,
        receiver_threads,
        target_batch_size: batch.target_size,
        batch_timeout_us: batch.timeout_us,
        worker_affinity,
    });

    if let Some(core_id) = scheduler.main_core() {
        core_affinity::set_for_current(core_id);
        tracing::info!(core = core_id.id, "pinned main thread");
    }

    // Compile the parsed graph into the IR (resolves function pointers, builds routing tables).
    // This step is separate from the runtime builder so transformation passes can be
    // inserted on `spec.graph` before this point.
    let compiled = spec.compile(&scheduler);

    let mut synrt = TomiiRtBuilder::new(compiled, scheduler)
        .base_instant(base_instant)
        .slots(slots)
        .max_streams(max_streams)
        .max_runtime(max_runtime)
        .use_rdtsc(use_rdtsc)
        .record(record)
        .record_stream(record_stream)
        .timing_enabled(timing_enabled)
        .slot_priority_enabled(slot_priority_enabled)
        .async_recorder(shared_recorder)
        .coalesce_barriers(coalesce_barriers)
        .inline_continuation(inline_continuation)
        .batch_queue_capacity(batch_queue_capacity)
        .socket_recv_buf_bytes(socket_recv_buf_bytes)
        .recv_pool_size(recv_pool_size)
        .spin_wait(spin_wait)
        .batch(batch)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Config error: {e}");
            std::process::exit(1);
        });

    synrt.run().unwrap_or_else(|e| {
        eprintln!("Runtime error: {e}");
        std::process::exit(1);
    });
    synrt
}
