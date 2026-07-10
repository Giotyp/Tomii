use clap::Parser;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::time::Instant;

use tomii_core::debug::init_debug;
use tomii_core::graph_gen::{from_json, GraphSpec}; // GraphCompiled produced via spec.compile()
use tomii_core::runtime::{BatchConfig, SpinWaitConfig, TomiiRt, TomiiRtBuilder};
use tomii_core::scheduler::{create_scheduler, SchedulerConfig, SchedulerType};
use tomii_core::utils_rdtsc;
use tomii_core::RuntimeConfig;
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
    #[clap(
        long,
        value_name = "SLOTS",
        required = false,
        default_value = "1",
        help = "Concurrent in-flight frames"
    )]
    slots: usize,
    #[clap(
        long,
        alias = "max-streams",
        value_name = "MAX_FRAMES",
        required = false,
        default_value = "1",
        help = "Total frames to process"
    )]
    max_frames: usize,
    #[clap(long, value_name = "FILE", required = false, help = "Enable timing")]
    timing: Option<String>,
    #[clap(
        long,
        value_name = "FILE",
        required = false,
        help = "Write a runtime state snapshot (JSON) to FILE at shutdown; on Unix, \
                SIGUSR1 additionally writes numbered snapshots (FILE.1, FILE.2, ...) \
                while the run is live — useful for diagnosing wedged runs"
    )]
    dump_state: Option<String>,
    #[clap(long, help = "Enable scheduler recording", required = false)]
    record: bool,
    #[clap(
        long,
        alias = "record-stream",
        value_name = "FRAME_ID",
        required = false,
        help = "Record only a specific frame (memory optimization)"
    )]
    record_frame: Option<usize>,
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
        help = "Disable 1:1 fanout-bulk dispatch (Upgrade 5). Produces bit-identical output to the per-cell path. Use for correctness verification."
    )]
    no_fanout_bulk: bool,
    #[clap(
        long,
        alias = "exclude-streams",
        value_name = "EXCLUDE_FRAMES",
        required = false,
        default_value = "0",
        help = "Number of initial frames to exclude from timing statistics (for steady-state measurement)"
    )]
    exclude_frames: usize,
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
    #[clap(
        long,
        value_name = "STRATEGY",
        required = false,
        default_value = "multi-slot-batch",
        help = "Resolution strategy to use. Available: multi-slot-batch"
    )]
    resolution_strategy: String,
}

fn main() {
    let args = Args::parse();

    // Initialize tracing subscriber. RUST_LOG overrides the --debug flag.
    // Disable ANSI colour codes when stdout is redirected to a file so that
    // the output file contains plain text rather than escape sequences.
    let default_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level)),
        )
        .with_ansi(args.output == "stdout")
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

    #[cfg(feature = "embed-python")]
    {
        // Initialize the embedded Python interpreter before loading the bridge
        // dylib so that libpython symbols are globally visible when dlopen runs.
        pyo3::Python::initialize();
        check_python_bridge_abi(&args.dylib);
    }

    {
        // set PLUGIN_LIB environment variable
        unsafe { std::env::set_var("PLUGIN_LIB", &args.dylib) };
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

    let cfg = RuntimeConfig {
        slots: args.slots,
        max_frames: args.max_frames,
        max_runtime: runtime,
        // system_threads, receiver_threads, workers, core_offset, receiver_core_offset,
        // and single_slot_mode are resolved at build time from the scheduler; these
        // initial values are overwritten by TomiiRtBuilder::build().
        system_threads: args.system_threads,
        receiver_threads: args.receiver_threads,
        workers: args.workers,
        core_offset: args.core_offset,
        receiver_core_offset: 0,
        slot_priority_enabled: args.slot_priority,
        coalesce_barriers: args.coalesce_barriers,
        inline_continuation: args.inline_continuation,
        no_fanout_bulk: args.no_fanout_bulk,
        single_slot_mode: args.slots == 1,
        record_frame: args.record_frame,
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
    };

    // Validate --resolution-strategy (v1: only "multi-slot-batch" is registered).
    match args.resolution_strategy.as_str() {
        "multi-slot-batch" => {
            tracing::info!("Strategy: multi-slot-batch (default)");
        }
        unknown => {
            eprintln!("Unknown resolution strategy '{unknown}'. Available: multi-slot-batch");
            std::process::exit(1);
        }
    }

    let synrt = run_graph(
        spec,
        cfg,
        scheduler_type,
        args.use_rdtsc,
        args.record,
        timing_enabled,
        args.batch_queue_capacity,
        args.socket_recv_buf_bytes,
        args.dump_state.clone(),
    );

    let time_file = args.timing;
    if let Some(time_file) = &time_file {
        let time_name = time_file.split('/').next_back().unwrap_or_default();
        synrt.print_statistics(time_name, Some(time_file), args.exclude_frames);

        if args.record {
            // remove  extension if present
            let time_name = time_name.split('.').next().unwrap_or_default();
            let path = PathBuf::from(&time_file);
            let dir = path.parent().unwrap();
            let csv_file = dir.join(format!("{}_sched.csv", time_name));
            synrt.write_record(csv_file.to_str().unwrap());
        }
    } else if args.record {
        synrt.write_record("scheduler_record.csv");
    }

    if let Some(report_path) = &args.report {
        synrt.write_json_report(report_path, args.exclude_frames);
    }
}

#[allow(clippy::too_many_arguments)]
pub fn run_graph(
    spec: GraphSpec,
    mut cfg: RuntimeConfig,
    scheduler_type: SchedulerType,
    use_rdtsc: bool,
    record: bool,
    timing_enabled: bool,
    batch_queue_capacity: usize,
    socket_recv_buf_bytes: usize,
    dump_state: Option<String>,
) -> TomiiRt {
    // Guard: network receiver threads only apply when a network config is present.
    if spec.graph.network_config().is_none() {
        cfg.receiver_threads = 0;
    }

    let workers = cfg.workers;
    let system_threads = cfg.system_threads;
    let receiver_threads = cfg.receiver_threads;

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
        core_offset: cfg.core_offset,
        num_workers: workers,
        record,
        external_recorder: shared_recorder.clone(),
        base_instant,
        system_threads,
        receiver_threads,
        target_batch_size: cfg.batch.target_size,
        batch_timeout_us: cfg.batch.timeout_us,
        worker_affinity,
        worker_hook: None,
    });

    if let Some(core_id) = scheduler.main_core() {
        core_affinity::set_for_current(core_id);
        tracing::info!(core = core_id.id, "pinned main thread");
    }

    // Compile the parsed graph into the IR (resolves function pointers, builds routing tables).
    // This step is separate from the runtime builder so transformation passes can be
    // inserted on `spec.graph` before this point.
    let compiled = spec.compile(&scheduler);

    let record_frame = cfg.record_frame;
    let mut synrt = TomiiRtBuilder::with_config(compiled, scheduler, cfg)
        .base_instant(base_instant)
        .use_rdtsc(use_rdtsc)
        .record(record)
        .record_frame(record_frame)
        .timing_enabled(timing_enabled)
        .async_recorder(shared_recorder)
        .batch_queue_capacity(batch_queue_capacity)
        .socket_recv_buf_bytes(socket_recv_buf_bytes)
        .build()
        .unwrap_or_else(|e| {
            eprintln!("Config error: {e}");
            std::process::exit(1);
        });

    if let Some(path) = &dump_state {
        spawn_state_dump_watcher(synrt.state_dumper(), path.clone());
    }

    synrt.run().unwrap_or_else(|e| {
        eprintln!("Runtime error: {e}");
        std::process::exit(1);
    });

    // Final snapshot at shutdown — the post-mortem view of the completed run.
    if let Some(path) = &dump_state {
        match std::fs::write(path, synrt.state_dumper().dump()) {
            Ok(()) => tracing::info!(path, "runtime state dumped"),
            Err(e) => eprintln!("state dump to {path} failed: {e}"),
        }
    }
    synrt
}

/// SIGUSR1 sets this flag; the watcher thread turns it into a snapshot file.
/// The handler itself only stores an atomic — async-signal-safe.
static DUMP_REQUESTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[cfg(unix)]
extern "C" fn on_sigusr1(_sig: libc::c_int) {
    DUMP_REQUESTED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// Register SIGUSR1 (Unix) and spawn a watcher thread that writes numbered
/// snapshots (`<path>.1`, `<path>.2`, ...) whenever the signal arrives.
/// The thread lives for the rest of the process; snapshots never touch the
/// hot path beyond atomic reads and short read-lock acquisitions.
fn spawn_state_dump_watcher(dumper: tomii_core::runtime::StateDumper, path: String) {
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGUSR1, on_sigusr1 as libc::sighandler_t);
    }
    std::thread::Builder::new()
        .name("state-dump".into())
        .spawn(move || {
            let mut seq = 0usize;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(200));
                if DUMP_REQUESTED.swap(false, std::sync::atomic::Ordering::Relaxed) {
                    seq += 1;
                    let snap_path = format!("{path}.{seq}");
                    match std::fs::write(&snap_path, dumper.dump()) {
                        Ok(()) => eprintln!("[tomii] state snapshot written to {snap_path}"),
                        Err(e) => eprintln!("[tomii] state snapshot failed: {e}"),
                    }
                }
            }
        })
        .expect("failed to spawn state-dump watcher");
}

/// Load the dylib, look for the `tomii_python_bridge_abi` symbol, and abort
/// with a clear diagnostic if the packed Python version doesn't match the
/// version this binary was linked against.
///
/// Expected ABI is computed at runtime from the already-initialized interpreter
/// so no build-time Python resolution is needed in tomii-core's build script.
/// Only compiled when `--features embed-python` is active.
#[cfg(feature = "embed-python")]
fn check_python_bridge_abi(dylib_path: &str) {
    use libloading::{Library, Symbol};

    // Build the expected ABI from the live interpreter. Python is already
    // initialized by Python::initialize() in the caller.
    let expected_abi: u32 = pyo3::Python::attach(|py| {
        let vi = py.version_info();
        let gil_disabled: u32 = if cfg!(Py_GIL_DISABLED) { 1 } else { 0 };
        ((vi.major as u32) << 24) | ((vi.minor as u32) << 16) | (gil_disabled << 15)
    });

    let lib = unsafe {
        Library::new(dylib_path).unwrap_or_else(|e| {
            eprintln!("tomii: failed to pre-load bridge dylib for ABI check: {e}");
            std::process::exit(1);
        })
    };

    // Non-Python plugins won't have this symbol — skip the check silently.
    let bridge_abi: u32 = unsafe {
        match lib.get::<unsafe extern "C" fn() -> u32>(b"tomii_python_bridge_abi\0") {
            Ok(sym) => {
                let f: Symbol<unsafe extern "C" fn() -> u32> = sym;
                f()
            }
            Err(_) => return,
        }
    };

    if bridge_abi != expected_abi {
        eprintln!(
            "tomii: Python bridge ABI mismatch!\n\
             \x20 Binary expects: Python {}.{} (GIL_DISABLED={})\n\
             \x20 Bridge reports: Python {}.{} (GIL_DISABLED={})\n\
             Rebuild the bridge with the same Python interpreter used to build this binary,\n\
             or pass python_interpreter= matching the bridge's Python version to app.build().",
            (expected_abi >> 24) & 0xFF,
            (expected_abi >> 16) & 0xFF,
            (expected_abi >> 15) & 1,
            (bridge_abi >> 24) & 0xFF,
            (bridge_abi >> 16) & 0xFF,
            (bridge_abi >> 15) & 1,
        );
        std::process::exit(1);
    }

    tracing::debug!(
        abi = format!("0x{:08X}", bridge_abi),
        "Python bridge ABI verified"
    );
}
