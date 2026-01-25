use clap::Parser;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::time::Instant;
use synstream_core::debug::*;
use synstream_core::graph::Graph;
use synstream_core::graph_gen::from_json;
use synstream_core::runtime::SynRt;
use synstream_core::scheduler::{create_scheduler, SchedulerType};

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
}

fn main() {
    let args = Args::parse();

    init_debug(args.debug);

    let _stdout_guard = if args.output != "stdout" {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&args.output)
            .expect("Failed to create output file");
        print_debug(|| "Output file opened".to_string());

        // Redirect stdout to file
        Some(gag::Redirect::stdout(file).expect("Failed to redirect stdout"))
    } else {
        None
    };

    {
        // set PLUGIN_LIB environment variable
        unsafe { std::env::set_var("PLUGIN_LIB", args.dylib) };
        synstream_core::wrappers::init_wrappers();
    }

    let runtime = match args.max_runtime {
        0 => None,
        _ => Some(args.max_runtime),
    };

    let scheduler_type = if args.fifo {
        SchedulerType::Fifo
    } else {
        SchedulerType::WorkStealing
    };

    print_debug(|| "Starting Graph Initialization".to_string());
    let graph = from_json(&args.json, args.workers).expect("Failed to parse graph from JSON file");
    print_debug(|| "Graph Initialized".to_string());
    // check if inits flag is set
    if args.inits {
        println!();
        graph.print_graph();
        println!();
        graph.print_init_objects();
        println!();
    }
    print_debug(|| "Objects Initialized".to_string());

    let timing_enabled = args.timing.is_some();

    let synrt = run_graph(
        &graph,
        scheduler_type,
        args.workers,
        args.core_offset,
        args.system_threads,
        args.receiver_threads,
        args.slots,
        args.max_streams,
        runtime,
        args.record,
        args.record_stream,
        args.use_rdtsc,
        timing_enabled,
        args.batching_size,
        args.batching_limit,
        args.slot_priority,
    );

    let time_file = args.timing;
    if let Some(time_file) = &time_file {
        let time_name = time_file.split('/').last().unwrap_or_default();
        synrt.print_statistics(&time_name, Some(&time_file));

        if args.record {
            // remove  extension if present
            let time_name = time_name.split('.').next().unwrap_or_default();
            let path = PathBuf::from(&time_file);
            let dir = path.parent().unwrap();
            let csv_file = dir.join(format!("{}_sched.csv", time_name));
            synrt.write_record(csv_file.to_str().unwrap());
        }
    }
}

pub fn run_graph(
    graph: &Graph,
    scheduler_type: SchedulerType,
    workers: usize,
    core_offset: usize,
    system_threads: usize,
    receiver_threads: usize,
    slots: usize,
    max_streams: usize,
    max_runtime: Option<u64>,
    record: bool,
    record_stream: Option<usize>,
    use_rdtsc: bool,
    timing_enabled: bool,
    batching_size: usize,
    batching_limit: u64,
    slot_priority_enabled: bool,
) -> SynRt {
    let receiver_threads = if graph.network_config().is_some() {
        receiver_threads
    } else {
        0
    };

    // Create a single AsyncRecorder sized for all threads: workers + network + system
    let total_recorders = workers + system_threads + receiver_threads;
    let shared_recorder = if record {
        Some(std::sync::Arc::new(
            synstream_core::async_recorder::AsyncRecorder::new(total_recorders, 1000),
        ))
    } else {
        None
    };

    let base_instant = Instant::now();

    // Create available_stream_slots early so it can be shared with scheduler
    use parking_lot::{Mutex, RwLock};
    use std::sync::Arc;
    let available_stream_slots = Arc::new(RwLock::new(vec![usize::MAX; slots]));

    // Phase 2: Create batch infrastructure before scheduler (shared between scheduler and runtime)
    use std::sync::atomic::AtomicUsize;
    let batch_buffer = Arc::new(Mutex::new(Vec::with_capacity(batching_size)));
    let batch_last_sent = Arc::new(Mutex::new(Instant::now()));
    let (flush_notify_tx, flush_notify_rx) = crossbeam_channel::unbounded();
    let flusher_shutdown = Arc::new(AtomicUsize::new(0));

    let scheduler = create_scheduler(
        scheduler_type,
        core_offset,
        workers,
        record,
        shared_recorder.clone(),
        base_instant,
        system_threads,
        receiver_threads,
        batching_size,
        batching_limit,
        record_stream,
        Arc::clone(&available_stream_slots),
        batch_buffer.clone(),
        batch_last_sent.clone(),
        flush_notify_rx,
        flush_notify_tx.clone(),
        flusher_shutdown.clone(),
    );

    let mut synrt = SynRt::new(
        graph,
        slots,
        max_streams,
        max_runtime,
        use_rdtsc,
        record,
        record_stream,
        timing_enabled,
        scheduler,
        base_instant,
        slot_priority_enabled,
        shared_recorder,
        available_stream_slots,
        // Phase 2: Pass shared batch infrastructure
        batch_buffer,
        batch_last_sent,
        batching_size,
        flush_notify_tx,
        flusher_shutdown,
    );

    synrt.run();
    synrt
}
