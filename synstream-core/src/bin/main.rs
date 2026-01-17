use clap::Parser;
use std::fs::OpenOptions;
use std::path::PathBuf;
use synstream_core::debug::*;
use synstream_core::graph::Graph;
use synstream_core::graph_gen::from_json;
use synstream_core::runtime::SynRt;
use synstream_core::scheduler::{create_unified_scheduler, SchedulerType};

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
        value_name = "NRX",
        required = false,
        default_value = "1",
        help = "Number of isolated network threads"
    )]
    nrx: usize,
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
        default_value = "100",
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
        args.nrx,
        args.slots,
        args.max_streams,
        runtime,
        args.record,
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
    nrx: usize,
    slots: usize,
    max_streams: usize,
    max_runtime: Option<u64>,
    record: bool,
    use_rdtsc: bool,
    timing_enabled: bool,
    batching_size: usize,
    batching_limit: u64,
    slot_priority_enabled: bool,
) -> SynRt {
    let mut synrt = SynRt::new(
        graph,
        slots,
        max_streams,
        max_runtime,
        use_rdtsc,
        record,
        timing_enabled,
        system_threads,
        nrx,
        slot_priority_enabled,
    );
    let scheduler = create_unified_scheduler(
        scheduler_type,
        core_offset,
        workers,
        nrx, // Network workers
        record,
        synrt.base_instant(),
        system_threads,
        batching_size,
        batching_limit,
    );

    synrt.run(scheduler, system_threads);
    synrt
}
