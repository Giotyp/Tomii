use clap::Parser;
use std::fs::OpenOptions;
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
    #[clap(long, value_name = "FILE", required = false)]
    dylib: Option<String>,
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
        value_name = "MAX_RUNTIME",
        required = false,
        default_value = "3"
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
    timing: String,
    #[clap(long, help = "Enable scheduler recording")]
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

    if let Some(dylib) = &args.dylib {
        // set PLUGIN_LIB environment variable
        unsafe { std::env::set_var("PLUGIN_LIB", dylib) };
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

    let synrt = run_graph(
        &graph,
        scheduler_type,
        args.workers,
        args.core_offset,
        args.slots,
        args.max_streams,
        runtime,
        args.record,
        args.use_rdtsc,
        args.batching_size,
        args.batching_limit,
    );

    let time_file = args.timing;
    if !time_file.is_empty() {
        let time_name = time_file.split('/').last().unwrap_or_default();
        synrt.print_statistics(&time_name, Some(&time_file));

        if args.record {
            // remove  extension if present
            let time_name = time_name.split('.').next().unwrap_or_default();
            synrt.write_record(&format!("{}_schedule.csv", time_name));
        }
    }
}

pub fn run_graph(
    graph: &Graph,
    scheduler_type: SchedulerType,
    workers: usize,
    core_offset: usize,
    slots: usize,
    max_streams: usize,
    max_runtime: Option<u64>,
    record: bool,
    use_rdtsc: bool,
    batching_size: usize,
    batching_limit: u64,
) -> SynRt {
    let mut synrt = SynRt::new(graph, slots, max_streams, max_runtime, use_rdtsc, record);
    let scheduler = create_scheduler(
        scheduler_type,
        core_offset,
        workers,
        record,
        synrt.base_instant(),
    );
    synrt.run(scheduler, batching_size, batching_limit);
    synrt
}
