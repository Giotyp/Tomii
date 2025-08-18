use clap::Parser;
use std::fs::OpenOptions;
use synstream_core::clerk::Clerk;
use synstream_core::debug::*;
use synstream_core::graph_gen::from_json;
use synstream_core::graph_struct::Graph;
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
        default_value = "0"
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
        print_debug("Output file opened");

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

    print_debug("Args Parsed");

    let runtime = match args.max_runtime {
        0 => None,
        _ => Some(args.max_runtime),
    };
    print_debug("Runtime");

    let scheduler_type = if args.fifo {
        SchedulerType::Fifo
    } else {
        SchedulerType::WorkStealing
    };

    print_debug("Starting Graph Initialization");
    let graph = from_json(&args.json, args.workers).expect("Failed to parse graph from JSON file");
    print_debug("Graph Initialized");
    // check if inits flag is set
    if args.inits {
        println!();
        graph.print_init_objects();
        println!();
    }
    print_debug("Objects Initialized");

    let _clerk = run_graph(
        &graph,
        scheduler_type,
        args.workers,
        args.core_offset,
        args.slots,
        args.max_streams,
        runtime,
    );
}

pub fn run_graph(
    graph: &Graph,
    scheduler_type: SchedulerType,
    workers: usize,
    core_offset: usize,
    slots: usize,
    max_streams: usize,
    max_runtime: Option<u64>,
) -> Clerk {
    let mut clerk = Clerk::new(graph, slots, max_streams, max_runtime);
    let scheduler = create_scheduler(scheduler_type, core_offset, workers);

    clerk.run(scheduler);
    clerk
}
