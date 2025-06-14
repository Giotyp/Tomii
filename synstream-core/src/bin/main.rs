use clap::Parser;
use std::fs::OpenOptions;
use synstream_core::clerk::Clerk;
use synstream_core::graph_gen::from_json;
use synstream_core::graph_struct::Graph;
use synstream_core::scheduler::Scheduler;

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
    #[clap(long, help = "Print Initializations to stdout")]
    inits: bool,
}

fn main() {
    let args = Args::parse();

    if let Some(dylib) = &args.dylib {
        // set PLUGIN_LIB environment variable
        unsafe { std::env::set_var("PLUGIN_LIB", dylib) };
        synstream_core::wrappers::init_wrappers();
    }

    let runtime = match args.max_runtime {
        0 => None,
        _ => Some(args.max_runtime),
    };

    let _stdout_guard = if args.output != "stdout" {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&args.output)
            .expect("Failed to create output file");

        // Redirect stdout to file
        Some(gag::Redirect::stdout(file).expect("Failed to redirect stdout"))
    } else {
        None
    };

    let graph = from_json(&args.json).expect("Failed to parse graph from JSON file");
    // check if inits flag is set
    if args.inits {
        println!();
        graph.print_init_objects();
        println!();
    }

    let clerk = run_graph(&graph, args.workers, args.core_offset, runtime);

    // print results
    clerk.print_all_results();
}

pub fn run_graph(
    graph: &Graph,
    workers: usize,
    core_offset: usize,
    max_runtime: Option<u64>,
) -> Clerk {
    let mut clerk = Clerk::new();
    let scheduler = Scheduler::new(core_offset, workers);

    clerk.run(graph, scheduler, max_runtime);
    clerk
}
