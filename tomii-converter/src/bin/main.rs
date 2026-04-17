//! CLI entry point for `tomii-converter`.
//!
//! Usage:
//!   tomii-converter --input <file.rs> --wrappers <out.rs> --registry <out.rs>

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "tomii-converter",
    about = "Generate Τομί wrappers and registry from a Rust plugin source file"
)]
struct Args {
    /// Path to the input Rust source file (plugin lib.rs)
    #[arg(short, long)]
    input: PathBuf,

    /// Path to write the generated wrappers.rs
    #[arg(short, long)]
    wrappers: PathBuf,

    /// Path to write the generated func_reg.rs
    #[arg(short, long)]
    registry: PathBuf,
}

fn main() {
    let args = Args::parse();

    if let Err(e) =
        tomii_converter::generate_from_file(&args.input, &args.wrappers, &args.registry)
    {
        eprintln!("tomii-converter error: {e}");
        std::process::exit(1);
    }

    println!(
        "Generated {} and {}",
        args.wrappers.display(),
        args.registry.display()
    );
}
