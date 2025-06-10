# SynStream

### Task-Graph Framework for Streaming Applications

SynStream automates the process of describing a computational graph and executing it in a specified environment. It focuses on streaming applications, which require low-latency and data-reuse between computation stages in a consumer-producer MIMO pattern.

## How to Use

1. Describe the application using a JSON file (or use SynStream-Visualizer).

2. Obtain (or create) a plugin library compatible with SynStream (dynamic .so file and header file (or Rust source file)).

3. If the application functions utilize explicitly Rust standard types, skip Step 2 and have the source code available.

4. Set **FUNC_PATH** environment variable to header or Rust source code.

5. Execute SynStream. See available arguments with `cargo run -- --help`:
```
Usage: main [OPTIONS] --json <FILE>

Options:
      --json <FILE>                
      --dylib <FILE>               
      --workers <CORES>            [default: 1]
      --core-offset <CORE_OFFSET>  [default: 0]
      --max-runtime <MAX_RUNTIME>  [default: 3]
      --output <FILE>              [default: stdout]
  -h, --help                       Print help
  -V, --version                    Print version
  ```

## Pending Work

1. Create synstream-macro to assist plugin library development.
2. Enhance transformer for correct wrapper creation.