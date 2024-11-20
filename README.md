# JRaph 🦒

### Task-Graph Framework for Streaming Applications

The JRaph library provides an API that automates the process of describing a computational graph and executing it in a specified environment. It focuses on streaming applications, which require low-latency and data-reuse between computation stages in a consumer-producer MIMO pattern.

## Features

1. Use the [`from_json`](src/graph_gen.rs) function to generate a [`Graph`](src/graph_struct.rs) object from a JSON file.

2. The JSON file (example in [graph.json](examples/graphs/graph.json)) has to follow certain rules, to describe the Graph structure; application **stages**, **nodes** and **tasks**.

3. The procedural macros in [cst_macros](cst_macros/src/lib.rs) can execute given task functions with/without arguments.

4. The python script, [translator.py](translator.py) is executed at compilation time, to modify function signatures in a format that JRaph accepts.
Currently, functions can be given in the following way:

    * The environment variable `FUNC_PATH` needs to point to the function file.

    * The function file must be a Rust **.rs** file.

    * Wrapper functions are generated in the same directory as the functino file provided and import those Rust functions.

## Future Work

1. Wrapper functions should link with the respective binaries.

2. Translator support for **C/C++** binaries and given header files. 

3. Executor that creates an environment with available CPU threads and schedules tasks of the graph.