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

    * The function file can either be a Rust **.rs** file or a C/C++ header **.h** file.

    * The wrapper functions are generated in the same direction as the function file.

    * If the functions are given in Rust, the wrappers import those Rust functions. An example is given in [rust_funcs](examples/rust_funcs/)

    * If the functions are given in a C/C++ header, they must be accompanied by a respected **.so** shared library file. During compilation, the *build.rs* links with the **.so** library and the generate wrappers call the functions defined. An example is given in [cpp_funcs](examples/cpp_funcs/)

## Future Work

1. Executor that creates an environment with available CPU threads and schedules tasks of the graph.