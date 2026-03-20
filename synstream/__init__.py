"""synstream — Python API for the SynStream task-graph framework.

Typical usage::

    import synstream as ss

    app = ss.Graph()

    buf_size    = app.var("buf_size", 100)
    num_nodes   = app.var("num_nodes", 200)
    fft_planner = app.var("fft_planner", func="fft_planner", args=[buf_size])

    gen_vec     = app.node("gen_vec",     func="generate_vector", factor=num_nodes, args=[buf_size])
    compute_fft = app.node("compute_fft", func="compute_fft",     factor=num_nodes,
                           args=[fft_planner, gen_vec.out(0)])

    # Build: point func_path at your annotated Rust source.
    # The build system auto-generates FFI wrappers from #[synstream_export] annotations.
    app.build(func_path="src/lib.rs", plugin_manifest="Cargo.toml")
    app.run(workers=4, slots=2, timing="timing.txt")
"""

from ._graph import Graph
from ._loop import Condition, IndexFunc, Loop
from ._node import NodeDep, NodeOutput
from ._types import (
    Complex32,
    Complex64,
    String,
    Vec,
    bool_,
    char_,
    f32,
    f64,
    i8,
    i16,
    i32,
    i64,
    i128,
    infer_type,
    isize,
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
)

__all__ = [
    # Core
    "Graph",
    # Node dependency helpers
    "NodeOutput",
    "NodeDep",
    # Helpers
    "Loop",
    "Condition",
    "IndexFunc",
    # Type wrappers
    "usize",
    "isize",
    "i8",
    "i16",
    "i32",
    "i64",
    "i128",
    "u8",
    "u16",
    "u32",
    "u64",
    "u128",
    "f32",
    "f64",
    "String",
    "bool_",
    "char_",
    "Complex32",
    "Complex64",
    "Vec",
    "infer_type",
]
