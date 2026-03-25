"""Machine-readable schema for SynStream graph construction parameters."""


def graph_schema() -> dict:
    """Return a JSON-serializable schema describing the graph Python API."""
    return {
        "overview": (
            "A SynStream application is a directed acyclic graph (DAG) of nodes. "
            "Nodes are Rust or C plugin functions compiled into a shared library. "
            "The graph is defined in Python with graph.var() and graph.node(), then "
            "executed with graph.run()."
        ),
        "graph_construction": {
            "var": {
                "description": "Define an initialization variable (computed once before streaming begins).",
                "parameters": {
                    "name": {"type": "str", "description": "Unique node name"},
                    "func": {"type": "str", "description": "Plugin function name (optional; use value= for constants)"},
                    "args": {"type": "list", "description": "Arguments to pass to func (see arg_types)"},
                    "factor": {"type": "int | Var", "description": "Number of instances to create (for vectorized inits)"},
                },
                "returns": "Var — a handle that can be referenced in node args via $ref",
            },
            "node": {
                "description": "Define a computation node, executed once per stream.",
                "parameters": {
                    "name": {"type": "str", "description": "Unique node name"},
                    "func": {"type": "str", "description": "Plugin function name"},
                    "args": {"type": "list", "description": "Arguments to pass to func (see arg_types)"},
                    "factor": {
                        "type": "int | Var",
                        "description": (
                            "Number of parallel task instances per stream. "
                            "Controls the granularity of parallelism for this node."
                        ),
                        "optimization_hint": (
                            "Large factor (>256) creates many fine-grained tasks and increases "
                            "scheduling overhead. If report.json shows "
                            "summary.scheduling_overhead_diagnostic.overhead_pct > 60%, reduce "
                            "factor via a coarsening parameter (e.g. tile_size in your graph "
                            "builder, or group_size on this node). "
                            "Typical sweet spot: factor 8–64 for compute-bound tasks."
                        ),
                        "example": 8,
                    },
                    "group_size": {
                        "type": "int",
                        "description": "Group consecutive task instances into batches for scheduling efficiency.",
                        "optimization_hint": (
                            "Equivalent to dividing factor by group_size for scheduling purposes — "
                            "N tasks become N/group_size scheduled units with no graph change. "
                            "Use when graph topology is fixed but task overhead is too high. "
                            "Try group_size=8 when factor>64 and overhead_pct is high."
                        ),
                        "example": 8,
                    },
                    "priority": {
                        "type": "int",
                        "description": "Scheduling priority. Higher value = scheduled first. Default 0.",
                    },
                    "use_workers": {
                        "type": "str",
                        "description": "Pin this node's tasks to a named worker pool (advanced; requires custom pool setup).",
                    },
                    "loop": {
                        "type": "Node | Var",
                        "description": "Enable looping: re-execute this node until a condition is met.",
                    },
                    "condition": {
                        "type": "Node | Var",
                        "description": "Conditional execution: only run this node if the referenced node's output is truthy.",
                    },
                },
                "returns": "Node — a handle that can be referenced in subsequent node args",
            },
        },
        "arg_types": {
            "$ref": {
                "syntax": "var_handle  (a Var object returned by graph.var())",
                "description": "Reference to an initialization variable. Passed as a pointer to the pre-computed result.",
                "example": "graph.node('process', func='process_cm', args=[data_var, idx])",
            },
            "$res": {
                "syntax": "node_handle  (a Node object returned by graph.node())",
                "description": "Data dependency: consume the output of a predecessor node. Creates an edge in the DAG.",
                "example": "graph.node('consume', func='consume_cm', args=[producer_node])",
            },
            "$dep": {
                "syntax": "('dep', node_handle)",
                "description": "Control dependency: wait for a predecessor node to complete without consuming its output.",
                "example": "graph.node('c', func='c_cm', args=[('dep', b_node), x])",
            },
            "$barrier": {
                "syntax": "('barrier', node_handle)",
                "description": (
                    "Fan-in synchronization: wait for ALL factor instances of a predecessor node to complete "
                    "before this node runs. Use when downstream processing requires the full parallel output."
                ),
                "example": "graph.node('reduce', func='reduce_cm', args=[('barrier', worker_node)])",
            },
            "$network": {
                "syntax": "('network', port)",
                "description": "Inject data from a network receiver thread (UDP/TCP). For network-input graphs only.",
            },
            "literal": {
                "syntax": "int | float | str | bool",
                "description": "Constant value passed directly to the plugin function.",
            },
        },
        "discovery_commands": {
            "--list-knobs": "Human-readable list of all graph.run() runtime flags",
            "--list-knobs-json": "Machine-readable JSON of all graph.run() runtime flags with search hints",
            "--schema": "This document",
        },
    }
