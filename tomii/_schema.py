"""Machine-readable schema for Τομί graph construction parameters.

Every Python snippet under an "example" key in this schema must be a
complete, self-contained program that builds a ``tomii.Graph`` and assigns
it to a variable named ``app`` (optionally also calling ``app.to_json()``).
``tomii/tests/test_schema.py`` execs each one and serializes the resulting
graph, so a change to the real API (``tomii/_graph.py``, ``tomii/_node.py``,
``tomii/_types.py``) that breaks one of these examples fails the test suite
instead of silently rotting the schema.
"""

from typing import Any


def graph_schema() -> "dict[str, Any]":
    """Return a JSON-serializable schema describing the graph Python API."""
    return {
        "overview": (
            "A Τομί application is a directed acyclic graph (DAG) of nodes. "
            "Nodes are Rust or C plugin functions compiled into a shared library. "
            "The graph is defined in Python with graph.var(), graph.node(), and "
            "graph.post_node(), then executed with graph.run() (optionally preceded "
            "by graph.build()). A node's dependencies on other nodes are NOT passed "
            "as raw tuples — they are built by calling .out()/.dep()/.wait() on the "
            "Node object returned by graph.node(), e.g. producer.out(0)."
        ),
        "graph_construction": {
            "var": {
                "description": "Define an initialization variable (computed once before streaming begins).",
                "parameters": {
                    "name": {"type": "str", "description": "Unique node name"},
                    "value": {
                        "type": "Any",
                        "description": (
                            "Literal constant (int/float/bool/str or a type wrapper like "
                            "tm.String(...)). Mutually exclusive in practice with func= "
                            "(pass one or the other)."
                        ),
                    },
                    "func": {
                        "type": "str",
                        "description": "Plugin function name (optional; omit when using value=)",
                    },
                    "args": {
                        "type": "list",
                        "description": (
                            "Arguments to pass to func: literals, type wrappers (tm.String, "
                            "tm.i32, ...), or other Var handles (serialize as $ref). "
                            "Init args cannot reference node outputs — no .out()/.dep()/.wait()."
                        ),
                    },
                    "factor": {
                        "type": "int | Var",
                        "description": "Number of instances to create (for vectorized inits)",
                    },
                },
                "returns": "Var — a handle that can be referenced in node/init args (serializes as $ref)",
                "example": (
                    "import tomii as tm\n"
                    "app = tm.Graph()\n"
                    "buf_size = app.var('buf_size', 100)\n"
                    "fft_planner = app.var('fft_planner', func='fft_planner', args=[buf_size])\n"
                    "app.node('producer', func='produce', args=[buf_size])\n"
                    "app.to_json()\n"
                ),
            },
            "node": {
                "description": "Define a computation node, executed once per frame.",
                "parameters": {
                    "name": {"type": "str", "description": "Unique node name"},
                    "func": {"type": "str", "description": "Plugin function name"},
                    "args": {
                        "type": "list",
                        "description": (
                            "Arguments to pass to func: literals, type wrappers, Var handles "
                            "($ref), or predecessor dependencies built via node.out()/node.dep()/"
                            "node.wait() — see dependency_methods below."
                        ),
                    },
                    "factor": {
                        "type": "int | Var",
                        "description": (
                            "Number of parallel task instances per frame. "
                            "Controls the granularity of parallelism for this node."
                        ),
                        "optimization_hint": (
                            "Large factor (>256) creates many fine-grained tasks and increases "
                            "scheduling overhead. If report.json shows "
                            "summary.scheduling_overhead_diagnostic.overhead_pct > 60%, reduce "
                            "factor via a coarsening parameter (e.g. tile_size in your graph "
                            "builder, or group_size on this node). "
                            "Typical sweet spot: factor 8-64 for compute-bound tasks."
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
                        "type": "str",
                        "description": (
                            'Scheduling priority: "high", "normal" (default), or "low". '
                            "Biases the scheduler to dispatch this node's tasks ahead of "
                            '("high") or behind ("low") default-priority work when both are ready.'
                        ),
                        "example_value": "high",
                    },
                    "use_workers": {
                        "type": "str",
                        "description": (
                            'Pin this node\'s tasks to a worker index range, e.g. "0-1" for '
                            "workers 0 and 1 (inclusive on both ends; advanced, requires "
                            "matching worker-pool setup)."
                        ),
                    },
                    "loop": {
                        "type": "tomii.Loop",
                        "description": (
                            "Re-execute this node's function for a fixed number of iterations: "
                            "tm.Loop(name, factor) where factor is an int or Var iteration count."
                        ),
                    },
                    "loop_args": {
                        "type": "list",
                        "description": "Per-iteration arguments passed alongside args when loop= is set.",
                    },
                    "condition": {
                        "type": "tomii.Condition",
                        "description": (
                            "Gate execution on a plugin-computed comparison: "
                            "tm.Condition(operation, value, value_type, func, args). "
                            'operation is e.g. "Eq"/"Neq"; the node only runs when '
                            "func(*args) satisfies the comparison against value."
                        ),
                    },
                },
                "returns": (
                    "Node — call .out()/.dep()/.wait() on it to build dependency args for "
                    "successor nodes (see dependency_methods)"
                ),
                "example": (
                    "import tomii as tm\n"
                    "app = tm.Graph()\n"
                    "buf_size = app.var('buf_size', 100)\n"
                    "producer = app.node('producer', func='produce', args=[buf_size])\n"
                    "process = app.node('process', func='process_cm',\n"
                    "                    args=[producer.out(0), buf_size],\n"
                    "                    priority='high')\n"
                    "app.to_json()\n"
                ),
            },
            "post_node": {
                "description": (
                    "Define a node that runs after the frame's compute nodes complete, "
                    "before the slot is released (e.g. per-frame state cleanup). "
                    "Takes the same parameters as node() and serializes into a separate "
                    "post_nodes array."
                ),
                "parameters": "Same as node() above.",
                "returns": "Node",
                "example": (
                    "import tomii as tm\n"
                    "app = tm.Graph()\n"
                    "producer = app.node('producer', func='produce', args=[])\n"
                    "app.post_node('cleanup', func='cleanup_state', args=[producer.dep(0)])\n"
                    "app.to_json()\n"
                ),
            },
        },
        "dependency_methods": {
            "description": (
                "A node argument that depends on a predecessor is built by calling a "
                "method on the predecessor Node object — NOT by passing a tuple like "
                "('dep', node) or passing the Node object directly as an arg. "
                "Signature shared by all three: "
                "node.out(start=0, end=None, *, group_by=None), same for .dep()/.wait()."
            ),
            "out": {
                "serializes_as": "$res",
                "description": "Data dependency: consume the output of a predecessor node. Creates an edge in the DAG.",
                "example": (
                    "import tomii as tm\n"
                    "app = tm.Graph()\n"
                    "producer = app.node('producer', func='produce', args=[])\n"
                    "app.node('consume', func='consume_cm', args=[producer.out(0)])\n"
                    "app.to_json()\n"
                ),
            },
            "dep": {
                "serializes_as": "$dep",
                "description": (
                    "Ordering-only dependency: wait for a predecessor node to complete "
                    "without consuming its output (the arg slot receives None). The "
                    "runtime skips result storage for nodes whose only non-barrier "
                    "successors are $dep edges."
                ),
                "example": (
                    "import tomii as tm\n"
                    "app = tm.Graph()\n"
                    "b = app.node('b', func='b_cm', args=[])\n"
                    "x = app.var('x', 1)\n"
                    "app.node('c', func='c_cm', args=[b.dep(0), x])\n"
                    "app.to_json()\n"
                ),
            },
            "wait": {
                "serializes_as": "$barrier",
                "description": (
                    "Fan-in synchronization: wait for the selected instances of a "
                    "predecessor node to complete before this node runs, without "
                    "fetching a value. Use a range covering all of a factor node's "
                    "instances when downstream processing needs the full parallel output."
                ),
                "example": (
                    "import tomii as tm\n"
                    "app = tm.Graph()\n"
                    "num_nodes = app.var('num_nodes', 200)\n"
                    "worker = app.node('worker', func='work_cm', factor=num_nodes, args=[])\n"
                    "app.node('reduce', func='reduce_cm', args=[worker.wait(0, num_nodes)])\n"
                    "app.to_json()\n"
                ),
            },
            "indexing": {
                "description": (
                    "start/end select predecessor instance offsets, relative to the "
                    "consuming instance (0 = same index, -1 = previous instance). A "
                    "single int/str start with end=None selects exactly one offset. A "
                    "list start (e.g. [0, 1, 2]) selects an exact set of offsets. A "
                    "start/end pair selects a range, and the two endpoint types differ "
                    "in inclusivity: a literal int end is INCLUSIVE (out(0, 5) selects "
                    "offsets 0 through 5 — 6 values), while a Var end is EXCLUSIVE "
                    "(out(0, num_nodes) with num_nodes=200 selects offsets 0 through "
                    "199 — 200 values). Use a Var end (the producer's own factor Var) "
                    "to fan in over exactly all of its instances; a literal int end is "
                    "the classic off-by-one here."
                ),
            },
            "group_by": {
                "description": (
                    "group_by=k on a range dependency splits it into independent groups "
                    "of k instances: the successor fires once per group instead of once "
                    "for the whole range. Example: worker.wait(0, total, group_by=8) with "
                    "total=32 fires compute_stats[i] when worker[i*8 .. (i+1)*8] complete "
                    "— four independent groups instead of one global barrier."
                ),
            },
            "collect_all_must_be_last_arg": {
                "description": (
                    "A dependency whose resolved index count is greater than 1 and does "
                    "not equal the consuming node's factor is a collect-all: it expands "
                    "in place to that many values, spliced into the argument buffer "
                    "starting at that argument's position. If such an argument is not "
                    "the LAST argument to the node, it overruns the slots of the "
                    "arguments after it. The graph loader rejects this at load time with "
                    "an error naming the node, the argument position, and the fix "
                    "(reorder it last, or make it 1:1 — one index, or exactly `factor` "
                    "indexes)."
                ),
            },
        },
        "special_refs": {
            "$index": {
                "description": (
                    "Reserved init name resolved at runtime to the instance index of "
                    "the node consuming it. Not created with graph.var() — referenced "
                    "directly as a $ref value."
                ),
                "usage": "from tomii._types import TypedValue; idx = TypedValue('$ref', '$index')",
                "example": (
                    "import tomii as tm\n"
                    "from tomii._types import TypedValue\n"
                    "app = tm.Graph()\n"
                    "idx = TypedValue('$ref', '$index')\n"
                    "num_nodes = app.var('num_nodes', 200)\n"
                    "app.node('tagged', func='tag_with_index', factor=num_nodes, args=[idx])\n"
                    "app.to_json()\n"
                ),
            },
            "$workers": {
                "description": (
                    "Reserved factor name resolving to the runtime's configured worker "
                    'count. Use as a factor string: factor="$workers".'
                ),
            },
            "$network": {
                "description": (
                    "Virtual predecessor node for network-input graphs (graph.network()). "
                    "Not created with graph.node() — reference it by constructing a Node "
                    'handle named "$network" and calling .out()/.wait() on it, as '
                    "examples/radar-pipeline/build_graph.py does: "
                    "Node('$network', func='').out(0, n_chirps)."
                ),
            },
        },
        "arg_types": {
            "$ref": {
                "syntax": "var_handle  (a Var object returned by graph.var(), or a TypedValue for $index/$workers)",
                "description": "Reference to an initialization variable. Passed as a pointer to the pre-computed result.",
            },
            "$res": {
                "syntax": "node_handle.out(start=0, end=None, *, group_by=None)",
                "description": "Data dependency: consume the output of a predecessor node. Creates an edge in the DAG.",
            },
            "$dep": {
                "syntax": "node_handle.dep(start=0, end=None, *, group_by=None)",
                "description": "Control dependency: wait for a predecessor node to complete without consuming its output.",
            },
            "$barrier": {
                "syntax": "node_handle.wait(start=0, end=None, *, group_by=None)",
                "description": (
                    "Fan-in synchronization: wait for ALL factor instances of a predecessor node to complete "
                    "before this node runs. Use when downstream processing requires the full parallel output."
                ),
            },
            "$network": {
                "syntax": "see special_refs.$network",
                "description": "Inject data from a network receiver thread (UDP/TCP). For network-input graphs only.",
            },
            "literal": {
                "syntax": "int | float | str | bool | tm.<type>(...)",
                "description": (
                    "Constant value passed directly to the plugin function. Plain "
                    "Python int/float/bool auto-infer to usize/f64/bool; use an explicit "
                    "wrapper (tm.i32, tm.String, tm.Vec, ...) for any other type."
                ),
            },
        },
        "discovery_commands": {
            "--list-knobs": "Human-readable list of all graph.run() runtime flags",
            "--list-knobs-json": "Machine-readable JSON of all graph.run() runtime flags with search hints",
            "--knob-space [graph.json]": "Generated tuning search space: perf-role runtime knobs plus per-graph knobs (factors, group widths) with value domains — consumable by any optimizer",
            "--schema": "This document",
        },
    }
