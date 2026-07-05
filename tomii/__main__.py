"""Entry point for `python -m tomii`.

Usage:
    python -m tomii --list-knobs         # human-readable list of graph.run() options
    python -m tomii --list-knobs-json    # machine-readable JSON of graph.run() options
    python -m tomii --knob-space [graph.json] [--workload NAME]
                                         # generated tuning search space (CLI +
                                         # per-graph knobs when a graph is given)
    python -m tomii --schema             # JSON schema for graph construction parameters
    python -m tomii --dump graph.json [--out FILE]
                                         # emit graph topology as GraphViz DOT
                                         # (runtime state snapshots: run the binary
                                         # with --dump-state FILE; SIGUSR1 for live)
    python -m tomii --help               # same as --list-knobs

    python -m tomii --visualize graph.json          # view mode (read-only)
    python -m tomii --visualize graph.json --edit   # edit mode (save back to file)
    python -m tomii --visualize new.json            # create mode (file doesn't exist)
    python -m tomii --visualize graph.json --ascii  # terminal ASCII art
    python -m tomii --visualize graph.json --port 8080  # custom port
"""

import json
import sys

from ._runner import list_knobs, list_knobs_json


def main() -> None:
    args = sys.argv[1:]

    if not args or "--help" in args or "-h" in args or "--list-knobs" in args:
        print(list_knobs())
    elif "--list-knobs-json" in args:
        print(json.dumps(list_knobs_json(), indent=2))
    elif "--knob-space" in args:
        _cmd_knob_space(args)
    elif "--schema" in args:
        from ._schema import graph_schema

        print(json.dumps(graph_schema(), indent=2))
    elif "--dump" in args:
        _cmd_dump(args)
    elif "--visualize" in args:
        _cmd_visualize(args)
    else:
        print(f"Unknown argument(s): {args}")
        print(
            "Usage: python -m tomii [--list-knobs | --list-knobs-json | --knob-space [graph.json] | --schema | --dump <graph.json> | --visualize <graph.json> | --help]"
        )
        sys.exit(1)


def _cmd_dump(args: "list[str]") -> None:
    from pathlib import Path

    from ._visualize._dot import to_dot
    from ._visualize._parser import parse_json_file

    idx = args.index("--dump")
    if idx + 1 >= len(args) or args[idx + 1].startswith("--"):
        print("Usage: python -m tomii --dump <graph.json> [--out FILE]")
        sys.exit(1)
    graph_path = args[idx + 1]

    out = None
    if "--out" in args:
        i = args.index("--out")
        if i + 1 < len(args):
            out = args[i + 1]

    dot = to_dot(parse_json_file(graph_path), title=Path(graph_path).stem)
    if out:
        Path(out).write_text(dot, encoding="utf-8")
        print(f"DOT written to {out}")
    else:
        print(dot, end="")


def _cmd_knob_space(args: "list[str]") -> None:
    from ._knobs import knob_space

    idx = args.index("--knob-space")
    graph_path = None
    if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
        graph_path = args[idx + 1]

    workload = None
    if "--workload" in args:
        i = args.index("--workload")
        if i + 1 < len(args):
            workload = args[i + 1]

    print(json.dumps(knob_space(graph_path, workload=workload), indent=2))


def _cmd_visualize(args: "list[str]") -> None:
    from pathlib import Path
    from ._visualize import visualize

    idx = args.index("--visualize")

    graph_path = None
    if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
        graph_path = args[idx + 1]

    if graph_path is None:
        print(
            "Usage: python -m tomii --visualize <graph.json> [--edit] [--ascii] [--port N]"
        )
        sys.exit(1)

    mode = "web"
    if "--ascii" in args:
        mode = "ascii"

    editable = "--edit" in args

    port = None
    if "--port" in args:
        i = args.index("--port")
        if i + 1 < len(args):
            try:
                port = int(args[i + 1])
            except ValueError:
                print("Error: --port requires an integer")
                sys.exit(1)

    visualize(graph_path, mode=mode, port=port, editable=editable)


if __name__ == "__main__":
    main()
