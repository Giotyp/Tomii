"""Entry point for `python -m tomii`.

Usage:
    python -m tomii --list-knobs         # human-readable list of graph.run() options
    python -m tomii --list-knobs-json    # machine-readable JSON of graph.run() options
    python -m tomii --schema             # JSON schema for graph construction parameters
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
    elif "--schema" in args:
        from ._schema import graph_schema
        print(json.dumps(graph_schema(), indent=2))
    elif "--visualize" in args:
        _cmd_visualize(args)
    else:
        print(f"Unknown argument(s): {args}")
        print("Usage: python -m tomii [--list-knobs | --list-knobs-json | --schema | --visualize <graph.json> | --help]")
        sys.exit(1)


def _cmd_visualize(args: list) -> None:
    from pathlib import Path
    from ._visualize import visualize

    idx = args.index("--visualize")

    graph_path = None
    if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
        graph_path = args[idx + 1]

    if graph_path is None:
        print("Usage: python -m tomii --visualize <graph.json> [--edit] [--ascii] [--port N]")
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
