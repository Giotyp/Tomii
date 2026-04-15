"""Entry point for `python -m synstream`.

Usage:
    python -m synstream --list-knobs         # human-readable list of graph.run() options
    python -m synstream --list-knobs-json    # machine-readable JSON of graph.run() options
    python -m synstream --schema             # JSON schema for graph construction parameters
    python -m synstream --help               # same as --list-knobs

    python -m synstream --visualize graph.json          # interactive web visualization
    python -m synstream --visualize graph.json --ascii  # terminal ASCII art
    python -m synstream --visualize graph.json --port 8080  # custom port
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
        print("Usage: python -m synstream [--list-knobs | --list-knobs-json | --schema | --visualize <graph.json> | --help]")
        sys.exit(1)


def _cmd_visualize(args: list) -> None:
    """Handle the --visualize subcommand."""
    from pathlib import Path
    from ._visualize import visualize

    idx = args.index("--visualize")

    # Positional: the JSON file comes right after --visualize
    graph_path = None
    if idx + 1 < len(args) and not args[idx + 1].startswith("--"):
        graph_path = args[idx + 1]

    if graph_path is None:
        print("Usage: python -m synstream --visualize <graph.json> [--ascii] [--port N]")
        sys.exit(1)

    if not Path(graph_path).exists():
        print(f"Error: file not found: {graph_path}")
        sys.exit(1)

    # Parse flags
    mode = "web"
    if "--ascii" in args:
        mode = "ascii"

    port = None
    if "--port" in args:
        i = args.index("--port")
        if i + 1 < len(args):
            try:
                port = int(args[i + 1])
            except ValueError:
                print("Error: --port requires an integer")
                sys.exit(1)

    visualize(graph_path, mode=mode, port=port)


if __name__ == "__main__":
    main()
