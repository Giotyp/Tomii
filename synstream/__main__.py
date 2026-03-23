"""Entry point for `python -m synstream`.

Usage:
    python -m synstream --list-knobs         # human-readable list of graph.run() options
    python -m synstream --list-knobs-json    # machine-readable JSON of graph.run() options
    python -m synstream --schema             # JSON schema for graph construction parameters
    python -m synstream --help               # same as --list-knobs
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
    else:
        print(f"Unknown argument(s): {args}")
        print("Usage: python -m synstream [--list-knobs | --list-knobs-json | --schema | --help]")
        sys.exit(1)


if __name__ == "__main__":
    main()
