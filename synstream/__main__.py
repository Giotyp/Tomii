"""Entry point for `python -m synstream`.

Usage:
    python -m synstream --list-knobs   # list all graph.run() options
    python -m synstream --help         # same
"""
import sys

from ._runner import list_knobs


def main() -> None:
    if "--list-knobs" in sys.argv or "--help" in sys.argv or "-h" in sys.argv or len(sys.argv) == 1:
        print(list_knobs())
    else:
        print(f"Unknown argument(s): {sys.argv[1:]}")
        print("Usage: python -m synstream [--list-knobs | --help]")
        sys.exit(1)


if __name__ == "__main__":
    main()
