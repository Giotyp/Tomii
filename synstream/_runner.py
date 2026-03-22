"""Run the SynStream binary with a graph JSON."""

from __future__ import annotations
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from ._serialize import to_json


# --------------------------------------------------------------------------- #
# CLI flag mapping
# --------------------------------------------------------------------------- #

_INT_FLAGS: Dict[str, str] = {
    "workers":           "--workers",
    "core_offset":       "--core-offset",
    "system_threads":    "--system-threads",
    "receiver_threads":  "--receiver-threads",
    "max_runtime":       "--max-runtime",
    "slots":             "--slots",
    "max_streams":       "--max-streams",
    "batching_size":     "--batching-size",
    "batching_limit":    "--batching-limit",
    "exclude_streams":   "--exclude-streams",
    "record_stream":     "--record-stream",
}

_BOOL_FLAGS: Dict[str, str] = {
    "fifo":                 "--fifo",
    "custom":               "--custom",
    "inits":                "--inits",
    "debug":                "--debug",
    "record":               "--record",
    "use_rdtsc":            "--use-rdtsc",
    "slot_priority":        "--slot-priority",
    "coalesce_barriers":    "--coalesce-barriers",
    "inline_continuation":  "--inline-continuation",
}

_STR_FLAGS: Dict[str, str] = {
    "output": "--output",
    "timing": "--timing",
    "report": "--report",
}

_KNOB_DESCRIPTIONS: Dict[str, str] = {
    # Int flags
    "workers":           "Rayon worker threads (match physical cores)",
    "core_offset":       "First CPU to pin workers to (use 1 to leave CPU 0 for OS)",
    "system_threads":    "Resolution/scheduler threads (default 1; rarely needs changing)",
    "receiver_threads":  "Dedicated network receiver threads (for network-input graphs)",
    "max_runtime":       "Stop after N seconds (0 = run until max_streams complete)",
    "slots":             "Concurrent in-flight streams (1 for latency, >1 for throughput)",
    "max_streams":       "Total streams to process (0 = run indefinitely)",
    "batching_size":     "Max tasks per scheduler batch",
    "batching_limit":    "Max outstanding batches before back-pressure",
    "exclude_streams":   "Skip first N streams from timing output",
    "record_stream":     "Record timing for this specific stream index only",
    # Bool flags
    "fifo":                 "Use FIFO task scheduling instead of default (depth-first)",
    "custom":               "Enable custom scheduling strategy",
    "inits":                "Re-run graph initializations on each stream",
    "debug":                "Print verbose debug output",
    "record":               "Enable timing/event recording to file",
    "use_rdtsc":            "Use RDTSC for sub-\u03bcs timing (x86 only; improves timer precision)",
    "slot_priority":        "Prioritize tasks from the earliest active slot",
    "coalesce_barriers":    "Batch barrier fan-outs into bulk tasks (reduces overhead for fine-grained graphs)",
    "inline_continuation":  "Run single-successor tasks inline (reduces scheduling overhead)",
    # Str flags
    "output":  "Path for raw timing output file",
    "timing":  "Path for per-node timing CSV",
    "report":  "Path for JSON summary report (avg/p99 latency, bottleneck hints)",
}


def list_knobs() -> str:
    """Return a human+machine-readable list of all graph.run() options."""
    lines = ["graph.run() options", "=" * 40]

    def _section(title: str, flags: Dict[str, str], typ: str) -> None:
        lines.append(f"\n[{title}]")
        for key, flag in flags.items():
            desc = _KNOB_DESCRIPTIONS.get(key, "")
            lines.append(f"  {key} ({typ}, CLI: {flag})")
            if desc:
                lines.append(f"      {desc}")

    _section("Integer flags", _INT_FLAGS, "int")
    _section("Boolean flags", _BOOL_FLAGS, "bool")
    _section("String flags", _STR_FLAGS, "str")
    return "\n".join(lines)


def build_command(
    binary: str,
    json_path: str,
    dylib: str,
    **kwargs: Any,
) -> List[str]:
    """Build the subprocess command list for the SynStream binary."""
    cmd: List[str] = [binary, "--json", json_path, "--dylib", dylib]

    for key, flag in _INT_FLAGS.items():
        val = kwargs.get(key)
        if val is not None:
            cmd += [flag, str(val)]

    for key, flag in _BOOL_FLAGS.items():
        if kwargs.get(key):
            cmd.append(flag)

    for key, flag in _STR_FLAGS.items():
        val = kwargs.get(key)
        if val is not None:
            cmd += [flag, str(val)]

    return cmd


def _find_binary(release: bool = True) -> str:
    """Auto-detect the synstream binary from the workspace."""
    from ._builder import find_workspace_root, BuildError
    try:
        workspace = find_workspace_root()
    except BuildError:
        raise RuntimeError(
            "Cannot auto-detect synstream binary: not inside a Cargo workspace. "
            "Pass binary= explicitly."
        )
    profile = "release" if release else "debug"
    binary = workspace / "target" / profile / "main"
    if binary.exists():
        return str(binary.resolve())
    raise RuntimeError(
        f"synstream binary not found at {binary}. "
        "Build the project first (app.build() or cargo build)."
    )


def run(
    graph: Any,
    *,
    dylib: str,
    binary: Optional[str] = None,
    release: bool = True,
    env: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> subprocess.CompletedProcess:
    """Write graph JSON to a temp file and invoke the SynStream binary.

    Args:
        graph:   Graph object to serialize.
        dylib:   Path to the plugin .so file.
        binary:  Explicit path to the synstream binary (auto-detected if None).
        release: Use release binary when auto-detecting (default True).
        env:     Extra environment variables (e.g. {"SCRIPT_DIR": "/path"}).
        **kwargs: All CLI arguments (workers, slots, timing, etc.).

    Returns:
        subprocess.CompletedProcess
    """
    if binary is None:
        binary = _find_binary(release=release)

    run_env = {**os.environ, **(env or {})}
    json_str = to_json(graph)

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        tmp.write(json_str)
        tmp_path = tmp.name

    try:
        cmd = build_command(binary, tmp_path, dylib, **kwargs)
        print(f"[synstream.run] {' '.join(cmd)}", flush=True)
        result = subprocess.run(cmd, env=run_env)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return result
