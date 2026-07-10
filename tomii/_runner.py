"""Run the Τομί binary with a graph JSON."""

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
    "workers": "--workers",
    "core_offset": "--core-offset",
    "system_threads": "--system-threads",
    "receiver_threads": "--receiver-threads",
    "max_runtime": "--max-runtime",
    "slots": "--slots",
    "max_frames": "--max-frames",
    "batching_size": "--batching-size",
    "batching_limit": "--batching-limit",
    "exclude_frames": "--exclude-frames",
    "record_frame": "--record-frame",
}

_BOOL_FLAGS: Dict[str, str] = {
    "fifo": "--fifo",
    "custom": "--custom",
    "inits": "--inits",
    "debug": "--debug",
    "record": "--record",
    "use_rdtsc": "--use-rdtsc",
    "slot_priority": "--slot-priority",
    "coalesce_barriers": "--coalesce-barriers",
    "inline_continuation": "--inline-continuation",
    "no_fanout_bulk": "--no-fanout-bulk",
}

_STR_FLAGS: Dict[str, str] = {
    "output": "--output",
    "timing": "--timing",
    "report": "--report",
    "dump_state": "--dump-state",
}

_KNOB_DESCRIPTIONS: Dict[str, str] = {
    # Int flags
    "workers": "Rayon worker threads (match physical cores)",
    "core_offset": "First CPU to pin workers to (use 1 to leave CPU 0 for OS)",
    "system_threads": "Resolution/scheduler threads (default 1; rarely needs changing)",
    "receiver_threads": "Dedicated network receiver threads (for network-input graphs)",
    "max_runtime": "Stop after N seconds (0 = run until max_frames complete)",
    "slots": "Concurrent in-flight frames (1 for latency, >1 for throughput)",
    "max_frames": "Total frames to process (0 = run indefinitely)",
    "batching_size": "Max tasks per scheduler batch",
    "batching_limit": "Max outstanding batches before back-pressure",
    "exclude_frames": "Skip first N frames from timing output",
    "record_frame": "Record timing for this specific frame index only",
    # Bool flags
    "fifo": "Use FIFO task scheduling instead of default (depth-first)",
    "custom": "Enable custom scheduling strategy",
    "inits": "Re-run graph initializations on each frame",
    "debug": "Print verbose debug output",
    "record": "Enable timing/event recording to file",
    "use_rdtsc": "Use RDTSC for sub-\u03bcs timing (x86 only; improves timer precision)",
    "slot_priority": "Prioritize tasks from the earliest active slot",
    "coalesce_barriers": "Batch barrier fan-outs into bulk tasks (reduces overhead for fine-grained graphs)",
    "inline_continuation": "Run single-successor tasks inline (reduces scheduling overhead)",
    # Str flags
    "output": "Path for raw timing output file",
    "dump_state": "Path for a runtime state snapshot (JSON) written at shutdown; "
    "SIGUSR1 writes numbered live snapshots (wedged-run debugging)",
    "timing": "Path for per-node timing CSV",
    "report": "Path for JSON summary report (avg/p99 latency, bottleneck hints)",
}

# Role of each knob in a tuning context:
#   perf        — affects performance and is safe to search (verifier-gated)
#   measurement — benchmark parameter; fix it for fair comparison, never search it
#   io          — output/diagnostic plumbing; never search
#   env         — machine-specific (CPU pinning); set once per host, don't search
_KNOB_ROLES: Dict[str, str] = {
    "workers": "perf",
    "core_offset": "env",
    "system_threads": "perf",
    "receiver_threads": "perf",
    "max_runtime": "measurement",
    "slots": "perf",
    "max_frames": "measurement",
    "batching_size": "perf",
    "batching_limit": "perf",
    "exclude_frames": "measurement",
    "record_frame": "measurement",
    "fifo": "perf",
    "custom": "perf",
    "inits": "measurement",
    "debug": "io",
    "record": "io",
    "use_rdtsc": "measurement",
    "slot_priority": "perf",
    "coalesce_barriers": "perf",
    "inline_continuation": "perf",
    "no_fanout_bulk": "perf",
    "output": "io",
    "timing": "io",
    "report": "io",
    "dump_state": "io",
}

# Value domains for searchable knobs.  "pow2" means the sensible search points
# are powers of two within [min, max] (sample the exponent, not the value).
# Knobs without a domain entry get {"kind": "bool"} if boolean, else none
# (not searchable).  `workers`' max is resolved to the host's CPU count at
# catalog-generation time.
_KNOB_DOMAINS: Dict[str, Dict[str, Any]] = {
    "workers": {"kind": "int", "min": 1, "max": None, "scale": "pow2"},  # max = cpus
    "slots": {"kind": "int", "min": 1, "max": 64, "scale": "pow2"},
    "batching_size": {"kind": "int", "min": 1, "max": 512, "scale": "pow2"},
    "batching_limit": {"kind": "int", "min": 1, "max": 8, "scale": "pow2"},
    "system_threads": {"kind": "int", "min": 1, "max": 4, "scale": "linear"},
    "receiver_threads": {"kind": "int", "min": 0, "max": 4, "scale": "linear"},
}

_KNOB_SEARCH_HINTS: Dict[str, str] = {
    "workers": "unimodal; binary search 1–physical_cores; diminishing returns past core count",
    "slots": "1 minimizes latency; >1 increases throughput; try 1,2,4,8",
    "batching_size": "unimodal; binary search 1–512; larger reduces scheduling overhead for fine-grained graphs",
    "batching_limit": "unimodal; try 1,2,4; higher allows more outstanding work",
    "core_offset": "set to 1 to leave CPU 0 for OS; rarely needs tuning otherwise",
    "system_threads": "leave at default 1 unless profiling shows scheduler bottleneck",
    "max_frames": "benchmark parameter; set to fixed value for fair comparison",
    "exclude_frames": "skip warmup frames; set to 1–3 to exclude JIT effects from timing",
    "inline_continuation": "try both; often reduces latency for linear chains or low-fan-out graphs",
    "coalesce_barriers": "helpful when node factor >> worker count (reduces barrier overhead)",
    "slot_priority": "try both when slots > 1; can reduce tail latency under imbalanced graphs",
    "fifo": "try both; depth-first (default) usually better for latency",
    "use_rdtsc": "enable for sub-us timing precision on x86; no effect on performance",
}


def list_knobs() -> str:
    """Return a human-readable list of all graph.run() options."""
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


def _resolved_domain(key: str) -> Optional[Dict[str, Any]]:
    """Return the value domain for `key` with host-dependent bounds resolved."""
    domain = _KNOB_DOMAINS.get(key)
    if domain is None:
        return None
    domain = dict(domain)
    if key == "workers" and domain.get("max") is None:
        domain["max"] = os.cpu_count() or 8
    return domain


def list_knobs_json() -> "dict[str, Any]":
    """Return a machine-readable dict of all graph.run() options.

    Each knob carries: name, type, cli flag, role (perf/measurement/io/env),
    description, search_hint, and — for searchable knobs — a value domain.
    This catalog is the single source the knob-space generator
    (`tomii.knob_space`) builds search spaces from.
    """
    knobs = []
    for key, flag in _INT_FLAGS.items():
        entry: Dict[str, Any] = {
            "name": key,
            "type": "int",
            "cli": flag,
            "role": _KNOB_ROLES.get(key, "perf"),
            "description": _KNOB_DESCRIPTIONS.get(key, ""),
            "search_hint": _KNOB_SEARCH_HINTS.get(key, ""),
        }
        domain = _resolved_domain(key)
        if domain is not None:
            entry["domain"] = domain
        knobs.append(entry)
    for key, flag in _BOOL_FLAGS.items():
        knobs.append(
            {
                "name": key,
                "type": "bool",
                "cli": flag,
                "role": _KNOB_ROLES.get(key, "perf"),
                "description": _KNOB_DESCRIPTIONS.get(key, ""),
                "search_hint": _KNOB_SEARCH_HINTS.get(key, "try both True and False"),
                "domain": {"kind": "bool"},
            }
        )
    for key, flag in _STR_FLAGS.items():
        knobs.append(
            {
                "name": key,
                "type": "str",
                "cli": flag,
                "role": _KNOB_ROLES.get(key, "io"),
                "description": _KNOB_DESCRIPTIONS.get(key, ""),
            }
        )
    return {"version": 3, "knobs": knobs}


def build_command(
    binary: str,
    json_path: str,
    dylib: str,
    **kwargs: Any,
) -> List[str]:
    """Build the subprocess command list for the Τομί binary."""
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
    """Auto-detect the tomii binary (bundled wheel binary or workspace build)."""
    from ._builder import _find_binary as _builder_find_binary

    return _builder_find_binary(release=release)


def run(
    graph: Any,
    *,
    dylib: str,
    binary: Optional[str] = None,
    release: bool = True,
    env: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> "subprocess.CompletedProcess[bytes]":
    """Write graph JSON to a temp file and invoke the Τομί binary.

    Args:
        graph:   Graph object to serialize.
        dylib:   Path to the plugin .so file.
        binary:  Explicit path to the tomii binary (auto-detected if None).
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
        print(f"[tomii.run] {' '.join(cmd)}", flush=True)
        result = subprocess.run(cmd, env=run_env)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return result
