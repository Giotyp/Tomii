"""Build orchestration: compile tomii-core + plugin library."""

from __future__ import annotations
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional


class BuildError(Exception):
    """Raised when a cargo build step fails."""


@dataclass
class BuildConfig:
    func_path: Optional[str] = None
    wrap_path: Optional[str] = None
    reg_path: Optional[str] = None
    plugin_manifest: Optional[str] = None
    release: bool = True
    clean: bool = False
    env: Dict[str, str] = field(default_factory=dict)


@dataclass
class BuildResult:
    dylib: str   # Absolute path to compiled .so
    binary: str  # Absolute path to tomii binary


def find_workspace_root() -> Path:
    """Walk up from this file's location to find the Cargo workspace root."""
    here = Path(__file__).resolve().parent
    for candidate in [here, *here.parents]:
        if (candidate / "Cargo.toml").exists():
            content = (candidate / "Cargo.toml").read_text()
            if "[workspace]" in content:
                return candidate
    raise BuildError(
        "Could not locate Cargo workspace root (Cargo.toml with [workspace]). "
        "Ensure the tomii package is inside the Τομί workspace."
    )


def _resolve(path: Optional[str]) -> Optional[str]:
    return str(Path(path).resolve()) if path else None


def _cargo(args: list, env: dict, cwd: Path) -> None:
    """Run a cargo command, streaming output to the terminal."""
    cmd = ["cargo"] + args
    print(f"[tomii.build] {' '.join(cmd)}", flush=True)
    result = subprocess.run(cmd, env=env, cwd=str(cwd))
    if result.returncode != 0:
        raise BuildError(
            f"cargo command failed (exit {result.returncode}):\n  {' '.join(cmd)}"
        )


def build(config: BuildConfig) -> BuildResult:
    """Run the two-stage Τομί build and return paths to outputs."""
    workspace = find_workspace_root()
    release_flag = ["-r"] if config.release else []
    profile = "release" if config.release else "debug"

    has_wrap = config.wrap_path and config.reg_path
    has_func = bool(config.func_path)
    if not has_wrap and not has_func:
        raise BuildError(
            "Must provide either (wrap_path + reg_path) or func_path to build."
        )

    build_env = {**os.environ, **config.env}

    if has_wrap:
        build_env["WRAP_PATH"] = _resolve(config.wrap_path)
        build_env["REG_PATH"] = _resolve(config.reg_path)
        build_env.pop("FUNC_PATH", None)
    else:
        build_env["FUNC_PATH"] = _resolve(config.func_path)
        build_env.pop("WRAP_PATH", None)
        build_env.pop("REG_PATH", None)

    # Build plugin first (its clean may wipe workspace artifacts)
    dylib: Optional[str] = None
    if config.plugin_manifest:
        manifest_path = str(Path(config.plugin_manifest).resolve())
        if config.clean:
            _cargo(
                ["clean", "--manifest-path", manifest_path],
                build_env,
                workspace,
            )
        _cargo(
            ["build", "--manifest-path", manifest_path] + release_flag,
            build_env,
            workspace,
        )
        dylib = _find_dylib(workspace / "target" / profile)

    # Build the tomii main binary last so the plugin clean can't wipe it.
    # (cargo clean --manifest-path on a workspace member cleans the whole workspace.)
    if config.clean:
        _cargo(["clean", "-p", "tomii-core"], build_env, workspace)

    _cargo(["build", "-p", "tomii-core", "--bin", "main"] + release_flag, build_env, workspace)
    _cargo(["build", "-p", "tomii-types"] + release_flag, build_env, workspace)

    binary = _find_binary(workspace, profile)

    return BuildResult(
        dylib=dylib or "",
        binary=binary,
    )


def _find_dylib(target_dir: Path) -> str:
    """Find the most recently modified .so in target_dir."""
    candidates = sorted(
        target_dir.glob("*.so"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not candidates:
        raise BuildError(f"No .so file found in {target_dir}")
    return str(candidates[0].resolve())


def _find_binary(workspace: Path, profile: str) -> str:
    """Locate the tomii main binary."""
    binary = workspace / "target" / profile / "main"
    if binary.exists():
        return str(binary.resolve())
    raise BuildError(
        f"tomii binary not found at {binary}. "
        "Did tomii-core build successfully?"
    )
