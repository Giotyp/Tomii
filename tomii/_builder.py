"""Build orchestration: compile tomii-core + plugin library."""

from __future__ import annotations
import os
import platform
import subprocess
import sys
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
    # Python bridge options
    python_plugin: bool = False
    python_interpreter: Optional[str] = None  # path to python executable; defaults to sys.executable


@dataclass
class BuildResult:
    dylib: str   # Absolute path to compiled .so
    binary: str  # Absolute path to tomii binary


def find_workspace_root() -> Path:
    """Walk up from this file's location to find the Cargo workspace root.

    Raises BuildError when not inside a workspace (e.g. tomii installed from PyPI).
    Use _try_workspace_root() if you need a non-raising variant.
    """
    root = _try_workspace_root()
    if root is None:
        raise BuildError(
            "Could not locate Cargo workspace root (Cargo.toml with [workspace]). "
            "Ensure the tomii package is inside the Τομί workspace, or use a "
            "pre-built tomii binary (install tomii from PyPI)."
        )
    return root


def _try_workspace_root() -> Optional[Path]:
    """Return the Cargo workspace root, or None if not inside one."""
    here = Path(__file__).resolve().parent
    for candidate in [here, *here.parents]:
        if (candidate / "Cargo.toml").exists():
            content = (candidate / "Cargo.toml").read_text()
            if "[workspace]" in content:
                return candidate
    return None


def _bridge_cache_dir() -> Path:
    """User-local cache directory for the compiled bridge dylib.

    Used when tomii is installed from PyPI (no Cargo workspace available).
    Stored under the active Python prefix so different venvs get separate caches.
    """
    prefix = Path(sys.prefix)
    return prefix / "tomii-bridge-cache"


def _bundled_binary() -> Optional[str]:
    """Return path to the pre-built tomii binary bundled with the wheel, or None."""
    candidate = Path(__file__).resolve().parent / "_bin" / "main"
    if candidate.exists():
        return str(candidate)
    return None


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


def _dylib_name(stem: str) -> str:
    """Return the platform-appropriate shared library filename."""
    if platform.system() == "Darwin":
        return f"lib{stem}.dylib"
    return f"lib{stem}.so"


def build(config: BuildConfig) -> BuildResult:
    """Run the two-stage Τομί build and return paths to outputs."""
    if config.python_plugin:
        return _build_python_plugin(config)

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

    binary = _find_binary_in_workspace(workspace, profile)

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


def _find_binary_in_workspace(workspace: Path, profile: str) -> str:
    """Locate the tomii main binary inside a Cargo workspace target dir."""
    binary = workspace / "target" / profile / "main"
    if binary.exists():
        return str(binary.resolve())
    raise BuildError(
        f"tomii binary not found at {binary}. "
        "Did tomii-core build successfully?"
    )


def _find_binary(release: bool = True) -> str:
    """Locate the tomii main binary from any supported location.

    Search order:
    1. Bundled binary packaged with the wheel (``tomii/_bin/main``).
    2. Workspace ``target/<profile>/main`` for development builds.

    Raises BuildError if neither is found.
    """
    bundled = _bundled_binary()
    if bundled:
        return bundled

    workspace = _try_workspace_root()
    if workspace:
        profile = "release" if release else "debug"
        binary = workspace / "target" / profile / "main"
        if binary.exists():
            return str(binary.resolve())

    raise RuntimeError(
        "tomii binary not found. Either:\n"
        "  • Build from source: app.build(python_plugin=True)\n"
        "  • Install a release wheel: pip install tomii"
    )


def _build_python_plugin(config: BuildConfig) -> BuildResult:
    """Build the generic PyO3 bridge plugin and locate the tomii-core binary.

    Two modes:
    - Development (inside a Cargo workspace): compiles both the bridge dylib and
      tomii-core into ``workspace/target/``.  This keeps the full build cache warm.
    - Installed (PyPI wheel, no workspace): compiles only the bridge dylib into
      ``<sys.prefix>/tomii-bridge-cache/`` and uses the pre-built binary bundled
      in ``tomii/_bin/main``.

    ``PYO3_PYTHON`` is set to the current interpreter
    (or ``config.python_interpreter`` if supplied).
    """
    bridge_dir = Path(__file__).resolve().parent / "_python_bridge"
    if not bridge_dir.exists():
        raise BuildError(
            f"Python bridge crate not found at {bridge_dir}. "
            "Reinstall the tomii package (pip install --force-reinstall tomii)."
        )

    release_flag = ["-r"] if config.release else []
    profile = "release" if config.release else "debug"
    manifest_path = str(bridge_dir / "Cargo.toml")

    # Choose Python interpreter ----------------------------------------------- #
    python_interp = config.python_interpreter or sys.executable
    _warn_about_gil(python_interp)

    build_env = {**os.environ, **config.env, "PYO3_PYTHON": python_interp}

    workspace = _try_workspace_root()

    if workspace is not None:
        # Development path — share the workspace target cache.
        target_dir = workspace / "target"
        cwd = workspace
    else:
        # PyPI path — compile bridge into a per-prefix cache dir.
        target_dir = _bridge_cache_dir()
        target_dir.mkdir(parents=True, exist_ok=True)
        cwd = bridge_dir

    # Build bridge plugin ------------------------------------------------------ #
    if config.clean:
        _cargo(
            ["clean", "--manifest-path", manifest_path, "--target-dir", str(target_dir)],
            build_env,
            cwd,
        )
    _cargo(
        ["build", "--manifest-path", manifest_path, "--target-dir", str(target_dir)]
        + release_flag,
        build_env,
        cwd,
    )

    dylib_name = _dylib_name("tomii_python_bridge")
    dylib_path = target_dir / profile / dylib_name
    if not dylib_path.exists():
        raise BuildError(f"Bridge dylib not found at {dylib_path} after build.")
    dylib = str(dylib_path.resolve())

    # Locate or build tomii-core binary --------------------------------------- #
    if workspace is not None:
        # Build tomii-core from source with FUNC_PATH pointing at the bridge so
        # the generated func_reg.rs includes the four bridge entry points.
        build_env["FUNC_PATH"] = str(bridge_dir / "src" / "lib.rs")
        build_env.pop("WRAP_PATH", None)
        build_env.pop("REG_PATH", None)
        if config.clean:
            _cargo(["clean", "-p", "tomii-core"], build_env, workspace)
        _cargo(["build", "-p", "tomii-core", "--bin", "main"] + release_flag, build_env, workspace)
        _cargo(["build", "-p", "tomii-types"] + release_flag, build_env, workspace)
        binary = _find_binary_in_workspace(workspace, profile)
    else:
        # PyPI path — use the pre-built binary bundled in the wheel.
        bundled = _bundled_binary()
        if bundled is None:
            raise BuildError(
                "No tomii binary found. When using an installed (PyPI) tomii package, "
                "the wheel must include a pre-built 'tomii/_bin/main' binary for this "
                "platform. Try reinstalling: pip install --upgrade tomii"
            )
        binary = bundled

    return BuildResult(dylib=dylib, binary=binary)


def _warn_about_gil(python_interp: str) -> None:
    """Print a one-time advisory if the interpreter has the GIL."""
    import subprocess as sp
    try:
        out = sp.check_output(
            [python_interp, "-c",
             "import sys; print(getattr(sys, '_is_gil_enabled', lambda: True)())"],
            stderr=sp.DEVNULL, text=True,
        ).strip()
        if out.lower() == "true":
            print(
                "[tomii.build] GIL is ENABLED in this interpreter. "
                "Parallelism limited to NumPy/BLAS ops that release the GIL internally. "
                "Install python3.13t and pass python_interpreter='python3.13t' "
                "for full multi-core Python parallelism.",
                flush=True,
            )
        else:
            print(
                "[tomii.build] Free-threaded Python detected (no GIL). "
                "Full multi-core parallelism available.",
                flush=True,
            )
    except Exception:
        pass  # non-fatal advisory
