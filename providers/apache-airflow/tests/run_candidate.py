"""Build the core candidate and provider wheels and run a test command in a clean venv.

This is the sole local and provider CI entry point. It builds an explicit
local core candidate wheel (with the real release version rewritten to the
requested candidate), builds the provider wheel, validates both METADATA
blocks against the published contract, creates a throwaway venv, installs
the exact local core/provider wheel paths plus the requested Apache Airflow
version, rejects any ``datasluice`` that did not come from the local core
wheel, and then executes the supplied test command.

Plan 08-07 performs the first source-bearing provider build and installed
run after it creates the ``airflow.providers.datasluice`` source package.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from email.message import Message
from email.parser import Parser
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

_CORE_NAME = "datasluice"
_PROVIDER_NAME = "apache-airflow-providers-datasluice"
_PROVIDER_VERSION = "0.1.0"
_PROVIDER_REQUIRES = frozenset({"datasluice>=1.0,<2", "apache-airflow>=3.2,<4"})


def main(argv: list[str] | None = None) -> int:
    """Build candidate wheels, validate them, and run the test command in a clean venv.

    Args:
        argv: Optional explicit argument vector for testing.

    Returns:
        ``0`` on success, or the test command's exit code on failure.
    """
    args, command = _parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="datasluice-candidate-") as tmp:
        tmp_dir = Path(tmp)
        core_wheel = _build_core_candidate(tmp_dir / "core", tmp_dir / "dist-core", args.core_version)
        _validate_core_wheel(core_wheel, args.core_version)
        provider_wheel = _build_provider_wheel()
        _validate_provider_wheel(provider_wheel)
        return _run_in_clean_venv(core_wheel, provider_wheel, args.airflow_version, command)


def _parse_args(argv: list[str] | None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description=(
            "Build the core candidate and provider wheels and run a test command in a clean "
            "venv that installs only the exact local wheels plus the requested Apache Airflow."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  %(prog)s --core-version 1.0.0 --airflow-version 3.2.2 "
            "-- pytest providers/apache-airflow/tests -x\n"
            "  %(prog)s --airflow-version 3.2.2 "
            '-- python -c "import airflow.providers.datasluice"\n\n'
            "datasluice is never resolved from an index or checkout."
        ),
    )
    parser.add_argument(
        "--core-version",
        default="1.0.0",
        help="core datasluice candidate version to build and install (default: 1.0.0)",
    )
    parser.add_argument(
        "--airflow-version",
        required=True,
        help="exact apache-airflow version to install in the clean venv",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="test command to execute inside the clean venv (pass after '--')",
    )
    parsed = parser.parse_args(argv)
    command = list(parsed.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("a test command is required (pass it after '--')")
    return parsed, command


def _build_core_candidate(dest_root: Path, out_dir: Path, candidate_version: str) -> Path:
    _copy_core_build_inputs(dest_root)
    pyproject = dest_root / "pyproject.toml"
    patched = _rewrite_core_version(pyproject.read_text(encoding="utf-8"), candidate_version)
    pyproject.write_text(patched, encoding="utf-8")
    out_dir.mkdir(parents=True, exist_ok=True)
    _run(["uv", "build", "--no-sources", "--out-dir", str(out_dir)], cwd=dest_root)
    return _single_wheel(out_dir, _CORE_NAME)


def _copy_core_build_inputs(dest_root: Path) -> None:
    dest_root.mkdir(parents=True, exist_ok=True)
    for name in ("pyproject.toml", "README.md", "LICENSE"):
        src = REPO_ROOT / name
        if src.exists():
            shutil.copy2(src, dest_root / name)
    shutil.copytree(REPO_ROOT / "src", dest_root / "src")


def _rewrite_core_version(text: str, candidate_version: str) -> str:
    lines = text.splitlines(keepends=True)
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith("version") and "=" in stripped:
            indent = line[: len(line) - len(stripped)]
            lines[i] = f'{indent}version = "{candidate_version}"\n'
            return "".join(lines)
    raise RuntimeError("could not locate project.version in core pyproject.toml")


def _build_provider_wheel() -> Path:
    provider_dir = REPO_ROOT / "providers" / "apache-airflow"
    out_dir = provider_dir / "dist"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    _run(["uv", "build", "--no-sources", "--out-dir", str(out_dir)], cwd=provider_dir)
    return _single_wheel(out_dir, _PROVIDER_NAME)


def _single_wheel(out_dir: Path, project_name: str) -> Path:
    wheels = sorted(out_dir.glob(f"{project_name}-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one {project_name} wheel in {out_dir}, found: {wheels}")
    return wheels[0]


def _read_metadata(wheel: Path) -> Message:
    with zipfile.ZipFile(wheel) as archive:
        name = next(n for n in archive.namelist() if n.endswith(".dist-info/METADATA"))
        return Parser().parsestr(archive.read(name).decode("utf-8"))


def _validate_core_wheel(wheel: Path, candidate_version: str) -> None:
    meta = _read_metadata(wheel)
    if meta["Name"] != _CORE_NAME:
        raise RuntimeError(f"core wheel Name={meta['Name']!r}, expected {_CORE_NAME!r}")
    if meta["Version"] != candidate_version:
        raise RuntimeError(f"core wheel Version={meta['Version']!r}, expected {candidate_version!r}")
    requires = meta.get_all("Requires-Dist") or []
    airflow_lines = [r for r in requires if "apache-airflow" in r]
    if airflow_lines:
        raise RuntimeError(f"core wheel declares an Airflow dependency/extra: {airflow_lines}")
    extras = meta.get_all("Provides-Extra") or []
    if "airflow" in extras:
        raise RuntimeError("core wheel still Provides-Extra: airflow")


def _validate_provider_wheel(wheel: Path) -> None:
    meta = _read_metadata(wheel)
    if meta["Name"] != _PROVIDER_NAME:
        raise RuntimeError(f"provider wheel Name={meta['Name']!r}, expected {_PROVIDER_NAME!r}")
    if meta["Version"] != _PROVIDER_VERSION:
        raise RuntimeError(f"provider wheel Version={meta['Version']!r}, expected {_PROVIDER_VERSION!r}")
    requires = frozenset(meta.get_all("Requires-Dist") or [])
    if requires != _PROVIDER_REQUIRES:
        missing = _PROVIDER_REQUIRES - requires
        extra = requires - _PROVIDER_REQUIRES
        raise RuntimeError(f"provider Requires-Dist mismatch: missing={sorted(missing)}, extra={sorted(extra)}")


def _run_in_clean_venv(
    core_wheel: Path,
    provider_wheel: Path,
    airflow_version: str,
    command: list[str],
) -> int:
    with tempfile.TemporaryDirectory(prefix="datasluice-venv-") as venv_str:
        venv = Path(venv_str)
        _run(["uv", "venv", str(venv)])
        env = _venv_env(venv)
        _run(
            [
                "uv",
                "pip",
                "install",
                "--python",
                str(venv / "bin" / "python"),
                str(core_wheel),
                str(provider_wheel),
                f"apache-airflow=={airflow_version}",
            ],
            env=env,
        )
        _assert_datasluice_is_local(core_wheel, venv)
        result = subprocess.run(command, env=env, cwd=str(REPO_ROOT))
        return result.returncode


def _venv_env(venv: Path) -> dict[str, str]:
    env = dict(os.environ)
    env["VIRTUAL_ENV"] = str(venv)
    env["UV_PROJECT_ENVIRONMENT"] = str(venv)
    bin_dir = str(venv / "bin")
    env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    return env


def _find_site_packages(venv: Path) -> Path:
    probe = subprocess.run(
        [str(venv / "bin" / "python"), "-c", "import sysconfig; print(sysconfig.get_path('purelib'))"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(probe.stdout.strip())


def _assert_datasluice_is_local(core_wheel: Path, venv: Path) -> None:
    site = _find_site_packages(venv)
    dist_infos = sorted(site.glob(f"{_CORE_NAME}-*.dist-info"))
    if not dist_infos:
        raise RuntimeError("datasluice is not installed in the candidate venv")
    direct_url = dist_infos[-1] / "direct_url.json"
    if not direct_url.exists():
        raise RuntimeError("datasluice has no direct_url.json; it was resolved from an index or checkout")
    payload = json.loads(direct_url.read_text(encoding="utf-8"))
    url = payload.get("url", "")
    if not url.startswith("file://"):
        raise RuntimeError(f"datasluice was installed from a non-local URL: {url}")
    expected = core_wheel.resolve().as_uri()
    if url != expected:
        raise RuntimeError(f"datasluice installed from {url}; expected the local core candidate wheel {expected}")


def _run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=env, check=True)


if __name__ == "__main__":
    sys.exit(main())
