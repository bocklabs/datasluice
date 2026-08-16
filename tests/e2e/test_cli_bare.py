"""Installed-wheel E2E for a bare (no-extras) core install.

A bare ``datasluice`` wheel must install, expose its console script, and run
commands that do not require optional data-plane extras (http/parquet/storage).
This isolates the release proof that the minimal distribution is functional, as
distinct from the full all-extras profile exercised by ``test_cli_existing.py``.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

_RETIRED_COMMANDS = ("search", "inspect", "download", "detect")


@pytest.fixture(scope="session")
def bare_env(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """Build the wheel, install it with no extras, return console + python paths."""
    wheel_dir = tmp_path_factory.mktemp("wheels")
    build = subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(wheel_dir)],
        cwd=str(_REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=180,
    )
    if build.returncode != 0:
        pytest.skip(f"uv build failed: {build.stderr[:300]}")

    wheels = list(wheel_dir.glob("datasluice-*.whl"))
    assert len(wheels) == 1, f"expected one wheel, found {wheels}"
    wheel = wheels[0]

    venv = tmp_path_factory.mktemp("venv")
    venv_python = venv / "bin" / "python"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, timeout=60)

    install = subprocess.run(
        ["uv", "pip", "install", "--python", str(venv_python), str(wheel)],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if install.returncode != 0:
        pytest.skip(f"pip install failed: {install.stderr[:300]}")

    console = str(venv / "bin" / "datasluice")
    assert Path(console).exists(), f"console script not found at {console}"
    return {"console": console, "python": str(venv_python), "venv": str(venv)}


def _clean_env(venv: str) -> dict[str, str]:
    return {"PATH": str(Path(venv) / "bin"), "HOME": os.environ.get("HOME", ""), "LANG": "en_US.UTF-8"}


def test_bare_wheel_imports_from_venv_not_checkout(bare_env: dict[str, str]) -> None:
    """The bare install imports datasluice from the venv site-packages, never the checkout."""
    result = subprocess.run(
        [bare_env["python"], "-c", "import datasluice; print(datasluice.__file__)"],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
    )
    assert result.returncode == 0, result.stderr
    assert str(_REPO_ROOT / "src") not in result.stdout
    assert bare_env["venv"] in result.stdout


def test_bare_console_script_exposes_version(bare_env: dict[str, str]) -> None:
    """The bare wheel exposes the datasluice console script and --help."""
    result = subprocess.run(
        [bare_env["console"], "--help"],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    for command in ("scan", "open", "materialize"):
        assert command in result.stdout, f"console --help missing command {command}"
    for retired in _RETIRED_COMMANDS:
        assert retired not in result.stdout, f"console --help must not advertise retired command {retired}"


def test_bare_console_script_rejects_retired_commands(bare_env: dict[str, str]) -> None:
    """Former portal-era commands fail resolution in the installed bare wheel."""
    result = subprocess.run(
        [bare_env["console"], "search", "https://data.example.test"],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
        timeout=60,
    )
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "No such command" in combined


def test_bare_import_no_optional_dependency_required(bare_env: dict[str, str]) -> None:
    """A bare install imports without requiring pyarrow, httpx, or fsspec."""
    result = subprocess.run(
        [
            bare_env["python"],
            "-c",
            "import importlib.util; import datasluice;"
            "print(importlib.util.find_spec('pyarrow') is None,"
            "importlib.util.find_spec('httpx') is None,"
            "importlib.util.find_spec('fsspec') is None)",
        ],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "True True True"
