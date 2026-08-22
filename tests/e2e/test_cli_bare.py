"""Installed-wheel E2E for a bare (no-extras) core install.

A bare ``datasluice`` wheel must install, expose its console script, and run
commands that do not require optional data-plane extras (http/parquet/storage).
This isolates the release proof that the minimal distribution is functional, as
distinct from the full all-extras profile exercised by ``test_cli_existing.py``.
"""

from __future__ import annotations

import os
import re
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
        pytest.fail(f"uv build failed: {build.stderr[:300]}", pytrace=False)

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
        pytest.fail(f"pip install failed: {install.stderr[:300]}", pytrace=False)

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


def test_bare_wheel_import_sweep_stays_optional_dependency_free(bare_env: dict[str, str]) -> None:
    """Every base-reachable public package imports without optional distributions."""
    result = subprocess.run(
        [
            bare_env["python"],
            "-c",
            "import sys;"
            "import datasluice;"
            "import datasluice.io;"
            "import datasluice.sync;"
            "import datasluice.discovery;"
            "import datasluice.integrations.dlt;"
            "import datasluice.runtime;"
            "import datasluice.runtime.bulk;"
            "import datasluice.runtime.mutation;"
            "import datasluice.runtime.oauth;"
            "import datasluice.connectors.catalog.ckan;"
            "import datasluice.connectors.catalog.udata;"
            "import datasluice.connectors.catalog.socrata;"
            "optional = ('boto3', 'dlt', 'duckdb', 'fsspec', 'httpx', 'hvac', 'keyring', 'openpyxl', "
            "'opentelemetry', 'pandas', 'polars', 'pyarrow', 'zstandard');"
            "loaded = [name for name in optional if any(module == name or module.startswith(name + '.') "
            "for module in sys.modules)];"
            "assert not loaded, loaded",
        ],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_bare_console_script_exposes_runtime_cli_surface(bare_env: dict[str, str]) -> None:
    """The bare wheel exposes version and the safe runtime CLI surface."""
    result = subprocess.run(
        [bare_env["console"], "--help"],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    version = subprocess.run(
        [bare_env["console"], "--version"],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
        timeout=60,
    )
    assert version.returncode == 0, version.stderr
    assert version.stdout.startswith("datasluice ")
    commands_section = result.stdout.split("Commands", 1)[1]
    for command in ("scan", "open", "materialize", "capabilities", "credentials"):
        assert re.search(rf"^[\W_]*{command}\b", commands_section, re.MULTILINE), (
            f"console --help missing command {command}"
        )
    for retired in _RETIRED_COMMANDS:
        assert not re.search(rf"^[\W_]*{retired}\b", commands_section, re.MULTILINE), (
            f"console --help must not advertise retired command {retired}"
        )


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


def test_bare_wheel_ships_reference_fixture_sets(bare_env: dict[str, str]) -> None:
    """The installed wheel loads every reference fixture set without the checkout."""
    result = subprocess.run(
        [
            bare_env["python"],
            "-c",
            "from datasluice.contracts.catalog import load_reference_fixture_set;"
            "print([load_reference_fixture_set(p).platform for p in ('ckan', 'udata', 'socrata')])",
        ],
        capture_output=True,
        text=True,
        env=_clean_env(bare_env["venv"]),
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "['ckan', 'udata', 'socrata']"


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
