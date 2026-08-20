"""Installed-wheel end-to-end contracts for the retained direct-resource CLI.

Builds the datasluice wheel once per session, installs it into an isolated
venv, and runs the ``datasluice`` console script against local deterministic
fixtures — never the repository checkout and never live portals. Only the
retained direct-resource commands (scan, open, materialize) are exercised;
former portal-era commands must be absent, not redirected.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

_RETIRED_COMMANDS = ("search", "inspect", "download", "detect")


@pytest.fixture(scope="session")
def installed_env(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """Build the wheel, install it into an isolated venv, return console + python paths."""
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

    extras = os.environ.get("DATASLUICE_E2E_EXTRAS", "http,parquet,storage")
    wheel_spec = str(wheel) if extras == "none" else f"{wheel}[{extras}]"

    install = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(venv_python),
            wheel_spec,
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if install.returncode != 0:
        pytest.fail(f"pip install failed: {install.stderr[:300]}", pytrace=False)

    console = str(venv / "bin" / "datasluice")
    assert Path(console).exists(), f"console script not found at {console}"

    return {"console": console, "python": str(venv_python), "venv": str(venv), "extras": extras}


@pytest.fixture(scope="session")
def all_connectors_env(tmp_path_factory: pytest.TempPathFactory) -> dict[str, str]:
    """Build the wheel and install its all-connectors extra in an isolated venv."""
    wheel_dir = tmp_path_factory.mktemp("connector-wheels")
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
    venv = tmp_path_factory.mktemp("all-connectors-venv")
    venv_python = venv / "bin" / "python"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, timeout=60)
    install = subprocess.run(
        ["uv", "pip", "install", "--python", str(venv_python), f"{wheels[0]}[all-connectors]"],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if install.returncode != 0:
        pytest.fail(f"all-connectors install failed: {install.stderr[:300]}", pytrace=False)

    return {"python": str(venv_python), "venv": str(venv)}


def _run_cli(env_info: dict[str, str], args: list[str]) -> subprocess.CompletedProcess[str]:
    """Run the installed datasluice console script with a checkout-free environment."""
    clean_env = {
        "PATH": str(Path(env_info["venv"]) / "bin"),
        "HOME": os.environ.get("HOME", ""),
        "LANG": "en_US.UTF-8",
        "LC_ALL": "en_US.UTF-8",
    }
    return subprocess.run(
        [env_info["console"], *args],
        capture_output=True,
        text=True,
        env=clean_env,
        timeout=60,
    )


def _write_csv(path: Path, rows: int = 5) -> None:
    """Write a small deterministic CSV file for scan/open/materialize tests."""
    lines = ["city,value\n"]
    for i in range(rows):
        lines.append(f"city-{i},{i}\n")
    path.write_text("".join(lines))


def test_installed_import_comes_from_venv_not_checkout(installed_env: dict[str, str]) -> None:
    """The installed distribution imports from the venv site-packages, never the checkout."""
    result = subprocess.run(
        [installed_env["python"], "-c", "import datasluice; print(datasluice.__file__)"],
        capture_output=True,
        text=True,
        env={
            "PATH": str(Path(installed_env["venv"]) / "bin"),
            "HOME": os.environ.get("HOME", ""),
        },
    )
    assert result.returncode == 0, result.stderr
    assert str(_REPO_ROOT / "src") not in result.stdout
    assert installed_env["venv"] in result.stdout


def test_installed_version_flag_reports_the_wheel_version(installed_env: dict[str, str]) -> None:
    """--version prints the wheel's own version."""
    result = _run_cli(installed_env, ["--version"])

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().startswith("datasluice ")


def test_all_connectors_unlocks_live_client_execution_gates(all_connectors_env: dict[str, str]) -> None:
    """The all-connectors wheel install provides httpx and passes every live-client gate."""
    result = subprocess.run(
        [
            all_connectors_env["python"],
            "-c",
            "import importlib\n"
            "import httpx\n"
            "platforms = ('ckan', 'udata', 'socrata')\n"
            "for platform in platforms:\n"
            "    module = importlib.import_module(f'datasluice.connectors.catalog.{platform}.live')\n"
            "    create = module.create_live_client\n"
            "    try:\n"
            "        create()\n"
            "    except NotImplementedError:\n"
            "        continue\n"
            "    except ImportError as exc:\n"
            "        raise AssertionError(f'{platform} extra gate did not unlock') from exc\n"
            "    raise AssertionError(f'{platform} live seam unexpectedly returned')\n",
        ],
        capture_output=True,
        text=True,
        env={"PATH": str(Path(all_connectors_env["venv"]) / "bin"), "HOME": os.environ.get("HOME", "")},
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_installed_help_advertises_exactly_the_retained_commands(installed_env: dict[str, str]) -> None:
    """Console help lists every retained direct command and no retired one."""
    result = _run_cli(installed_env, ["--help"])

    assert result.returncode == 0, result.stderr
    for command in ("scan", "open", "materialize"):
        assert command in result.stdout, f"installed --help missing command {command}"
    for retired in _RETIRED_COMMANDS:
        assert retired not in result.stdout, f"installed --help must not advertise retired command {retired}"


def test_installed_retired_commands_are_not_invokable(installed_env: dict[str, str]) -> None:
    """Former portal-era commands fail resolution instead of redirecting."""
    result = _run_cli(installed_env, ["search", "https://data.example.test"])

    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "No such command" in combined


def test_installed_scan_reads_local_csv(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Installed scan reads a local CSV and emits bounded JSON on stdout."""
    csv = tmp_path / "data.csv"
    _write_csv(csv)

    result = _run_cli(installed_env, ["scan", str(csv), "--output", "json"])

    assert result.returncode == 0, result.stderr
    parsed = json.loads(result.stdout)
    assert parsed["rows"] == 5
    assert len(parsed["columns"]) == 2


def test_installed_scan_failure_missing_file_exits_nonzero(installed_env: dict[str, str], tmp_path: Path) -> None:
    """A nonexistent file path fails with a nonzero exit and no stdout JSON."""
    result = _run_cli(installed_env, ["scan", str(tmp_path / "nonexistent.csv"), "--output", "json"])

    assert result.returncode != 0
    assert result.stdout == ""


def test_installed_open_previews_local_csv(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Installed open previews a local CSV as JSON on stdout."""
    csv = tmp_path / "data.csv"
    _write_csv(csv, rows=3)

    result = _run_cli(installed_env, ["open", str(csv), "--output", "json"])

    assert result.returncode == 0, result.stderr
    parsed = json.loads(result.stdout)
    assert len(parsed["rows"]) == 3


def test_installed_open_failure_missing_file_exits_nonzero(installed_env: dict[str, str], tmp_path: Path) -> None:
    """A nonexistent file fails without partial success output."""
    result = _run_cli(installed_env, ["open", str(tmp_path / "missing.csv")])

    assert result.returncode != 0
    assert result.stdout == ""


def test_installed_materialize_produces_artifact_json(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Installed materialize writes a Parquet artifact and emits canonical JSON."""
    csv = tmp_path / "data.csv"
    _write_csv(csv, rows=3)
    dest = tmp_path / "output.parquet"

    result = _run_cli(
        installed_env,
        ["materialize", str(csv), "--destination", str(dest), "--output", "json"],
    )

    assert result.returncode == 0, result.stderr
    artifact = json.loads(result.stdout)
    assert artifact["schema_version"] == 1
    assert artifact["kind"] == "artifact"
    assert "content_digest" in artifact
    assert dest.exists()


def test_installed_materialize_failure_missing_destination_exits_nonzero(
    installed_env: dict[str, str], tmp_path: Path
) -> None:
    """Missing --destination is rejected by Typer before any side effect."""
    csv = tmp_path / "data.csv"
    _write_csv(csv)

    result = _run_cli(installed_env, ["materialize", str(csv)])

    assert result.returncode != 0
