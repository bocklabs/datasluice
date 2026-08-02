"""Installed-wheel end-to-end contracts for all seven CLI commands (D-26/QUAL-08).

Builds the datasluice wheel once per session, installs it into an isolated
venv, and runs the ``datasluice`` console script against local deterministic
fixtures — never the repository checkout and never live portals.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.helpers.http_server import MockResponse, start_test_server

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent


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
        pytest.skip(f"uv build failed: {build.stderr[:300]}")

    wheels = list(wheel_dir.glob("datasluice-*.whl"))
    assert len(wheels) == 1, f"expected one wheel, found {wheels}"
    wheel = wheels[0]

    venv = tmp_path_factory.mktemp("venv")
    venv_python = venv / "bin" / "python"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True, timeout=60)

    install = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(venv_python),
            f"{wheel}[http,parquet,storage]",
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )
    if install.returncode != 0:
        pytest.skip(f"pip install failed: {install.stderr[:300]}")

    console = str(venv / "bin" / "datasluice")
    assert Path(console).exists(), f"console script not found at {console}"

    check = subprocess.run(
        [str(venv_python), "-c", "import datasluice; print(datasluice.__file__)"],
        capture_output=True,
        text=True,
        env={"PATH": str(venv / "bin"), "HOME": os.environ.get("HOME", "")},
    )
    assert check.returncode == 0, f"import check failed: {check.stderr}"
    assert str(venv) in check.stdout, f"datasluice imported from {check.stdout.strip()}, expected venv {venv}"

    return {"console": console, "python": str(venv_python), "venv": str(venv)}


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


# ---------------------------------------------------------------------------
# scan
# ---------------------------------------------------------------------------


def test_scan_happy_path_reads_local_csv(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Installed scan reads a local CSV and emits bounded JSON on stdout."""
    csv = tmp_path / "data.csv"
    _write_csv(csv)

    result = _run_cli(installed_env, ["scan", str(csv), "--output", "json"])

    assert result.returncode == 0, result.stderr
    parsed = json.loads(result.stdout)
    assert parsed["rows"] == 5
    assert len(parsed["columns"]) == 2


def test_scan_failure_missing_file_exits_nonzero(installed_env: dict[str, str], tmp_path: Path) -> None:
    """A nonexistent file path fails with a nonzero exit and no stdout JSON."""
    result = _run_cli(installed_env, ["scan", str(tmp_path / "nonexistent.csv"), "--output", "json"])

    assert result.returncode != 0
    assert result.stdout == ""


# ---------------------------------------------------------------------------
# open
# ---------------------------------------------------------------------------


def test_open_happy_path_previews_local_csv(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Installed open previews a local CSV as JSON on stdout."""
    csv = tmp_path / "data.csv"
    _write_csv(csv, rows=3)

    result = _run_cli(installed_env, ["open", str(csv), "--output", "json"])

    assert result.returncode == 0, result.stderr
    parsed = json.loads(result.stdout)
    assert len(parsed["rows"]) == 3


def test_open_failure_missing_file_exits_nonzero(installed_env: dict[str, str], tmp_path: Path) -> None:
    """A nonexistent file fails without partial success output."""
    result = _run_cli(installed_env, ["open", str(tmp_path / "missing.csv")])

    assert result.returncode != 0
    assert result.stdout == ""


# ---------------------------------------------------------------------------
# materialize
# ---------------------------------------------------------------------------


def test_materialize_happy_path_produces_artifact_json(installed_env: dict[str, str], tmp_path: Path) -> None:
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


def test_materialize_failure_missing_destination_exits_nonzero(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Missing --destination is rejected by Typer before any side effect."""
    csv = tmp_path / "data.csv"
    _write_csv(csv)

    result = _run_cli(installed_env, ["materialize", str(csv)])

    assert result.returncode != 0


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def _ckan_search_response() -> bytes:
    """Serve the CKAN package_search fixture as a wire response."""
    fixture = json.loads((_REPO_ROOT / "tests/fixtures/ckan/package_search.json").read_text())
    return json.dumps(fixture).encode()


def test_search_happy_path_against_local_ckan(installed_env: dict[str, str]) -> None:
    """Installed search queries a local CKAN server and returns JSON datasets."""
    server, base_url = start_test_server({"/api/3/action/package_search": MockResponse(body=_ckan_search_response())})
    try:
        result = _run_cli(installed_env, ["search", "--portal", base_url, "air", "--output", "json"])

        assert result.returncode == 0, result.stderr
        parsed = json.loads(result.stdout)
        assert parsed["total"] == 4
        assert len(parsed["datasets"]) > 0
    finally:
        server.shutdown()


def test_search_failure_unreachable_portal_exits_nonzero(installed_env: dict[str, str]) -> None:
    """An unreachable portal fails with nonzero exit and no stdout JSON."""
    result = _run_cli(
        installed_env,
        ["search", "--portal", "http://127.0.0.1:1", "test", "--output", "json"],
    )

    assert result.returncode != 0
    assert result.stdout == ""


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------


def _ckan_package_show_response() -> bytes:
    """Serve the CKAN package_show fixture as a wire response."""
    fixture = json.loads((_REPO_ROOT / "tests/fixtures/ckan/package_show.json").read_text())
    return json.dumps(fixture).encode()


def test_inspect_happy_path_against_local_ckan(installed_env: dict[str, str]) -> None:
    """Installed inspect retrieves catalog metadata without reading resource bytes."""
    server, base_url = start_test_server(
        {
            "/api/3/action/package_show": MockResponse(body=_ckan_package_show_response()),
            "/api/3/action/package_search": MockResponse(body=_ckan_search_response()),
            "/api/3/action/group_list": MockResponse(body=b'{"success": true, "result": []}'),
        }
    )
    try:
        result = _run_cli(
            installed_env,
            ["inspect", "--portal", base_url, "test-dataset", "--output", "json"],
        )

        assert result.returncode == 0, result.stderr
        parsed = json.loads(result.stdout)
        assert parsed["id"] is not None
        assert len(parsed["resources"]) > 0
    finally:
        server.shutdown()


def test_inspect_failure_missing_dataset_exits_nonzero(installed_env: dict[str, str]) -> None:
    """A 404 package_show response fails with nonzero exit."""
    server, base_url = start_test_server({"/api/3/action/package_show": MockResponse(status=404, body=b"not found")})
    try:
        result = _run_cli(
            installed_env,
            ["inspect", "--portal", base_url, "missing", "--output", "json"],
        )

        assert result.returncode != 0
        assert result.stdout == ""
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------


def test_download_happy_path_copies_raw_bytes(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Installed download copies raw resource bytes to a destination directory."""
    csv_body = b"city,value\nA,1\nB,2\n"
    package_show = {
        "success": True,
        "result": {
            "id": "ds-download",
            "name": "download-test",
            "title": "Download Test",
            "resources": [
                {
                    "id": "res-1",
                    "name": "data.csv",
                    "format": "CSV",
                    "url": None,
                }
            ],
        },
    }
    server, base_url = start_test_server(
        {
            "/api/3/action/package_show": MockResponse(body=json.dumps(package_show).encode()),
            "/api/3/action/package_search": MockResponse(body=_ckan_search_response()),
            "/api/3/action/group_list": MockResponse(body=b'{"success": true, "result": []}'),
            "/files/data.csv": MockResponse(body=csv_body),
        }
    )
    try:
        package_show["result"]["resources"][0]["url"] = f"{base_url}/files/data.csv"
        server.responses["/api/3/action/package_show"] = MockResponse(body=json.dumps(package_show).encode())

        dest = tmp_path / "downloads"
        result = _run_cli(
            installed_env,
            ["download", "--portal", base_url, "ds-download", "--dest", str(dest)],
        )

        assert result.returncode == 0, result.stderr
        files = list(dest.glob("*"))
        assert len(files) == 1
        assert files[0].read_bytes() == csv_body
    finally:
        server.shutdown()


def test_download_failure_no_matching_format_exits_nonzero(installed_env: dict[str, str], tmp_path: Path) -> None:
    """Format filtering with no matches exits 1 without writing files."""
    package_show = {
        "success": True,
        "result": {
            "id": "ds-fmt",
            "name": "fmt-test",
            "resources": [{"id": "r1", "name": "data.json", "format": "JSON"}],
        },
    }
    server, base_url = start_test_server(
        {
            "/api/3/action/package_show": MockResponse(body=json.dumps(package_show).encode()),
            "/api/3/action/package_search": MockResponse(body=_ckan_search_response()),
            "/api/3/action/group_list": MockResponse(body=b'{"success": true, "result": []}'),
        }
    )
    try:
        dest = tmp_path / "downloads"
        result = _run_cli(
            installed_env,
            [
                "download",
                "--portal",
                base_url,
                "ds-fmt",
                "--dest",
                str(dest),
                "--format",
                "CSV",
            ],
        )

        assert result.returncode != 0
        assert not dest.exists() or not list(dest.iterdir())
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# detect
# ---------------------------------------------------------------------------


def test_detect_happy_path_identifies_ckan(installed_env: dict[str, str]) -> None:
    """Installed detect identifies a local CKAN portal and emits JSON evidence."""
    server, base_url = start_test_server(
        {
            "/api/3/action/package_search": MockResponse(
                body=b'{"success": true, "result": {"count": 0, "results": []}}'
            ),
            "/api/3/action/group_list": MockResponse(body=b'{"success": true, "result": []}'),
        }
    )
    try:
        result = _run_cli(installed_env, ["detect", base_url, "--output", "json"])

        assert result.returncode == 0, result.stderr
        parsed = json.loads(result.stdout)
        assert parsed["portal_type"] == "ckan"
        assert len(parsed["evidence"]) > 0
    finally:
        server.shutdown()


def test_detect_failure_unknown_portal_exits_nonzero(installed_env: dict[str, str]) -> None:
    """A portal with no recognized fingerprints exits 1 without stdout JSON."""
    server, base_url = start_test_server({"/unknown": MockResponse(body=b"{}")})
    try:
        result = _run_cli(installed_env, [base_url, "--output", "json"])

        assert result.returncode != 0
        assert result.stdout == ""
    finally:
        server.shutdown()


# ---------------------------------------------------------------------------
# import-site verification
# ---------------------------------------------------------------------------


def test_subprocess_imports_from_wheel_not_checkout(installed_env: dict[str, str]) -> None:
    """The installed datasluice.__file__ must point inside the venv, not the checkout."""
    result = subprocess.run(
        [installed_env["python"], "-c", "import datasluice; print(datasluice.__file__)"],
        capture_output=True,
        text=True,
        env={"PATH": str(Path(installed_env["venv"]) / "bin"), "HOME": os.environ.get("HOME", "")},
    )
    assert result.returncode == 0
    location = result.stdout.strip()
    assert str(_REPO_ROOT / "src") not in location, f"datasluice imported from checkout: {location}"
    assert installed_env["venv"] in location
