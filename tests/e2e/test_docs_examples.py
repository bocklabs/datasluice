"""Executable canonical documentation examples gate.

Every Python fence in the canonical documentation must run against the
installed package using deterministic fixtures only: canonical platform
imports, pinned-profile fixture inspection, the reference-fake compliance
run, and the retained direct-resource data plane. The former local-CKAN
endpoint and removed-command execution is gone with the surfaces it
documented.

Data-plane example URLs are rebound to a fixture-owned local file before
execution, and a socket guard proves the examples never contact external
hosts or require credentials: reference fakes, pinned fixtures, and local
files are the only inputs. ``docs/examples/dlt.md`` shows the Phase 3+
caller-owned client pattern with an unbound placeholder, so it is scanned
statically but not executed; ``tests/helpers/http_server.py`` remains the
pattern for transport and lifecycle boundary tests, which this gate does
not exercise.
"""

from __future__ import annotations

import importlib
import re
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

EXECUTABLE_PAGES: tuple[Path, ...] = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "docs/connectors.md",
    REPO_ROOT / "docs/supported-portals.md",
    REPO_ROOT / "docs/examples/application.md",
    REPO_ROOT / "docs/examples/ckan.md",
    REPO_ROOT / "docs/examples/socrata.md",
)

OPTIONAL_EXTRA_PAGES: dict[str, Path] = {
    "pandas": REPO_ROOT / "docs/examples/pandas.md",
}

STATIC_ONLY_PAGES: tuple[Path, ...] = (REPO_ROOT / "docs/examples/dlt.md",)

CREDENTIAL_ACCESS_PATTERNS: tuple[str, ...] = (
    "os.environ",
    "os.getenv",
    "getpass",
    "keyring",
    "requests.get",
    "requests.post",
)

_FENCE_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)
_DATA_PLANE_PAGES: tuple[Path, ...] = (REPO_ROOT / "README.md", REPO_ROOT / "docs/examples/application.md")


def _python_blocks(doc: Path) -> list[str]:
    text = doc.read_text(encoding="utf-8") if doc.exists() else ""
    return _FENCE_RE.findall(text)


def _rebind_to_local_fixture(body: str, source: Path, destination: Path) -> str:
    rebound = body.replace('"https://example.org/data.csv"', f'"{source}"')
    rebound = rebound.replace('SOURCE_FILE = "/tmp/datasluice-source.csv"', f'SOURCE_FILE = r"{source}"')
    rebound = rebound.replace('"/tmp/datasluice-out.parquet"', f'"{destination}"')
    rebound = rebound.replace('"out.parquet"', f'"{destination}"')
    return rebound


@contextmanager
def _no_external_sockets() -> Iterator[None]:
    real_connect = socket.socket.connect
    real_create = socket.create_connection

    def blocked_connect(sock: socket.socket, address: Any) -> None:
        raise AssertionError(f"documented example contacted {address!r}")

    def blocked_create(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
        raise AssertionError(f"documented example contacted {address!r}")

    socket.socket.connect = blocked_connect  # ty: ignore[invalid-assignment]
    socket.create_connection = blocked_create  # ty: ignore[invalid-assignment]
    try:
        yield
    finally:
        socket.socket.connect = real_connect
        socket.create_connection = real_create


def _execute_page(page: Path, tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("city,value\nA,1\nB,2\n", encoding="utf-8")
    destination = tmp_path / "out.parquet"
    for block in _python_blocks(page):
        body = _rebind_to_local_fixture(block, source, destination)
        namespace: dict[str, object] = {}
        with _no_external_sockets():
            exec(compile(body, str(page), "exec"), namespace)


@pytest.mark.parametrize(
    "page",
    [pytest.param(page, id=page.relative_to(REPO_ROOT).as_posix()) for page in EXECUTABLE_PAGES],
)
def test_canonical_imports_profile_inspection_and_compliance_examples_execute(page: Path, tmp_path: Path) -> None:
    """Test 1: documented canonical, profile-inspection, and compliance examples run."""
    _execute_page(page, tmp_path)


def test_optional_extra_example_pages_execute_when_the_extra_is_installed(tmp_path: Path) -> None:
    """Test 1: optional-extra pages run under the same offline harness when installed."""
    for extra, page in OPTIONAL_EXTRA_PAGES.items():
        try:
            importlib.import_module(extra)
        except ImportError:
            pytest.skip(f"optional extra {extra!r} not installed")
        _execute_page(page, tmp_path)


@pytest.mark.parametrize(
    "page", [pytest.param(page, id=page.relative_to(REPO_ROOT).as_posix()) for page in _DATA_PLANE_PAGES]
)
def test_direct_data_plane_examples_execute_without_a_portal_connector(page: Path, tmp_path: Path) -> None:
    """Test 2: the facade flow runs from a local file with no connector import."""
    for block in _python_blocks(page):
        if "DataSluice(" in block:
            assert "datasluice.connectors" not in block, "facade flow must not construct portal connectors"
    _execute_page(page, tmp_path)


@pytest.mark.parametrize(
    "page",
    [
        pytest.param(page, id=page.relative_to(REPO_ROOT).as_posix())
        for page in (*EXECUTABLE_PAGES, *OPTIONAL_EXTRA_PAGES.values(), *STATIC_ONLY_PAGES)
    ],
)
def test_documented_examples_never_contact_external_hosts_or_read_credentials(page: Path) -> None:
    """Test 3: no documented fence fetches credentials or opens network sessions."""
    for block in _python_blocks(page):
        for pattern in CREDENTIAL_ACCESS_PATTERNS:
            assert pattern not in block, f"{page.name} fence reads credentials or web sessions ({pattern})"


def test_retained_cli_commands_document_the_direct_data_plane() -> None:
    """Test 4: the documented CLI keeps exactly the retained direct-resource commands."""
    from datasluice.cli.app import app

    names = {info.name for info in app.registered_commands}
    assert names == {"scan", "open", "materialize"}
    application_doc = (REPO_ROOT / "docs/examples/application.md").read_text(encoding="utf-8")
    for command in names:
        assert re.search(rf"datasluice {command}\b", application_doc), f"{command} missing from application docs"
