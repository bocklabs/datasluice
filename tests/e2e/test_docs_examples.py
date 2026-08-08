"""Executable documentation examples gate (D-30).

Release validation must prove that the published facade and CLI examples run
against built artifacts, so this test executes the Python blocks extracted from
``docs/examples/application.md`` and asserts every CLI command documented there
works. The example's example-domain portal URL is rebound to a local mock CKAN
server so no live network is touched.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

import pytest

from tests.helpers.http_server import MockResponse, start_test_server

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
APPLICATION_DOC = REPO_ROOT / "docs" / "examples" / "application.md"
EXAMPLE_PORTAL = "https://catalog.example.test/api"

_MOCK_CKAN = {
    "/api/3/action/package_search": MockResponse(
        body=b'{"success": true, "result": {"count": 2, "results": ['
        b'{"id": "dataset-1", "name": "climate", "title": "Climate", "resources": []},'
        b'{"id": "dataset-2", "name": "weather", "title": "Weather", "resources": []}'
        b"]}}"
    ),
    "/api/3/action/package_show": MockResponse(
        body=json.dumps(
            {
                "success": True,
                "result": {
                    "id": "dataset-1",
                    "name": "climate",
                    "title": "Climate",
                    "resources": [
                        {
                            "id": "resource-1",
                            "name": "data.csv",
                            "format": "CSV",
                            "url": "PLACEHOLDER_URL",
                        }
                    ],
                },
            }
        ).encode()
    ),
    "/api/3/action/group_list": MockResponse(body=b'{"success": true, "result": []}'),
}

SOURCE_FILE = "/tmp/datasluice-source.csv"
DEST_FILE = "/tmp/datasluice-out.parquet"


def _python_blocks(doc: Path) -> list[str]:
    text = doc.read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def _bind_env(code: str, base_url: str) -> str:
    return code.replace(EXAMPLE_PORTAL, base_url).replace(
        'SOURCE_FILE = "/tmp/datasluice-source.csv"', f'SOURCE_FILE = r"{SOURCE_FILE}"'
    )


def test_application_facade_example_executes_against_built_artifacts() -> None:
    """The facade Python block runs against installed DataSluice and a local portal."""
    if not APPLICATION_DOC.exists():
        pytest.skip("application.md missing")
    Path(SOURCE_FILE).write_text("city,value\nA,1\nB,2\n", encoding="utf-8")
    for path in (Path(SOURCE_FILE).with_suffix(".parquet"), Path(DEST_FILE)):
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

    server, base_url = start_test_server(dict(_MOCK_CKAN))
    try:
        _MOCK_CKAN["/api/3/action/package_show"] = MockResponse(
            body=json.dumps(
                {
                    "success": True,
                    "result": {
                        "id": "dataset-1",
                        "name": "climate",
                        "title": "Climate",
                        "resources": [
                            {"id": "resource-1", "name": "data.csv", "format": "CSV", "url": f"{base_url}/file.csv"}
                        ],
                    },
                }
            ).encode()
        )
        server.responses["/api/3/action/package_show"] = _MOCK_CKAN["/api/3/action/package_show"]
        server.responses["/file.csv"] = MockResponse(body=b"city,value\nA,1\nB,2\n")
        _MOCK_CKAN["/api/3/action/package_search"] = MockResponse(
            body=b'{"success": true, "result": {"count": 2, "results": ['
            b'{"id": "dataset-1", "name": "climate", "title": "Climate", "resources": []},'
            b'{"id": "dataset-2", "name": "weather", "title": "Weather", "resources": []}'
            b"]}}"
        )
        for block in _python_blocks(APPLICATION_DOC):
            body = _bind_env(block, base_url)
            namespace: dict[str, object] = {}
            exec(compile(body, str(APPLICATION_DOC), "exec"), namespace)
    finally:
        server.shutdown()
        for path in (Path(SOURCE_FILE), Path(SOURCE_FILE).with_suffix(".parquet"), Path(DEST_FILE)):
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()


def test_application_cli_example_commands_are_present_in_help() -> None:
    """Every CLI command documented in application.md is registered on the app."""
    from datasluice.cli.app import app

    names = {info.name for info in app.registered_commands}
    for command in ("search", "inspect", "download", "detect", "scan", "open", "materialize"):
        assert command in names, f"documented CLI command {command} not registered"
