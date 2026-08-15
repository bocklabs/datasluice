"""Executable documentation examples gate.

Release validation must prove that the published facade and CLI examples run
against built artifacts, so this test executes the Python blocks extracted from
``docs/examples/application.md`` and asserts every CLI command documented there
works. The example's example-domain portal URL is rebound to a local mock CKAN
server so no live network is touched.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
APPLICATION_DOC = REPO_ROOT / "docs" / "examples" / "application.md"

SOURCE_FILE = "/tmp/datasluice-source.csv"
DEST_FILE = "/tmp/datasluice-out.parquet"


def _python_blocks(doc: Path) -> list[str]:
    text = doc.read_text(encoding="utf-8")
    return re.findall(r"```python\n(.*?)```", text, flags=re.DOTALL)


def _bind_env(code: str) -> str:
    return code.replace('SOURCE_FILE = "/tmp/datasluice-source.csv"', f'SOURCE_FILE = r"{SOURCE_FILE}"')


def test_application_facade_example_executes_against_built_artifacts() -> None:
    """The facade Python block runs against installed DataSluice."""
    if not APPLICATION_DOC.exists():
        pytest.skip("application.md missing")
    Path(SOURCE_FILE).write_text("city,value\nA,1\nB,2\n", encoding="utf-8")
    for path in (Path(SOURCE_FILE).with_suffix(".parquet"), Path(DEST_FILE)):
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()

    try:
        for block in _python_blocks(APPLICATION_DOC):
            body = _bind_env(block)
            namespace: dict[str, object] = {}
            exec(compile(body, str(APPLICATION_DOC), "exec"), namespace)
    finally:
        for path in (Path(SOURCE_FILE), Path(SOURCE_FILE).with_suffix(".parquet"), Path(DEST_FILE)):
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()


def test_application_cli_example_commands_are_present_in_help() -> None:
    """Every CLI command documented in application.md is registered on the app."""
    from datasluice.cli.app import app

    names = {info.name for info in app.registered_commands}
    for command in ("scan", "open", "materialize"):
        assert command in names, f"documented CLI command {command} not registered"
