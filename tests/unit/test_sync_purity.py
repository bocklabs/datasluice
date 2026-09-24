"""Bare-import guards for the dep-free datasluice.sync package core."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
import textwrap
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run_import_check(script: str) -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_REPO_ROOT / "src")
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr


_FORBIDDEN_OPTIONAL_MODULES = ("pyarrow", "dlt", "duckdb")


def _purge_optional_modules() -> None:
    for name in list(sys.modules):
        if name.split(".")[0] in _FORBIDDEN_OPTIONAL_MODULES:
            del sys.modules[name]


def _assert_optional_modules_absent() -> None:
    present = [name for name in _FORBIDDEN_OPTIONAL_MODULES if name in sys.modules]
    assert present == [], f"datasluice.sync pulled optional deps: {present}"


def test_sync_imports_zero_optional_deps() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import sys

        forbidden = %r
        for name in list(sys.modules):
            if name.split(".")[0] in forbidden:
                del sys.modules[name]
        importlib.import_module("datasluice.sync")
        present = [name for name in forbidden if name in sys.modules]
        assert present == [], present
        """
    ) % (_FORBIDDEN_OPTIONAL_MODULES,)
    _run_import_check(script)


def test_import_state_store_no_optional_deps() -> None:
    script = textwrap.dedent(
        """
        import importlib
        import sys

        forbidden = %r
        for name in list(sys.modules):
            if name.split(".")[0] in forbidden:
                del sys.modules[name]
        importlib.import_module("datasluice.sync.state_store")
        present = [name for name in forbidden if name in sys.modules]
        assert present == [], present
        """
    ) % (_FORBIDDEN_OPTIONAL_MODULES,)
    _run_import_check(script)


def test_session_sync_composition_imports_no_optional_deps() -> None:
    _purge_optional_modules()
    sys.modules.pop("datasluice.runtime.session", None)

    importlib.import_module("datasluice.runtime.session")

    _assert_optional_modules_absent()


def test_file_state_store_accessible() -> None:
    sync = importlib.import_module("datasluice.sync")

    assert sync.FileStateStore is not None
    assert sync.InMemoryStateStore is not None


def test_integration_dlt_imports_no_optional_dlt_module() -> None:
    _purge_optional_modules()
    sys.modules.pop("datasluice.integrations.dlt", None)

    importlib.import_module("datasluice.integrations.dlt")

    assert "dlt" not in sys.modules
