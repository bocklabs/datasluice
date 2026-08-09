"""Backend-error discrimination for FileStateStore.

Closes the blocker: only FileNotFoundError maps to None (absent state);
backend PermissionError, TimeoutError, and other OSError subclasses surface as
StateStoreError and are never swallowed as missing state.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from datasluice.exceptions import StateStoreError
from datasluice.sync.state_store import FileStateStore


def test_permission_error_raises_statestoreerror(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    def raise_permission(self: Any, path: str) -> bytes:
        raise PermissionError(f"denied: {path}")

    with patch.object(type(store._fs), "cat_file", raise_permission):
        with pytest.raises(StateStoreError):
            store.get(key)


def test_timeout_error_raises_statestoreerror(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    def raise_timeout(self: Any, path: str) -> bytes:
        raise TimeoutError(f"timed out reading: {path}")

    with patch.object(type(store._fs), "cat_file", raise_timeout):
        with pytest.raises(StateStoreError):
            store.get(key)


def test_generic_oserror_raises_statestoreerror(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    def raise_oserror(self: Any, path: str) -> bytes:
        raise OSError("connection reset")

    with patch.object(type(store._fs), "cat_file", raise_oserror):
        with pytest.raises(StateStoreError):
            store.get(key)


def test_file_not_found_returns_none(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    # No file written: get must return None (absent state), not raise.
    assert store.get(key) is None
    # read_version / read_raw follow the same discrimination.
    assert store.read_raw(key) is None
    assert store.read_version(key) is None


def test_read_version_raises_statestoreerror_on_backend_failure(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    def raise_permission(self: Any, path: str) -> bytes:
        raise PermissionError(f"denied: {path}")

    with patch.object(type(store._fs), "cat_file", raise_permission):
        with pytest.raises(StateStoreError):
            store.read_version(key)


def test_delete_raises_statestoreerror_on_backend_failure(tmp_path: Path) -> None:
    store = FileStateStore(f"file://{tmp_path}/state")
    key = "resource-1"

    def raise_oserror(self: Any, path: str) -> None:
        raise OSError("backend rm failure")

    with patch.object(type(store._fs), "rm", raise_oserror):
        with pytest.raises(StateStoreError):
            store.delete(key)
