"""Shared fixtures for datasluice.sync unit tests."""

from __future__ import annotations

import pytest

from datasluice.sync.state_store import FileStateStore


@pytest.fixture()
def memory_store() -> FileStateStore:
    """FileStateStore on the in-process fsspec ``memory://`` backend."""
    return FileStateStore("memory://state")


@pytest.fixture()
def file_store(tmp_path) -> FileStateStore:
    """FileStateStore on a local ``file://{tmp_path}/state`` directory."""
    return FileStateStore(f"file://{tmp_path}/state")
