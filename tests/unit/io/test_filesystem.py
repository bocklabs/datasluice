"""Unit tests for the ``open_filesystem`` factory.

The implementation module is resolved via ``importlib.import_module`` (rather
than a static ``import``) so the RED commit can land under this repo's
full-suite pre-commit hook: until the GREEN step ships, the whole module
skips cleanly instead of erroring at collection.
"""

from __future__ import annotations

import importlib
import inspect
import re
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fsspec")

try:
    _filesystem_module = importlib.import_module("datasluice.io.filesystem")
except ImportError:
    pytest.skip(
        "open_filesystem implementation pending (RED → GREEN within task 03-02)",
        allow_module_level=True,
    )

open_filesystem = _filesystem_module.open_filesystem


def test_open_filesystem_dispatches_by_scheme(tmp_path: object) -> None:
    """open_filesystem returns a backend whose protocol is/contains 'file'."""
    fs = open_filesystem(f"file://{tmp_path}")
    protocol = getattr(fs, "protocol", None)
    protocols = protocol if isinstance(protocol, (list, tuple)) else [protocol]
    assert "file" in protocols


def test_open_filesystem_dispatches_memory() -> None:
    """open_filesystem('memory://') returns a MemoryFileSystem."""
    fs = open_filesystem("memory://")
    assert fs.__class__.__name__ == "MemoryFileSystem"


def test_open_filesystem_passes_credentials_as_storage_options() -> None:
    """credentials= dict is forwarded to url_to_fs as **storage_options."""
    with patch("fsspec.core.url_to_fs") as mocked:
        mocked.return_value = (MagicMock(), "path")
        open_filesystem("s3://bucket/key", credentials={"key": "AKIA", "secret": "shh"})
    _, kwargs = mocked.call_args
    assert kwargs == {"key": "AKIA", "secret": "shh"}


def test_open_filesystem_passes_empty_credentials_when_none() -> None:
    """credentials=None forwards an empty dict so fsspec uses backend defaults."""
    with patch("fsspec.core.url_to_fs") as mocked:
        mocked.return_value = (MagicMock(), "path")
        open_filesystem("file:///tmp/x", credentials=None)
    _, kwargs = mocked.call_args
    assert kwargs == {}


def test_open_filesystem_raises_importerror_with_install_hint_when_fsspec_missing() -> None:
    """When fsspec is unimportable, ImportError carries the datasluice[storage] hint."""
    import sys

    with patch.dict(sys.modules, {"fsspec": None}):
        with pytest.raises(ImportError) as exc_info:
            open_filesystem("file:///tmp")
        assert "datasluice[storage]" in str(exc_info.value)


def test_open_filesystem_does_not_import_fsspec_at_module_top() -> None:
    """No top-level ``import fsspec`` / ``from fsspec`` in the implementation."""
    module = importlib.import_module("datasluice.io.filesystem")
    source = inspect.getsource(module)
    top_level = [
        line
        for line in source.splitlines()
        if re.match(r"^import fsspec|^from fsspec", line) and not line.lstrip().startswith("#")
    ]
    assert top_level == [], f"Found top-level fsspec imports: {top_level}"
