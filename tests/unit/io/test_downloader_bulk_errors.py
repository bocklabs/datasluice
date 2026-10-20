from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from datasluice.domain import Resource
from datasluice.exceptions import ChecksumMismatchError, DownloadError
from datasluice.io.downloader import Downloader
from datasluice.runtime.transport.base import CatalogTransport

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "error", [DownloadError("download failed"), ChecksumMismatchError("checksum failed"), OSError()]
)
def test_bulk_download_continues_after_download_and_filesystem_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    downloader = Downloader(Mock(spec=CatalogTransport))
    output = tmp_path / "downloaded.csv"
    download = Mock(side_effect=[error, output])
    monkeypatch.setattr(downloader, "download", download)
    resources = [Resource(id="failed"), Resource(id="succeeded")]

    assert downloader.download_many(resources, tmp_path) == [output]
    assert download.call_count == 2
    download.assert_called_with(resources[1], tmp_path)
