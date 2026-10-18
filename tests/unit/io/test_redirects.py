from __future__ import annotations

import pytest

from datasluice.exceptions import DownloadError
from datasluice.io._redirects import _MAX_HOPS, follow_redirects
from datasluice.runtime.transport.base import RuntimeResponse

_PUBLIC_IP = "93.184.216.34"


def _redirecting(url: str) -> RuntimeResponse:
    return RuntimeResponse(302, {"Location": f"https://{_PUBLIC_IP}/next"}, b"")


def test_follow_redirects_reports_redirect_limit() -> None:
    sends: list[str] = []

    def send(url: str) -> RuntimeResponse:
        sends.append(url)
        return _redirecting(url)

    with pytest.raises(DownloadError, match="exceeded its redirect limit"):
        follow_redirects(
            f"https://{_PUBLIC_IP}/start",
            allow_private=False,
            label="Download",
            error=DownloadError,
            send=send,
        )

    assert len(sends) == _MAX_HOPS + 1


def test_follow_redirects_discards_the_final_redirect_response() -> None:
    discarded: list[RuntimeResponse] = []

    with pytest.raises(DownloadError, match="exceeded its redirect limit"):
        follow_redirects(
            f"https://{_PUBLIC_IP}/start",
            allow_private=False,
            label="Download",
            error=DownloadError,
            send=_redirecting,
            on_discard=discarded.append,
        )

    assert len(discarded) == _MAX_HOPS + 1
