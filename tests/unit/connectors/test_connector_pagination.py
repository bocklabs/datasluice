"""Regression tests for connector pagination and CKAN success-envelope handling."""

from __future__ import annotations

from typing import Any

import pytest

from datasluice.connectors.ckan import CKANAdapter
from datasluice.domain import Query
from datasluice.exceptions import PortalError


class _StubTransport:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload
        self.requested: list[tuple[str, dict[str, Any]]] = []

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes:
        return b""

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        self.requested.append((url, dict(kwargs)))
        return self._payload

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


def test_ckan_limit_zero_does_not_report_has_next() -> None:
    payload = {"success": True, "result": {"results": [], "count": 5}}
    adapter = CKANAdapter("https://x", transport=_StubTransport(payload))
    result = adapter.search(Query(text="data", limit=0))
    assert result.has_next is False


def test_ckan_success_false_raises_portal_error() -> None:
    payload = {"success": False, "error": {"message": "bad query"}}
    adapter = CKANAdapter("https://x", transport=_StubTransport(payload))
    with pytest.raises(PortalError):
        adapter.search(Query(text="data"))
