"""Stdlib catalog transport safety tests."""

from __future__ import annotations

import pytest

from datasluice.domain.catalog.observability import TLSPolicy
from datasluice.runtime.transport.base import RuntimeRequest
from datasluice.runtime.transport.urllib_transport import UrllibCatalogTransport
from tests.helpers.catalog_transport import SyncLoopbackTransport


def test_urllib_transport_uses_verified_tls_by_default() -> None:
    assert UrllibCatalogTransport()._tls_policy.verify


def test_tls_policy_rejects_unscoped_disablement() -> None:
    with pytest.raises(ValueError, match="explicit narrow override"):
        TLSPolicy(verify=False)


def test_loopback_sync_transport_refuses_non_loopback_targets() -> None:
    transport = SyncLoopbackTransport()

    with pytest.raises(ValueError):
        transport.get("https://127.0.0.1:443/")
    with pytest.raises(ValueError):
        transport.get("http://example.test:80/")


def test_runtime_request_freezes_header_mapping() -> None:
    request = RuntimeRequest("GET", "http://127.0.0.1:8000/", {"Authorization": "Bearer secret"})

    assert dict(request.headers) == {"Authorization": "Bearer secret"}
