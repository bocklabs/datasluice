"""Unit tests for Transport Protocol structural conformance with HttpClient."""

from __future__ import annotations

from datasluice.ports import Transport
from datasluice.transport import HttpClient


def test_http_client_satisfies_transport_protocol() -> None:
    client = HttpClient()
    assert isinstance(client, Transport)


def test_transport_protocol_methods_exist_on_http_client() -> None:
    client = HttpClient()
    for method in ("request", "get_json", "download"):
        assert callable(getattr(client, method, None)), f"HttpClient missing callable {method}"


def test_http_client_is_runtime_checkable_transport() -> None:
    assert getattr(Transport, "_is_runtime_protocol", False) is True
