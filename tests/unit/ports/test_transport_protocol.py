"""Protocol-level tests for the Transport and StreamingTransport ports.

These complement ``test_transport_conformance.py`` (which checks the existing
buffered ``HttpClient`` against ``Transport``) by asserting the Protocol
*shapes* themselves are ``@runtime_checkable`` and that the new
``HttpxTransport`` structurally satisfies **both** the buffered and streaming
Protocols.
"""

from __future__ import annotations

import importlib

import pytest

from datasluice.ports import StreamingTransport, Transport


def test_transport_protocol_is_runtime_checkable() -> None:
    """Transport must keep its runtime-checkable flag (carry-forward)."""

    assert getattr(Transport, "_is_runtime_protocol", False) is True


def test_streaming_transport_protocol_is_runtime_checkable() -> None:
    """StreamingTransport must be a @runtime_checkable Protocol."""

    assert getattr(StreamingTransport, "_is_runtime_protocol", False) is True


def test_streaming_transport_protocol_declares_stream() -> None:
    """StreamingTransport Protocol surface must declare ``stream``."""

    assert hasattr(StreamingTransport, "stream")


def test_httpx_transport_satisfies_both_protocols() -> None:
    """HttpxTransport must satisfy Transport AND StreamingTransport.

    The HttpxTransport module lands; until then this test skips
    cleanly so pre-commit's full suite and ``ty check`` stay green. Resolved
    via ``importlib.import_module`` (rather than a static ``import``) because
    the target module genuinely does not exist until — a static import
    would be flagged as ``unresolved-import`` by ``ty`` during the
    commit. Once ships the real module, the runtime resolution succeeds
    and the isinstance assertions execute.
    """

    pytest.importorskip("httpx")
    try:
        module = importlib.import_module("datasluice.transport.httpx_transport")
    except ImportError:
        pytest.skip("HttpxTransport lands in Task 2 (03-01)")
    transport = module.HttpxTransport()
    assert isinstance(transport, Transport)
    assert isinstance(transport, StreamingTransport)
