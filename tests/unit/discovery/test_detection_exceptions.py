"""Exception-handling tests — specific tuple caught, unexpected propagate.

The legacy ``except Exception: continue`` (detector.py:56) silently swallowed
transport-layer defects. The rewrite catches ONLY the
``(NotFoundError, PortalError, OSError)`` tuple and lets every other exception
propagate so defects surface as test failures, not silent misses.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from datasluice.discovery.detector import detect
from datasluice.exceptions import NotFoundError, PortalError
from datasluice.runtime.plugin_manager import PluginManager


def _pm() -> PluginManager:
    pm = PluginManager()
    pm.register("ckan", lambda ctx: None)
    pm.register("udata", lambda ctx: None)
    pm.register("socrata", lambda ctx: None)
    return pm


class _RaisingTransport:
    """Raises *exc_factory(url)* per request to drive every probe to miss."""

    def __init__(self, exc_factory) -> None:  # type: ignore[no-untyped-def]
        self.exc_factory = exc_factory

    def request(self, url: str, **kwargs: Any) -> bytes:
        raise self.exc_factory(url)

    def get_json(self, url: str, **kwargs: Any) -> dict[str, Any]:
        raise self.exc_factory(url)

    def download(self, url: str, **kwargs: Any) -> bytes:
        raise self.exc_factory(url)


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda url: NotFoundError(f"404 at {url}"),
        lambda url: PortalError(f"portal error at {url}"),
        lambda url: OSError(f"conn error {url}"),
    ],
    ids=["NotFoundError", "PortalError", "OSError"],
)
def test_specific_exceptions_become_misses(exc_factory, caplog) -> None:  # type: ignore[no-untyped-def]
    """Each entry in ``_PROBE_EXCEPTIONS`` is caught, logged at DEBUG, recorded."""

    transport = _RaisingTransport(exc_factory)
    with caplog.at_level("DEBUG", logger="datasluice.discovery"):
        result = detect("https://example.gov", transport, _pm())
    assert result.portal_type is None
    assert result.confidence == 0.0
    assert len(result.evidence) == 6
    assert all(not ev.matched for ev in result.evidence)
    assert any("missed" in rec.message for rec in caplog.records)


def test_httpx_connecterror_is_not_caught_by_default(caplog) -> None:  # type: ignore[no-untyped-def]
    """``Httpx.ConnectError`` is NOT in ``_PROBE_EXCEPTIONS``.

    Callers injecting ``HttpxTransport`` may see httpx-native exceptions
    propagate. This documents the limitation noted on ``_PROBE_EXCEPTIONS``;
    the CLI uses ``HttpClient()`` (urllib) by default where this is moot.
    """

    transport = _RaisingTransport(lambda url: httpx.ConnectError(f"cannot connect {url}"))
    with pytest.raises(httpx.ConnectError):
        detect("https://example.gov", transport, _pm())


@pytest.mark.parametrize(
    "exc_factory",
    [
        lambda url: TypeError(f"bug at {url}"),
        lambda url: RuntimeError(f"bug at {url}"),
        lambda url: AttributeError(f"bug at {url}"),
    ],
    ids=["TypeError", "RuntimeError", "AttributeError"],
)
def test_unexpected_exceptions_propagate(exc_factory) -> None:  # type: ignore[no-untyped-def]
    """``TypeError``/``RuntimeError``/``AttributeError`` propagate, NOT silenced."""

    transport = _RaisingTransport(exc_factory)
    with pytest.raises((TypeError, RuntimeError, AttributeError)):
        detect("https://example.gov", transport, _pm())


def test_keyboard_interrupt_propagates() -> None:
    """``KeyboardInterrupt`` is a ``BaseException`` and MUST propagate."""

    transport = _RaisingTransport(lambda url: KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        detect("https://example.gov", transport, _pm())
