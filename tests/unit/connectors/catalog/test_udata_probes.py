"""Unit tests for the strict uData site-version gate."""

from __future__ import annotations

import json

import pytest

from datasluice.connectors.catalog.udata.probes import (
    UDataVersionError,
    parse_site_version,
    require_exact_version,
)


def _site_payload(version: str = "17.6.0") -> dict[str, object]:
    return {
        "feed_size": 1,
        "id": "site-evidence",
        "keywords": [],
        "metrics": {},
        "title": "uData",
        "version": version,
    }


def test_parse_site_version_accepts_the_exact_stock_document() -> None:
    observed = parse_site_version(_site_payload())

    assert observed.version == "17.6.0"
    assert observed.site_id == "site-evidence"


@pytest.mark.parametrize(
    ("payload", "state"),
    [
        ({}, "missing"),
        (_site_payload(version="not-a-version"), "malformed"),
        (_site_payload(version="17.6.0.0"), "malformed"),
        ({**_site_payload(), "title": "17.6.0"}, "ambiguous"),
    ],
)
def test_parse_site_version_fails_closed_with_typed_states(payload: dict[str, object], state: str) -> None:
    with pytest.raises(UDataVersionError) as excinfo:
        parse_site_version(payload)

    assert excinfo.value.version_state == state
    assert excinfo.value.safe_action


def test_require_exact_version_rejects_any_other_release() -> None:
    observed = parse_site_version(_site_payload(version="17.5.9"))

    with pytest.raises(UDataVersionError, match="not the pinned"):
        require_exact_version(observed, "17.6.0")


def test_gate_caches_one_anonymous_probe_per_caller_identity() -> None:
    from datasluice.connectors.catalog.udata.probes import SiteVersionGate
    from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

    class Transport:
        def __init__(self) -> None:
            self.calls = 0

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            self.calls += 1
            return RuntimeResponse(status_code=200, headers={}, body=json.dumps(_site_payload()).encode())

        def close(self) -> None:
            return None

    transport = Transport()
    gate = SiteVersionGate(
        pinned_version="17.6.0",
        origin="http://127.0.0.1:5640",
        transport=transport,
        ttl_seconds=60,
        clock=lambda: 0.0,
    )

    first = gate.require_current(None)
    second = gate.require_current(None)

    assert first == second
    assert transport.calls == 1
    gate.invalidate()
    gate.require_current(None)
    assert transport.calls == 2
