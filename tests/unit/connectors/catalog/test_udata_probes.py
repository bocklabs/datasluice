from __future__ import annotations

import json

import pytest

from datasluice.connectors.catalog.udata.probes import (
    UDataVersionError,
    parse_site_version,
    require_supported_version,
)
from datasluice.domain.catalog.auth import UDataCredential


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


def test_require_supported_version_rejects_any_other_release() -> None:
    observed = parse_site_version(_site_payload(version="18.0.0"))

    with pytest.raises(UDataVersionError, match="outside the supported range"):
        require_supported_version(observed)


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
    gate.require_current(UDataCredential(api_key="other-key"))
    assert transport.calls == 3


@pytest.mark.parametrize("status", [401, 403, 423])
def test_site_access_errors_preserve_status_and_capability_state(status: int) -> None:
    from datasluice.connectors.catalog.udata.probes import SiteVersionGate
    from datasluice.errors.catalog import CatalogError
    from tests.helpers.udata_test_support import sync_route_table

    router = sync_route_table({("GET", "http://127.0.0.1:5640/api/1/site/"): (status, {})})
    gate = SiteVersionGate(origin="http://127.0.0.1:5640", transport=router, ttl_seconds=60)
    with pytest.raises(CatalogError) as raised:
        gate.require_current(None)
    assert raised.value.metadata["status_code"] == status
    assert raised.value.capability_state == {401: "unauthorized", 403: "forbidden", 423: "deployment-disabled"}[status]
