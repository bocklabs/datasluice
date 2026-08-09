"""Unit tests for data.gouv unsupported-query-field reject policy.

Covers the capabilities ClassVar gate (Test 1), the pre-flight reject on
``groups`` (Test 2 — udata has no ``groups`` param), the udata-native param
translation (Tests 3-6: ``format`` singular, ``license``,
``tag`` array, ``organization``), the all-supported-fields-no-raise case
(Test 7), and the mapper access/schema descriptors (Tests 8-9).
"""

from __future__ import annotations

from typing import Any

import pytest

from datasluice.connectors.datagouv import DataGouvAdapter
from datasluice.domain import Query
from datasluice.exceptions import UnsupportedQueryFieldError


class _StubTransport:
    """Stub satisfying the Transport Protocol structurally for reject-policy tests."""

    def __init__(self) -> None:
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
        return {"data": [], "total": 0}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


def _captured_params(stub: _StubTransport) -> dict[str, Any]:
    """Return the query-string params captured on the first request."""
    assert stub.requested, "no request was captured"
    _url, kwargs = stub.requested[0]
    params = kwargs.get("params")
    assert isinstance(params, dict)
    return params


def test_datagouv_search_rejects_groups_field() -> None:
    adapter = DataGouvAdapter("https://x", transport=_StubTransport())
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(groups=["transport"]))
    err = exc_info.value
    assert err.field == "groups"
    for expected in ("text", "tags", "organizations", "res_format", "license_id", "sort"):
        assert expected in err.supported_fields
    assert "groups" not in err.supported_fields


def test_datagouv_search_translates_res_format_to_singular_format_param() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    adapter.search(Query(res_format="CSV"))
    params = _captured_params(stub)
    assert params.get("format") == "CSV"
    assert "res_format" not in params


def test_datagouv_search_translates_license_id_to_license_param() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    adapter.search(Query(license_id="cc-by"))
    params = _captured_params(stub)
    assert params.get("license") == "cc-by"
    assert "license_id" not in params


def test_datagouv_search_translates_tags_to_tag_array_param() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    adapter.search(Query(tags=["economy", "budget"]))
    params = _captured_params(stub)
    assert params.get("tag") == ["economy", "budget"]


def test_datagouv_search_translates_organizations_to_organization_param() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    adapter.search(Query(organizations=["myorg"]))
    params = _captured_params(stub)
    assert params.get("organization") == "myorg"


def test_datagouv_search_all_supported_fields_no_raise() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    adapter.search(
        Query(
            text="water",
            tags=["t"],
            organizations=["o"],
            res_format="CSV",
            license_id="cc-by",
            sort="name",
        )
    )
    assert stub.requested


def test_datagouv_search_reject_gate_fires_before_transport_call() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    with pytest.raises(UnsupportedQueryFieldError):
        adapter.search(Query(groups=["transport"]))
    assert not stub.requested


def test_datagouv_search_empty_tags_list_treated_as_unset() -> None:
    stub = _StubTransport()
    adapter = DataGouvAdapter("https://x", transport=stub)
    adapter.search(Query(tags=[]))
    params = _captured_params(stub)
    assert "tag" not in params


def test_datagouv_capabilities_published_as_classvar() -> None:
    from datasluice.domain import CatalogCapabilities

    capabilities = DataGouvAdapter.capabilities
    assert isinstance(capabilities, CatalogCapabilities)
    assert capabilities.supports_search is True
    assert capabilities.supports_organizations is True
    assert capabilities.supported_query_fields == frozenset(
        {"text", "tags", "organizations", "res_format", "license_id", "sort"}
    )
