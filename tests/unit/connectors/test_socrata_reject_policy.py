"""Unit tests for Socrata unsupported-query-field reject policy.

Covers the lean capabilities ClassVar (Test 1: only ``{text, tags, sort}``;
``supports_organizations=False``), the pre-flight reject on every unsupported
field (Tests 2-5: organizations/groups/res_format/license_id), the
all-supported-fields-no-raise case (Test 6), the honest ``sort``→``order``
translation (: ``order`` token mapping, ascending-only, unmappable or
descending specs rejected pre-flight), and the gate-fires-before-
transport property.
"""

from __future__ import annotations

from typing import Any

import pytest

from datasluice.connectors.socrata import SocrataAdapter
from datasluice.domain import CatalogCapabilities, Query
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
        return {"results": [], "resultSetSize": 0}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


def test_socrata_capabilities_lean_supported_query_fields() -> None:
    capabilities = SocrataAdapter.capabilities
    assert isinstance(capabilities, CatalogCapabilities)
    assert capabilities.supported_query_fields == frozenset({"text", "tags", "sort"})
    assert capabilities.supports_organizations is False


def test_socrata_search_rejects_organizations_field() -> None:
    adapter = SocrataAdapter("https://x", transport=_StubTransport())
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(organizations=["myorg"]))
    err = exc_info.value
    assert err.field == "organizations"
    for expected in ("text", "tags", "sort"):
        assert expected in err.supported_fields
    for unsupported in ("organizations", "groups", "res_format", "license_id"):
        assert unsupported not in err.supported_fields


def test_socrata_search_rejects_groups_field() -> None:
    adapter = SocrataAdapter("https://x", transport=_StubTransport())
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(groups=["x"]))
    assert exc_info.value.field == "groups"


def test_socrata_search_rejects_res_format_field() -> None:
    adapter = SocrataAdapter("https://x", transport=_StubTransport())
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(res_format="CSV"))
    assert exc_info.value.field == "res_format"


def test_socrata_search_rejects_license_id_field() -> None:
    adapter = SocrataAdapter("https://x", transport=_StubTransport())
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(license_id="cc-by"))
    assert exc_info.value.field == "license_id"


def test_socrata_search_all_supported_fields_no_raise() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    adapter.search(Query(text="water", tags=["t"], sort="name"))
    assert stub.requested


def _captured_params(stub: _StubTransport) -> dict[str, Any]:
    """Return the query-string params captured on the first request."""
    assert stub.requested, "no request was captured"
    _url, kwargs = stub.requested[0]
    params = kwargs.get("params")
    assert isinstance(params, dict)
    return params


def test_socrata_search_translates_sort_to_order_param() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    adapter.search(Query(sort="name"))
    params = _captured_params(stub)
    assert params.get("order") == "name"
    assert "sort" not in params


def test_socrata_search_translates_ascending_sort() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    adapter.search(Query(sort="name asc"))
    assert _captured_params(stub).get("order") == "name"


def test_socrata_search_maps_ckan_style_sort_field_to_order_token() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    adapter.search(Query(sort="metadata_modified"))
    assert _captured_params(stub).get("order") == "updatedAt"


def test_socrata_search_rejects_descending_sort() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(sort="name desc"))
    assert exc_info.value.field == "sort"
    assert exc_info.value.portal_name == "socrata"
    assert not stub.requested


def test_socrata_search_rejects_unmappable_sort_field() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(sort="bogus_field"))
    assert exc_info.value.field == "sort"
    assert not stub.requested


def test_socrata_search_rejects_malformed_sort_spec() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        adapter.search(Query(sort="name asc extra"))
    assert exc_info.value.field == "sort"
    assert not stub.requested


def test_socrata_search_reject_gate_fires_before_transport_call() -> None:
    stub = _StubTransport()
    adapter = SocrataAdapter("https://x", transport=stub)
    with pytest.raises(UnsupportedQueryFieldError):
        adapter.search(Query(organizations=["myorg"]))
    assert not stub.requested
