"""Unit tests for the unsupported-query-field reject policy.

Follows the RED->GREEN TDD pattern: the module skips cleanly at
collection time while the reject helper / exception / capabilities are not yet
implemented, then runs and passes once GREEN lands the real code.
"""

from __future__ import annotations

import importlib
from typing import Any

import pytest

try:
    _reject_mod = importlib.import_module("datasluice.connectors._reject")
    _exceptions_mod = importlib.import_module("datasluice.exceptions")
    _ckan_adapter_mod = importlib.import_module("datasluice.connectors.ckan.adapter")
    _has_unsupported = hasattr(_exceptions_mod, "UnsupportedQueryFieldError")
    _has_capabilities = hasattr(_ckan_adapter_mod.CKANAdapter, "capabilities")
    if not (_has_unsupported and _has_capabilities):
        raise ImportError("RED phase: implementation pending")
except ImportError:
    pytest.skip("Reject policy implementation pending", allow_module_level=True)

from datasluice.connectors.ckan import CKANAdapter  # noqa: E402
from datasluice.domain import DetectionResult, Query  # noqa: E402
from datasluice.exceptions import (  # noqa: E402
    DataSluiceError,
    PortalDetectionError,
    PortalError,
    UnsupportedQueryFieldError,
)

_reject_unsupported_fields = _reject_mod._reject_unsupported_fields


def test_unsupportedqueryfielderror_hierarchy() -> None:
    assert issubclass(UnsupportedQueryFieldError, DataSluiceError)
    assert not issubclass(UnsupportedQueryFieldError, PortalError)


def test_unsupportedqueryfielderror_kwonly_init_auto_message() -> None:
    err = UnsupportedQueryFieldError(field="groups", supported_fields=["tags", "text"], portal_name="datagouv")
    message = str(err)
    assert "groups" in message
    assert "datagouv" in message
    assert "tags" in message
    assert "text" in message
    assert err.field == "groups"
    assert err.supported_fields == ["tags", "text"]
    assert err.portal_name == "datagouv"


def test_unsupportedqueryfielderror_no_supported_fields_renders_none() -> None:
    err = UnsupportedQueryFieldError(field="tags", supported_fields=[], portal_name="test")
    assert "(none)" in str(err)


def test_portaldetectionerror_backward_compat() -> None:
    err = PortalDetectionError("msg")
    assert err.detection_result is None
    assert str(err) == "msg"


def test_portaldetectionerror_carries_detection_result() -> None:
    detection_result = DetectionResult(portal_type=None)
    err = PortalDetectionError("msg", detection_result=detection_result)
    assert err.detection_result is detection_result


def test_reject_helper_isolation_raises() -> None:
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        _reject_unsupported_fields(Query(tags=["x"]), frozenset({"text"}), "test")
    err = exc_info.value
    assert err.field == "tags"
    assert "text" in err.supported_fields
    assert err.portal_name == "test"


def test_reject_helper_empty_query_passes() -> None:
    assert _reject_unsupported_fields(Query(), frozenset(), "test") is None


def test_reject_helper_empty_list_treated_as_unset() -> None:
    assert _reject_unsupported_fields(Query(tags=[]), frozenset(), "test") is None


def test_reject_helper_deterministic_field_order() -> None:
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        _reject_unsupported_fields(
            Query(tags=["x"], organizations=["y"]),
            frozenset(),
            "test",
        )
    assert exc_info.value.field == "tags"


def test_reject_helper_supports_all_declaration_order() -> None:
    with pytest.raises(UnsupportedQueryFieldError) as exc_info:
        _reject_unsupported_fields(
            Query(organizations=["y"], tags=["x"]),
            frozenset(),
            "test",
        )
    assert exc_info.value.field == "tags"


class _StubTransport:
    """Stub satisfying the Transport Protocol structurally for reject-policy tests."""

    def __init__(self) -> None:
        self.requested: list[tuple[str, dict[str, object]]] = []

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
        return {"result": {"results": [], "count": 0}}

    def download(self, url: str, **kwargs: Any) -> bytes:
        return b""


def test_ckan_search_all_supported_fields_no_raise() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(
        Query(
            tags=["economy"],
            organizations=["myorg"],
            groups=["g"],
            res_format="CSV",
            license_id="cc-by",
            sort="name asc",
        )
    )
    assert stub.requested


def _captured_fq(stub: _StubTransport) -> str:
    """Return the ``fq`` query-string param captured on the first request."""
    assert stub.requested, "no request was captured"
    _url, kwargs = stub.requested[0]
    params = kwargs.get("params")
    assert isinstance(params, dict)
    return str(params.get("fq", ""))


def test_ckan_search_translates_tags_to_fq() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(tags=["economy"]))
    assert "tags:economy" in _captured_fq(stub)


def test_ckan_search_translates_organizations_to_fq() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(organizations=["myorg"]))
    assert "organization:myorg" in _captured_fq(stub)


def test_ckan_search_translates_groups_to_fq() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(groups=["transport"]))
    assert "groups:transport" in _captured_fq(stub)


def test_ckan_search_translates_res_format_to_fq() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(res_format="CSV"))
    assert "res_format:CSV" in _captured_fq(stub)


def test_ckan_search_translates_license_id_to_fq() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(license_id="cc-by"))
    assert "license_id:cc-by" in _captured_fq(stub)


def test_ckan_search_translates_multi_field_fq() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(tags=["economy"], organizations=["myorg"]))
    fq = _captured_fq(stub)
    assert "tags:economy" in fq
    assert "organization:myorg" in fq


def test_ckan_search_omits_fq_when_no_filter_set() -> None:
    stub = _StubTransport()
    adapter = CKANAdapter("https://x", transport=stub)
    adapter.search(Query(text="climate"))
    assert stub.requested
    _url, kwargs = stub.requested[0]
    params = kwargs.get("params")
    assert isinstance(params, dict)
    assert "fq" not in params
