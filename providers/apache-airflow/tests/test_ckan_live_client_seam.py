"""Seam contracts for the provider hook composing real dual-surface CKAN live clients.

Runs inside the wheel-only candidate venv built by ``run_candidate.py`` so every
assertion reflects the installed-wheel experience Airflow will encounter.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from airflow.providers.datasluice.hooks.datasluice import (
    _TOKEN_FIELDS,
    DatasluiceHook,
    _ckan_sync_client,
)

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest, SyncCatalogClient
from datasluice.domain.catalog.auth import CKANCredential, CredentialResolver
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.operations import OperationId
from datasluice.runtime.transport.base import RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
SEAM_TOKEN = "loopback-token"

_RESOURCE_SEARCH_BODY = json.dumps(
    {
        "success": True,
        "result": {
            "count": 1,
            "results": [
                {
                    "id": "res-seam-1",
                    "package_id": "ds-seam-1",
                    "name": "Seam People",
                    "url": "https://catalog.example.gov/people.csv",
                }
            ],
        },
    }
).encode("utf-8")


class Connection:
    """Airflow connection double carrying mapping-valued extras."""

    def __init__(self, extra_dejson: dict[str, object], password: str | None = None) -> None:
        self.extra_dejson = extra_dejson
        self.password = password


class _CkanCaptureTransport:
    """A loopback-safe borrowed transport recording every dispatched Action API request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[Any] = []
        self.close_count = 0

    def send(self, request: Any) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    def close(self) -> None:
        self.close_count += 1


def _hook_with(monkeypatch: pytest.MonkeyPatch, connection: Connection) -> DatasluiceHook:
    monkeypatch.setattr(DatasluiceHook, "get_connection", lambda self, _: connection)
    return DatasluiceHook(airflow_conn_id="seam")


def _resources_list_query() -> CatalogOperationRequest:
    return CatalogOperationRequest(operation_id=OperationId(platform="ckan", service="resources", method="list"))


def test_ckan_connection_composes_a_real_live_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """A platform=ckan connection yields one real dual-surface live client with the mapped credential."""
    hook = _hook_with(
        monkeypatch,
        Connection({"platform": "ckan", "base_url": LOOPBACK_ORIGIN, "api_token": SEAM_TOKEN}),
    )

    client = hook.get_conn()

    assert isinstance(client, SyncCatalogClient)
    assert isinstance(client.credentials, CKANCredential)
    assert client.platform_metadata()["platform"] == "ckan"


def test_hook_memoizes_and_closes_its_owned_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated access returns one client and hook cleanup closes its owned transport once."""

    class _ClientSpy:
        def __init__(self) -> None:
            self.close_count = 0

        def close(self) -> None:
            self.close_count += 1

    client = _ClientSpy()
    extras: dict[str, object] = {"platform": "ckan", "base_url": LOOPBACK_ORIGIN, "api_token": SEAM_TOKEN}
    connection = Connection(extras)
    hook = _hook_with(monkeypatch, connection)
    monkeypatch.setattr(
        "airflow.providers.datasluice.hooks.datasluice._ckan_sync_client",
        lambda connection, extras: client,
    )

    first = hook.get_conn()
    second = hook.get_conn()
    hook.close()
    hook.close()

    assert first is second
    assert client.close_count == 1


def test_typed_read_rides_the_mapped_api_token_over_the_borrowed_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    """One resources.list read dispatches through the injected borrowed transport with the mapped token."""
    capture = _CkanCaptureTransport(body=_RESOURCE_SEARCH_BODY)
    extras: dict[str, object] = {"platform": "ckan", "base_url": LOOPBACK_ORIGIN, "api_token": SEAM_TOKEN}
    connection = Connection(extras)
    client = _ckan_sync_client(connection, extras, sync_transport=capture)

    query = _resources_list_query()
    envelope = client.resources.list(query, CatalogOperationGuard(operation_id=query.operation_id))

    assert [record.name for record in envelope.items] == ["Seam People"]
    assert len(capture.requests) == 1
    sent = capture.requests[0]
    assert sent.url == f"{LOOPBACK_ORIGIN}/api/3/action/resource_search"
    assert sent.headers.get("Authorization") == SEAM_TOKEN
    assert SEAM_TOKEN.encode("utf-8") not in (sent.body or b"")
    assert capture.close_count == 0


def test_missing_base_url_raises_an_actionable_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Absent origin extras fail fast naming the missing key and an example shape."""
    hook = _hook_with(monkeypatch, Connection({"platform": "ckan", "api_token": SEAM_TOKEN}))

    with pytest.raises(ValueError) as excinfo:
        hook.get_conn()

    message = str(excinfo.value)
    assert "'base_url'" in message
    assert '"platform": "ckan"' in message
    assert "base_url" in message


def test_ckan_token_field_mapping_is_locked() -> None:
    """The platform token-field table maps CKAN connections to the api_token field."""
    assert _TOKEN_FIELDS[CatalogPlatform.CKAN] == "api_token"


def test_non_ckan_platforms_keep_the_deferred_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """udata connections still build the deferred facade client instead of a live client."""
    hook = _hook_with(monkeypatch, Connection({"platform": "udata", "api_key": "loopback-key"}))

    client = hook.get_conn()

    assert isinstance(client, SyncCatalogClient)
    assert isinstance(client.credentials, CredentialResolver)
    assert client.platform_metadata()["platform"] == "udata"
