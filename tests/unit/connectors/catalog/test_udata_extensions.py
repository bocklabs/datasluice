"""Exact-wire, failure, and harmless capability-probe evidence for the uData extension family."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata.models.extensions import AvatarRequest, ExtensionEvidence, TagSuggestQuery
from datasluice.connectors.catalog.udata.services.extensions import (
    AsyncExtensionsService,
    ExtensionEvidenceCache,
    SyncExtensionsService,
)
from datasluice.connectors.catalog.udata.wire import extensions as wire
from datasluice.domain.catalog.auth import UDataCredential
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError
from tests.helpers.udata_test_support import (
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    AsyncRouteRouter,
    RouteTable,
    SyncRouteRouter,
    async_client,
    json_media,
    sync_client,
    with_site_route,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.runtime.transport.base import RuntimeRequest

_PNG_BODY = b"\x89PNG\r\n\x1a\navatarbody"
_REASON_CATEGORIES_URL = f"{UDATA_ORIGIN}/api/1/access_type/reason_categories/"
_SUGGEST_TAGS_URL = f"{UDATA_ORIGIN}/api/1/tags/suggest/"
_AVATAR_URL = f"{UDATA_ORIGIN}/api/1/avatars/"
_CAPTCHETAT_URL = f"{UDATA_ORIGIN}/api/2/captchetat/"
_PROBE_SUGGEST_TAGS_URL = f"{UDATA_ORIGIN}/api/1/tags/suggest/?q=a&size=1"
_PROBE_AVATAR_URL = f"{UDATA_ORIGIN}/api/1/avatars/udata/1/"


def _image_media(payload: object) -> str:
    return "image/png" if isinstance(payload, bytes) else json_media(payload)


def _url(path: str) -> str:
    return f"{UDATA_ORIGIN}{path}"


def _sync_router(routes: RouteTable) -> SyncRouteRouter:
    return SyncRouteRouter(with_site_route(routes), _image_media)


def _async_router(routes: RouteTable) -> AsyncRouteRouter:
    return AsyncRouteRouter(with_site_route(routes), _image_media)


def _sync_service(
    router: SyncRouteRouter, credentials: object | None = UDATA_CREDENTIAL, cache: ExtensionEvidenceCache | None = None
) -> SyncExtensionsService:
    return SyncExtensionsService(sync_client(router, credentials), cache)


def _async_service(
    router: AsyncRouteRouter, credentials: object | None = UDATA_CREDENTIAL, cache: ExtensionEvidenceCache | None = None
) -> AsyncExtensionsService:
    return AsyncExtensionsService(async_client(router, credentials), cache)


def _probe_routes(avatar_status: int = 200) -> RouteTable:
    return {
        ("GET", _REASON_CATEGORIES_URL): (200, [{"id": "reason-1", "reason": "Restricted"}]),
        ("GET", _PROBE_SUGGEST_TAGS_URL): (200, [{"id": "env", "count": 2}]),
        ("GET", _PROBE_AVATAR_URL): (avatar_status, _PNG_BODY if avatar_status == 200 else {"message": "absent"}),
        ("GET", _CAPTCHETAT_URL): (200, {"active": False}),
    }


def _counted(router: SyncRouteRouter | AsyncRouteRouter, suffix: str) -> list[RuntimeRequest]:
    return [request for request in router.requests if request.url.endswith(suffix)]


def _without_site(router: SyncRouteRouter | AsyncRouteRouter) -> list[RuntimeRequest]:
    return [request for request in router.requests if not request.url.endswith("/api/1/site/")]


def test_extension_request_builders_pin_the_exact_wire() -> None:
    assert wire.reason_categories_request() == ("GET", "/api/1/access_type/reason_categories/", {}, None)
    assert wire.suggest_tags_request(TagSuggestQuery(q="env", size=5)) == (
        "GET",
        "/api/1/tags/suggest/?q=env&size=5",
        {},
        None,
    )
    assert wire.suggest_tags_request(TagSuggestQuery(q="env")) == (
        "GET",
        "/api/1/tags/suggest/?q=env&size=10",
        {},
        None,
    )
    assert wire.avatar_request(AvatarRequest(identifier="org-1", size=32)) == (
        "GET",
        "/api/1/avatars/org-1/32/",
        {},
        None,
    )
    assert wire.captchetat_request() == ("GET", "/api/2/captchetat/", {}, None)


def test_avatar_identifier_is_encoded_exactly_once() -> None:
    method, path, _, _ = wire.avatar_request(AvatarRequest(identifier="org 1", size=8))

    assert method == "GET"
    assert path == "/api/1/avatars/org%201/8/"


@pytest.mark.parametrize("identifier", [".", "..", "a/b", 'a"b'])
def test_avatar_identifier_rejects_unsafe_segments(identifier: str) -> None:
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.avatar_request(AvatarRequest(identifier=identifier, size=8))


@pytest.mark.parametrize(("q", "size"), [("", 5), ("env", 0), ("env", 101), ("env", True)])
def test_tag_suggest_query_rejects_undocumented_values(q: str, size: int) -> None:
    with pytest.raises(ValueError):
        TagSuggestQuery(q=q, size=size)


@pytest.mark.parametrize("size", [0, 1025, "32"])
def test_avatar_request_rejects_undocumented_sizes(size: object) -> None:
    with pytest.raises(ValueError):
        AvatarRequest(identifier="org-1", size=cast("int", size))


def test_require_extension_operation_rejects_unknown_operations() -> None:
    with pytest.raises(CatalogValidationError, match="not a documented stock extension operation"):
        wire.require_extension_operation("udata/api-v1.datasets")

    assert wire.require_extension_operation(wire.AVATAR_OPERATION) == wire.AVATAR_OPERATION


@pytest.mark.parametrize(
    ("parser", "payload"),
    [
        (wire.parse_reason_categories, {"id": "reason-1"}),
        (wire.parse_tag_suggestions, {"id": "env"}),
        (wire.parse_tag_suggestions, [1, 2]),
        (wire.parse_captchetat, ["active"]),
        (wire.parse_captchetat, {"active": "yes"}),
    ],
)
def test_extension_decoders_reject_undocumented_shapes(
    parser: Callable[[object, str], object], payload: object
) -> None:
    with pytest.raises(CatalogValidationError):
        parser(payload, wire.EXTENSIONS_OPERATION)


def test_parse_avatar_retains_only_bounded_image_metadata() -> None:
    record = wire.parse_avatar(
        _PNG_BODY, response_media_type="image/png; charset=binary", operation=wire.AVATAR_OPERATION
    )

    assert record.payload["media_type"] == "image/png"
    assert record.payload["size_bytes"] == len(_PNG_BODY)
    assert record.payload["sha256"]
    assert "body" not in record.payload
    assert _PNG_BODY not in json.dumps(record.to_dict()).encode()


@pytest.mark.parametrize("media_type", ["text/html", "application/octet-stream", None])
def test_parse_avatar_rejects_undocumented_media_types(media_type: str | None) -> None:
    with pytest.raises(NativeCatalogError):
        wire.parse_avatar(_PNG_BODY, response_media_type=media_type, operation=wire.AVATAR_OPERATION)


def test_parse_avatar_rejects_unbuffered_bodies() -> None:
    with pytest.raises(NativeCatalogError):
        wire.parse_avatar("not-bytes", response_media_type="image/png", operation=wire.AVATAR_OPERATION)


def test_extension_evidence_retains_only_the_allowlisted_fields() -> None:
    evidence = ExtensionEvidence(
        operation_id=wire.REASON_CATEGORIES_OPERATION,
        state="available",
        response_class="success",
        credential_scope="udatacredential-0123456789abcdef",
    )

    assert evidence.available is True
    assert evidence.to_dict() == {
        "operation_id": wire.REASON_CATEGORIES_OPERATION,
        "state": "available",
        "response_class": "success",
        "credential_scope": "udatacredential-0123456789abcdef",
    }


@pytest.mark.parametrize(
    ("state", "response_class"),
    [("available", "success"), ("unsupported", "unsupported"), ("unavailable", "unavailable")],
)
def test_extension_evidence_accepts_the_closed_vocabulary(state: str, response_class: str) -> None:
    evidence = ExtensionEvidence(
        operation_id=wire.CAPTCHETAT_OPERATION,
        state=state,
        response_class=response_class,
        credential_scope="anonymous",
    )

    assert evidence.to_dict()["state"] == state


@pytest.mark.parametrize(
    ("state", "response_class"),
    [("available", "not-a-class"), ("assumed", "success"), ("", "success")],
)
def test_extension_evidence_rejects_unknown_states(state: str, response_class: str) -> None:
    with pytest.raises(ValueError):
        ExtensionEvidence(
            operation_id=wire.CAPTCHETAT_OPERATION,
            state=state,
            response_class=response_class,
            credential_scope="anonymous",
        )


def test_row1_reason_categories_exact_wire() -> None:
    router = _sync_router({("GET", _REASON_CATEGORIES_URL): (200, [{"id": "reason-1", "reason": "Restricted"}])})
    service = _sync_service(router)

    records = service.reason_categories()

    assert [record.payload["id"] for record in records] == ["reason-1"]
    assert _counted(router, "/access_type/reason_categories/")[0].method == "GET"


def test_row196_suggest_tags_exact_wire() -> None:
    router = _sync_router({("GET", f"{_SUGGEST_TAGS_URL}?q=env&size=5"): (200, [{"id": "env", "count": 2}])})
    service = _sync_service(router)

    records = service.suggest_tags(TagSuggestQuery(q="env", size=5))

    assert [record.payload["count"] for record in records] == [2]
    assert router.requests[-1].url == f"{_SUGGEST_TAGS_URL}?q=env&size=5"


def test_row232_avatar_returns_bounded_image_metadata() -> None:
    router = _sync_router({("GET", f"{_AVATAR_URL}org-1/32/"): (200, _PNG_BODY)})
    service = _sync_service(router)

    record = service.avatar(AvatarRequest(identifier="org-1", size=32))

    assert record.payload["media_type"] == "image/png"
    assert record.payload["size_bytes"] == len(_PNG_BODY)
    request = router.requests[-1]
    assert request.url == f"{_AVATAR_URL}org-1/32/"
    assert request.max_response_bytes == wire.AVATAR_MAX_BYTES
    assert _PNG_BODY not in json.dumps(record.to_dict()).encode()


def test_row262_captchetat_exact_wire() -> None:
    router = _sync_router({("GET", _CAPTCHETAT_URL): (200, {"active": True})})
    service = _sync_service(router)

    record = service.captchetat()

    assert record.payload["active"] is True
    assert router.requests[-1].url == _CAPTCHETAT_URL


def test_extension_service_matches_between_modes() -> None:
    routes = _probe_routes()
    sync_router = _sync_router(routes)
    async_router = _async_router(routes)

    with sync_client(sync_router, UDATA_CREDENTIAL) as client:
        sync_service = SyncExtensionsService(client)
        sync_results = [
            [record.to_dict() for record in sync_service.reason_categories()],
            [record.to_dict() for record in sync_service.suggest_tags(wire.probe_tag_suggest_query())],
            sync_service.avatar(wire.probe_avatar_request()).to_dict(),
            sync_service.captchetat().to_dict(),
        ]

    async def run() -> list[list[object] | dict[str, object]]:
        async with async_client(async_router, UDATA_CREDENTIAL) as client:
            async_service = AsyncExtensionsService(client)
            return [
                [record.to_dict() for record in await async_service.reason_categories()],
                [record.to_dict() for record in await async_service.suggest_tags(wire.probe_tag_suggest_query())],
                (await async_service.avatar(wire.probe_avatar_request())).to_dict(),
                (await async_service.captchetat()).to_dict(),
            ]

    async_results = asyncio.run(run())

    sync_urls = [request.url for request in _without_site(sync_router)]
    async_urls = [request.url for request in _without_site(async_router)]
    assert sync_urls == async_urls
    assert sync_results == async_results


def test_capability_proof_dispatches_only_harmless_reads() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)

    for operation in sorted(wire.EXTENSION_OPERATIONS):
        evidence = service.capability(operation)
        assert evidence.available is True, operation

    for request in _without_site(router):
        assert request.method == "GET"
        assert request.body is None or request.body == b""


def test_capability_evidence_is_cached_within_the_ttl() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)
    operation = wire.REASON_CATEGORIES_OPERATION

    first = service.capability(operation)
    second = service.capability(operation)

    assert first == second
    assert len(_counted(router, "/access_type/reason_categories/")) == 1


def test_capability_evidence_expires_after_the_ttl() -> None:
    router = _sync_router(_probe_routes())
    now = {"value": 100.0}
    cache = ExtensionEvidenceCache(ttl_seconds=10.0, clock=lambda: now["value"])
    service = _sync_service(router, cache=cache)
    operation = wire.REASON_CATEGORIES_OPERATION

    service.capability(operation)
    now["value"] += 5.0
    service.capability(operation)
    assert len(_counted(router, "/access_type/reason_categories/")) == 1

    now["value"] += 6.0
    service.capability(operation)
    assert len(_counted(router, "/access_type/reason_categories/")) == 2


def test_refresh_invalidates_prior_evidence() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)
    operation = wire.CAPTCHETAT_OPERATION

    service.capability(operation)
    service.refresh()
    assert service.evidence(operation) is None

    service.capability(operation)
    assert len(_counted(router, "/captchetat/")) == 2


def test_a_successful_read_never_proves_another_operation_available() -> None:
    router = _sync_router(
        {
            ("GET", _REASON_CATEGORIES_URL): (200, [{"id": "reason-1"}]),
            ("GET", _PROBE_AVATAR_URL): (404, {"message": "absent"}),
        }
    )
    service = _sync_service(router)

    service.reason_categories()

    assert service.evidence(wire.AVATAR_OPERATION) is None
    assert service.proven_operations() == ()
    evidence = service.capability(wire.AVATAR_OPERATION)

    assert evidence.available is False
    assert evidence.state == "unsupported"
    assert service.proven_operations() == ()
    assert len(_counted(router, "/avatars/udata/1/")) == 1


def test_proven_operations_lists_only_allowlisted_available_operations() -> None:
    router = _sync_router(_probe_routes(avatar_status=404))
    service = _sync_service(router)

    service.capability(wire.REASON_CATEGORIES_OPERATION)
    service.capability(wire.AVATAR_OPERATION)

    assert service.proven_operations() == (wire.REASON_CATEGORIES_OPERATION,)


def test_ambiguous_payload_resolves_unavailable() -> None:
    router = _sync_router({("GET", _CAPTCHETAT_URL): (200, ["not-an-object"])})
    service = _sync_service(router)

    evidence = service.capability(wire.CAPTCHETAT_OPERATION)

    assert evidence.available is False
    assert evidence.state == "unavailable"
    assert evidence.response_class == "unavailable"


@pytest.mark.parametrize(
    ("status", "state"),
    [
        (401, "unauthorized"),
        (403, "forbidden"),
        (423, "deployment-disabled"),
        (500, "unavailable"),
        (429, "unavailable"),
    ],
)
def test_failure_statuses_resolve_fail_closed_states(status: int, state: str) -> None:
    router = _sync_router({("GET", _CAPTCHETAT_URL): (status, {"message": "denied"})})
    service = _sync_service(router)

    evidence = service.capability(wire.CAPTCHETAT_OPERATION)

    assert evidence.available is False
    assert evidence.state == state


def test_evidence_is_scoped_to_the_credential_identity() -> None:
    router = _sync_router(_probe_routes())
    cache = ExtensionEvidenceCache(ttl_seconds=60.0)
    operation = wire.REASON_CATEGORIES_OPERATION
    rotated = UDataCredential(api_key="rotated-key")

    first = _sync_service(router, UDATA_CREDENTIAL, cache)
    second = _sync_service(router, rotated, cache)

    first_evidence = first.capability(operation)
    second_evidence = second.capability(operation)

    assert first_evidence.credential_scope != second_evidence.credential_scope
    assert second_evidence.available is True
    assert first.evidence(operation) == first_evidence
    assert len(_counted(router, "/access_type/reason_categories/")) == 2


def test_sync_proof_is_single_flight() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)
    operation = wire.CAPTCHETAT_OPERATION
    evidence: list[ExtensionEvidence] = []
    barrier = threading.Barrier(8)

    def prove() -> None:
        barrier.wait()
        evidence.append(service.capability(operation))

    threads = [threading.Thread(target=prove) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(evidence) == 8
    assert all(entry == evidence[0] for entry in evidence)
    assert len(_counted(router, "/captchetat/")) == 1


def test_async_proof_is_single_flight() -> None:
    router = _async_router(_probe_routes())
    service = _async_service(router)
    operation = wire.CAPTCHETAT_OPERATION

    async def run() -> list[ExtensionEvidence]:
        return list(await asyncio.gather(*[service.capability(operation) for _ in range(6)]))

    evidence = asyncio.run(run())

    assert len(evidence) == 6
    assert all(entry == evidence[0] for entry in evidence)
    assert len(_counted(router, "/captchetat/")) == 1


def test_async_capability_matches_sync_evidence() -> None:
    routes = _probe_routes(avatar_status=404)
    sync_router = _sync_router(routes)
    async_router = _async_router(routes)
    sync_service = _sync_service(sync_router)

    async def run() -> list[ExtensionEvidence]:
        service = _async_service(async_router)
        return [
            await service.capability(wire.REASON_CATEGORIES_OPERATION),
            await service.capability(wire.AVATAR_OPERATION),
        ]

    async_evidence = asyncio.run(run())
    sync_evidence = [
        sync_service.capability(wire.REASON_CATEGORIES_OPERATION),
        sync_service.capability(wire.AVATAR_OPERATION),
    ]

    assert [entry.to_dict() for entry in async_evidence] == [entry.to_dict() for entry in sync_evidence]
    assert [entry.state for entry in async_evidence] == ["available", "unsupported"]


def test_evidence_never_retains_secrets_or_deployment_details() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)

    evidence = service.capability(wire.SUGGEST_TAGS_OPERATION)
    document = json.dumps(evidence.to_dict())

    assert set(evidence.to_dict()) == {"operation_id", "state", "response_class", "credential_scope"}
    assert "secret-key" not in document
    assert UDATA_ORIGIN not in document
    assert "body" not in document
    assert "config" not in document


def test_capability_rejects_undeclared_operations_before_dispatch() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)

    with pytest.raises(CatalogValidationError):
        service.capability("udata/api-v1.suggest-datasets")

    assert _without_site(router) == []


def test_evidence_lookup_does_not_dispatch() -> None:
    router = _sync_router(_probe_routes())
    service = _sync_service(router)

    assert service.evidence(wire.REASON_CATEGORIES_OPERATION) is None
    assert _without_site(router) == []


def test_extension_evidence_cache_rejects_invalid_ttls() -> None:
    for ttl in (-1.0, float("inf"), float("nan")):
        with pytest.raises(ValueError):
            ExtensionEvidenceCache(ttl_seconds=ttl)
