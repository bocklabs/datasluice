"""Complete dual-mode uData root-profile service over guarded dispatch."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Never, cast

from datasluice.connectors.catalog.udata.models.root_profile import (
    ROOT_OPERATION,
    SITE_RESOURCE_KIND,
    SiteCatalogQuery,
    SiteDocument,
    SiteMutationResult,
    SitePatchInput,
    SiteProfile,
)
from datasluice.connectors.catalog.udata.wire import root_profile as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential, credential_scope
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, IdempotencyPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
    attach_catalog_metadata,
)
from datasluice.runtime.mutation import build_mutation_receipt
from datasluice.runtime.transport.base import TransportFailure

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"
_SITE_TARGET = "site"
_CSV_MEDIA_TYPE = "text/csv"


def _operation_id(operation: str) -> OperationId:
    platform, _, tail = operation.partition("/")
    service, _, method = tail.partition(".")
    return OperationId(platform=platform, service=service or "native", method=method or tail)


def _require_mutation_permission(
    resolved: object,
    operation: str,
    permissions: EffectivePermissions | None,
) -> UDataCredential:
    if not isinstance(resolved, UDataCredential):
        raise UnauthenticatedError(
            "uData site mutations require an explicitly resolved API credential.",
            operation=operation,
            platform="udata",
            capability_state="unauthorized",
            safe_action="Construct the client with UDataCredential or a resolver yielding one.",
        )
    if permissions is None:
        raise ForbiddenError(
            "uData site mutations require explicit effective-permission evidence.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass EffectivePermissions.for_credential for the resolved credential identity.",
        )
    if permissions.platform != CatalogPlatform.UDATA:
        raise ForbiddenError(
            "uData site permission evidence must declare the uData platform.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Pass EffectivePermissions for the uData platform.",
        )
    if not permissions.authenticated or permissions.credential_scope != credential_scope(resolved):
        raise ForbiddenError(
            "uData site permission evidence does not match the resolved credential identity.",
            operation=operation,
            platform="udata",
            capability_state="forbidden",
            safe_action="Build permission evidence with EffectivePermissions.for_credential using this credential.",
        )
    permissions.require(operation, roles={"admin"})
    return resolved


def _receipt_policy(policy: MutationPolicy | None) -> MutationPolicy:
    if policy is None:
        return MutationPolicy(concurrency=ConcurrencyPolicy(overwrite=True))
    return MutationPolicy(
        destructive=policy.destructive,
        concurrency=ConcurrencyPolicy(overwrite=True),
        idempotency=IdempotencyPolicy(
            safe=policy.idempotency.safe,
            explicit_retry_opt_in=policy.idempotency.explicit_retry_opt_in,
        ),
        dry_run=policy.dry_run,
    )


def _policy_metadata(policy: MutationPolicy | None) -> dict[str, object]:
    if policy is None:
        return {"provided": False}
    confirmation = policy.confirmation
    concurrency = policy.concurrency
    return {
        "provided": True,
        "destructive": policy.destructive,
        "confirmed": confirmation is not None and confirmation.confirmed,
        "overwrite": concurrency is not None and concurrency.overwrite,
        "concurrency_provided": concurrency is not None,
        "retry_safe": policy.idempotency.safe,
        "retry_opt_in": policy.idempotency.explicit_retry_opt_in,
        "idempotency_provided": policy.idempotency.key is not None,
        "dry_run": policy.dry_run.requested,
    }


def _build_receipt(policy: MutationPolicy | None, outcome: str, *, status_code: int, mutation: str) -> MutationReceipt:
    return build_mutation_receipt(
        _operation_id(wire.ROOT_OPERATIONS["set_site"]),
        CatalogId(platform=CatalogPlatform.UDATA, resource_kind=SITE_RESOURCE_KIND, value=_SITE_TARGET),
        _receipt_policy(policy),
        outcome,
        {"mutation": mutation, "status_code": status_code, "policy": _policy_metadata(policy)},
    )


def _error_status(error: BaseException, response: object | None = None) -> int:
    status = getattr(error, "status_code", None)
    if isinstance(status, int):
        return status
    metadata = getattr(error, "metadata", None)
    if isinstance(metadata, Mapping) and isinstance(metadata.get("status_code"), int):
        return cast(int, metadata["status_code"])
    response_status = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else 0


def _mutation_outcome(error: BaseException, response: object | None = None) -> str:
    if error.__class__.__name__ == "CancelledError":
        return "cancelled"
    metadata = getattr(error, "metadata", None)
    if isinstance(metadata, Mapping) and metadata.get("ambiguous") is True:
        return "ambiguous"
    response_status = getattr(response, "status_code", None)
    if isinstance(response_status, int) and 200 <= response_status < 300:
        return "ambiguous"
    if isinstance(error, TransportFailure):
        return "ambiguous"
    if _error_status(error, response) == 0 and isinstance(
        error, (UnauthenticatedError, ForbiddenError, CatalogValidationError)
    ):
        return "rejected"
    return "failed"


def _attach_receipt(error: BaseException, receipt: MutationReceipt) -> BaseException:
    attach_catalog_metadata(error, {"receipt": receipt.to_dict()})
    error_dict = getattr(error, "__dict__", None)
    if isinstance(error_dict, dict):
        error_dict["mutation_receipt"] = receipt
    return error


def _raise_with_receipt(error: BaseException, receipt: MutationReceipt) -> Never:
    raise _attach_receipt(error, receipt)


def _require_controlled_origin(origin: str, operation: str) -> None:
    if origin != _CONTROLLED_ORIGIN:
        raise CatalogValidationError(
            "uData site PATCH is restricted to the seeded loopback evidence stack.",
            operation=operation,
            platform="udata",
            safe_action=f"Use the controlled deployment origin {_CONTROLLED_ORIGIN} for site mutations.",
        )


def _enforce_patch_policy(policy: MutationPolicy | None) -> None:
    operation = wire.ROOT_OPERATIONS["set_site"]

    def reject(message: str, action: str) -> Never:
        raise ForbiddenError(
            message, operation=operation, platform="udata", capability_state="forbidden", safe_action=action
        )

    if not isinstance(policy, MutationPolicy):
        reject(
            "uData site PATCH requires an explicit MutationPolicy.",
            "Pass a confirmed MutationPolicy with ConcurrencyPolicy(overwrite=True).",
        )
    confirmation = policy.confirmation
    if confirmation is None or not confirmation.confirmed:
        reject(
            "The uData site PATCH is not explicitly confirmed.",
            "Pass ConfirmationPolicy(confirmed=True, operation=..., target='site').",
        )
    if confirmation.operation != operation or confirmation.target != _SITE_TARGET:
        reject(
            "uData site confirmation must be bound to the set_site operation and singleton site target.",
            "Pass ConfirmationPolicy(confirmed=True, operation='udata/api-v1.set-site', target='site').",
        )
    if policy.destructive:
        reject(
            "The uData site PATCH cannot use the destructive policy tier.",
            "Pass destructive=False for site settings updates.",
        )
    if policy.dry_run.requested:
        reject("The stock uData site PATCH does not support dry-run previews.", "Retry without requesting dry-run.")
    if policy.concurrency is None or not policy.concurrency.overwrite:
        reject(
            "The stock uData site PATCH has no conditional version token and requires explicit overwrite intent.",
            "Pass ConcurrencyPolicy(overwrite=True).",
        )
    if policy.concurrency.token is not None:
        reject(
            "The stock uData site PATCH has no conditional concurrency token support.",
            "Pass ConcurrencyPolicy(overwrite=True) without a token.",
        )
    if policy.idempotency.allows_retry():
        reject(
            "The uData site PATCH is not proven safe to retry.",
            "Leave IdempotencyPolicy at its non-retrying default.",
        )


def _mutating(
    policy: MutationPolicy | None,
    dispatch: Callable[[], tuple[int, object, object]],
    decode: Callable[[object], SiteProfile | None],
) -> SiteMutationResult:
    response: object | None = None
    try:
        status, payload, response = dispatch()
        profile = decode(payload)
    except BaseException as error:
        receipt = _build_receipt(
            policy,
            _mutation_outcome(error, response),
            status_code=_error_status(error, response),
            mutation="set_site",
        )
        _raise_with_receipt(error, receipt)
    receipt = _build_receipt(policy, "succeeded", status_code=status, mutation="set_site")
    return SiteMutationResult(receipt=receipt, profile=profile)


async def _amutating(
    policy: MutationPolicy | None,
    dispatch: Callable[[], Awaitable[tuple[int, object, object]]],
    decode: Callable[[object], SiteProfile | None],
) -> SiteMutationResult:
    response: object | None = None
    try:
        status, payload, response = await dispatch()
        profile = decode(payload)
    except BaseException as error:
        receipt = _build_receipt(
            policy,
            _mutation_outcome(error, response),
            status_code=_error_status(error, response),
            mutation="set_site",
        )
        _raise_with_receipt(error, receipt)
    receipt = _build_receipt(policy, "succeeded", status_code=status, mutation="set_site")
    return SiteMutationResult(receipt=receipt, profile=profile)


def _decode_profile(payload: object) -> SiteProfile:
    return wire.parse_site_profile(payload, operation=ROOT_OPERATION)


def _decode_patch(payload: object) -> SiteProfile | None:
    return None if payload is None else _decode_profile(payload)


def _parse_redirect(
    status: int,
    value: object,
    endpoint: str,
    origin: str,
    expected_path: str | None = None,
) -> SiteDocument:
    return wire.parse_redirect(
        status_code=status,
        headers=cast(Mapping[str, str], value),
        endpoint=endpoint,
        origin=origin,
        expected_path=expected_path,
        operation=ROOT_OPERATION,
    )


def _content_type(headers: Mapping[str, str]) -> str | None:
    return next((value for key, value in headers.items() if key.lower() == "content-type"), None)


class SyncRootProfileService:
    """Typed synchronous service for all assigned root-profile rows."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native error type used by root-profile decoding."""
        return NativeCatalogError

    def get(self) -> SiteProfile:
        """GET /api/1/site/ (row 183)."""
        method, path, headers, _ = wire.get_site_request()
        _, payload, _ = self._client._root_call(
            method=method, path=path, owning_operation=ROOT_OPERATION, headers=headers
        )
        return _decode_profile(payload)

    def set_site(
        self,
        client_input: SitePatchInput,
        *,
        permissions: EffectivePermissions | None,
        mutation_policy: MutationPolicy | None = None,
    ) -> SiteMutationResult:
        """PATCH /api/1/site/ (row 184) on the controlled stack only."""
        operation = wire.ROOT_OPERATIONS["set_site"]

        def dispatch() -> tuple[int, object, object]:
            _require_controlled_origin(self._client._origin, operation)
            if not isinstance(client_input, SitePatchInput):
                raise CatalogValidationError(
                    "uData site PATCH requires SitePatchInput.",
                    operation=operation,
                    platform="udata",
                    safe_action="Pass a typed SitePatchInput.",
                )
            resolved = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
            _enforce_patch_policy(mutation_policy)
            method, path, headers, body = wire.set_site_request(client_input)
            return self._client._root_call(
                method=method,
                path=path,
                owning_operation=ROOT_OPERATION,
                headers=headers,
                json_body=body,
                credential=resolved,
                idempotency_policy=IdempotencyPolicy(),
            )

        return _mutating(mutation_policy, dispatch, _decode_patch)

    def data_portal(self, fmt: str) -> SiteDocument:
        """GET /api/1/site/data.<format> (row 185)."""
        method, path, headers, _ = wire.data_portal_request(fmt)
        status, value, _ = self._client._root_call(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        expected = f"/api/1/site/catalog.{fmt.lower()}"
        return _parse_redirect(status, value, path, self._client._origin, expected)

    def rdf_catalog(self, query: SiteCatalogQuery | None = None, *, accept: str | None = None) -> SiteDocument:
        """GET /api/1/site/catalog (row 186)."""
        method, path, headers, _ = wire.rdf_catalog_request(query, accept=accept)
        status, value, _ = self._client._root_call(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        return _parse_redirect(status, value, path, self._client._origin)

    def rdf_catalog_format(self, fmt: str, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/catalog.<format> (row 187)."""
        method, path, headers, _ = wire.rdf_catalog_format_request(fmt, query or SiteCatalogQuery())
        status, value, response = self._client._root_call(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        if status in {301, 302, 303, 307, 308}:
            return _parse_redirect(status, value, path, self._client._origin)
        return wire.parse_document(
            cast(bytes, value),
            endpoint=path,
            expected_media_type=wire.media_type_for_format(fmt),
            response_media_type=_content_type(response.headers),
            status_code=status,
            fmt=fmt,
            operation=ROOT_OPERATION,
        )

    def _csv(self, name: str, query: SiteCatalogQuery | None = None) -> SiteDocument:
        method, path, headers, _ = wire.csv_request(name, query)
        status, value, response = self._client._root_call(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        if status in {301, 302, 303, 307, 308}:
            return _parse_redirect(status, value, path, self._client._origin)
        return wire.parse_document(
            cast(bytes, value),
            endpoint=path,
            expected_media_type=_CSV_MEDIA_TYPE,
            response_media_type=_content_type(response.headers),
            status_code=status,
            operation=ROOT_OPERATION,
        )

    def datasets_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/datasets.csv (row 188)."""
        return self._csv("datasets", query)

    def resources_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/resources.csv (row 189)."""
        return self._csv("resources", query)

    def organizations_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/organizations.csv (row 190)."""
        return self._csv("organizations", query)

    def reuses_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/reuses.csv (row 191)."""
        return self._csv("reuses", query)

    def dataservices_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/dataservices.csv (row 192)."""
        return self._csv("dataservices", query)

    def harvests_csv(self) -> SiteDocument:
        """GET /api/1/site/harvests.csv (row 193)."""
        return self._csv("harvests")

    def tags_csv(self) -> SiteDocument:
        """GET /api/1/site/tags.csv (row 194)."""
        return self._csv("tags")

    def jsonld_context(self) -> SiteDocument:
        """GET /api/1/site/context.jsonld (row 195)."""
        method, path, headers, _ = wire.jsonld_context_request()
        status, body, response = self._client._root_call(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
        )
        return wire.parse_jsonld_context(
            cast(bytes, body),
            endpoint=path,
            response_media_type=_content_type(response.headers),
            status_code=status,
            operation=ROOT_OPERATION,
        )

    site_data_portal = data_portal
    site_rdf_catalog = rdf_catalog
    site_rdf_catalog_format = rdf_catalog_format
    site_datasets_csv = datasets_csv
    site_resources_csv = resources_csv
    site_organizations_csv = organizations_csv
    site_reuses_csv = reuses_csv
    site_dataservices_csv = dataservices_csv
    site_harvests_csv = harvests_csv
    site_tags_csv = tags_csv
    site_jsonld_context = jsonld_context
    site_json_ld_context = jsonld_context
    get_site = get
    catalog = rdf_catalog
    catalog_format = rdf_catalog_format
    context = jsonld_context


class AsyncRootProfileService:
    """Typed asynchronous service mirroring every root-profile operation."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native error type used by root-profile decoding."""
        return NativeCatalogError

    async def get(self) -> SiteProfile:
        """GET /api/1/site/ (row 183)."""
        method, path, headers, _ = wire.get_site_request()
        _, payload, _ = await self._client._root_call_async(
            method=method, path=path, owning_operation=ROOT_OPERATION, headers=headers
        )
        return _decode_profile(payload)

    async def set_site(
        self,
        client_input: SitePatchInput,
        *,
        permissions: EffectivePermissions | None,
        mutation_policy: MutationPolicy | None = None,
    ) -> SiteMutationResult:
        """PATCH /api/1/site/ (row 184) on the controlled stack only."""
        operation = wire.ROOT_OPERATIONS["set_site"]

        async def dispatch() -> tuple[int, object, object]:
            _require_controlled_origin(self._client._origin, operation)
            if not isinstance(client_input, SitePatchInput):
                raise CatalogValidationError(
                    "uData site PATCH requires SitePatchInput.",
                    operation=operation,
                    platform="udata",
                    safe_action="Pass a typed SitePatchInput.",
                )
            resolved = await self._client._resolved_credential_async()
            _require_mutation_permission(resolved, operation, permissions)
            _enforce_patch_policy(mutation_policy)
            method, path, headers, body = wire.set_site_request(client_input)
            return await self._client._root_call_async(
                method=method,
                path=path,
                owning_operation=ROOT_OPERATION,
                headers=headers,
                json_body=body,
                credential=resolved,
                idempotency_policy=IdempotencyPolicy(),
            )

        return await _amutating(mutation_policy, dispatch, _decode_patch)

    async def data_portal(self, fmt: str) -> SiteDocument:
        """GET /api/1/site/data.<format> (row 185)."""
        method, path, headers, _ = wire.data_portal_request(fmt)
        status, value, _ = await self._client._root_call_async(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        return _parse_redirect(status, value, path, self._client._origin, f"/api/1/site/catalog.{fmt.lower()}")

    async def rdf_catalog(self, query: SiteCatalogQuery | None = None, *, accept: str | None = None) -> SiteDocument:
        """GET /api/1/site/catalog (row 186)."""
        method, path, headers, _ = wire.rdf_catalog_request(query, accept=accept)
        status, value, _ = await self._client._root_call_async(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        return _parse_redirect(status, value, path, self._client._origin)

    async def rdf_catalog_format(self, fmt: str, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/catalog.<format> (row 187)."""
        method, path, headers, _ = wire.rdf_catalog_format_request(fmt, query or SiteCatalogQuery())
        status, value, response = await self._client._root_call_async(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        if status in {301, 302, 303, 307, 308}:
            return _parse_redirect(status, value, path, self._client._origin)
        return wire.parse_document(
            cast(bytes, value),
            endpoint=path,
            expected_media_type=wire.media_type_for_format(fmt),
            response_media_type=_content_type(response.headers),
            status_code=status,
            fmt=fmt,
            operation=ROOT_OPERATION,
        )

    async def _csv(self, name: str, query: SiteCatalogQuery | None = None) -> SiteDocument:
        method, path, headers, _ = wire.csv_request(name, query)
        status, value, response = await self._client._root_call_async(
            method=method,
            path=path,
            owning_operation=ROOT_OPERATION,
            headers=headers,
            raw_text=True,
            redirect_mode=True,
        )
        if status in {301, 302, 303, 307, 308}:
            return _parse_redirect(status, value, path, self._client._origin)
        return wire.parse_document(
            cast(bytes, value),
            endpoint=path,
            expected_media_type=_CSV_MEDIA_TYPE,
            response_media_type=_content_type(response.headers),
            status_code=status,
            operation=ROOT_OPERATION,
        )

    async def datasets_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/datasets.csv (row 188)."""
        return await self._csv("datasets", query)

    async def resources_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/resources.csv (row 189)."""
        return await self._csv("resources", query)

    async def organizations_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/organizations.csv (row 190)."""
        return await self._csv("organizations", query)

    async def reuses_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/reuses.csv (row 191)."""
        return await self._csv("reuses", query)

    async def dataservices_csv(self, query: SiteCatalogQuery | None = None) -> SiteDocument:
        """GET /api/1/site/dataservices.csv (row 192)."""
        return await self._csv("dataservices", query)

    async def harvests_csv(self) -> SiteDocument:
        """GET /api/1/site/harvests.csv (row 193)."""
        return await self._csv("harvests")

    async def tags_csv(self) -> SiteDocument:
        """GET /api/1/site/tags.csv (row 194)."""
        return await self._csv("tags")

    async def jsonld_context(self) -> SiteDocument:
        """GET /api/1/site/context.jsonld (row 195)."""
        method, path, headers, _ = wire.jsonld_context_request()
        status, body, response = await self._client._root_call_async(
            method=method, path=path, owning_operation=ROOT_OPERATION, headers=headers, raw_text=True
        )
        return wire.parse_jsonld_context(
            cast(bytes, body),
            endpoint=path,
            response_media_type=_content_type(response.headers),
            status_code=status,
            operation=ROOT_OPERATION,
        )

    site_data_portal = data_portal
    site_rdf_catalog = rdf_catalog
    site_rdf_catalog_format = rdf_catalog_format
    site_datasets_csv = datasets_csv
    site_resources_csv = resources_csv
    site_organizations_csv = organizations_csv
    site_reuses_csv = reuses_csv
    site_dataservices_csv = dataservices_csv
    site_harvests_csv = harvests_csv
    site_tags_csv = tags_csv
    site_jsonld_context = jsonld_context
    site_json_ld_context = jsonld_context
    get_site = get
    catalog = rdf_catalog
    catalog_format = rdf_catalog_format
    context = jsonld_context


__all__ = ["AsyncRootProfileService", "SyncRootProfileService"]
