from __future__ import annotations

import asyncio
import hashlib
import json
import math
import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date
from functools import cached_property, lru_cache, partial
from importlib import resources
from time import monotonic, sleep
from typing import TYPE_CHECKING, ClassVar, Self, TypedDict, Unpack, cast
from urllib.parse import urlsplit

from datasluice.connectors.catalog.udata.compatibility import UDataRouteGate
from datasluice.connectors.catalog.udata.mapping import (
    _DATASETS_OPERATION_ID,
    _PAGED_PATH,
    PLATFORM,
    parse_native_page,
    shape_dataset_page,
    unimplemented_family,
)
from datasluice.connectors.catalog.udata.probes import (
    AsyncSiteVersionGate,
    SiteVersion,
    SiteVersionGate,
)
from datasluice.connectors.catalog.udata.settings import LOOPBACK_HOSTS, UDataClientSettings
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.auth import credential_scope as _credential_scope
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import (
    DeclaredCapabilityProfile,
    EffectiveCapabilityProfile,
    ProbeEvidence,
    ProbeResponseClass,
)
from datasluice.domain.catalog.resilience import CircuitKey, TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.domain.catalog.udata import SET_SITE_OPERATION
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogUnavailableError,
    CatalogValidationError,
    NativeCatalogError,
    UnsupportedCapabilityError,
    attach_catalog_metadata,
    map_catalog_error,
)
from datasluice.runtime.capability import (
    AsyncProbeRunner,
    EffectiveCapabilityCache,
    ProbeRunner,
    build_catalog_operation_guard,
)
from datasluice.runtime.clients import (
    AsyncCatalogTransport,
    _auth_headers,
    _capability_value,
    _circuit_key,
    _refreshed_credential,
    _refreshed_credential_async,
)
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
)
from datasluice.runtime.defaults import create_default_async_transport, create_default_sync_transport
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.extras import require_extra
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    CatalogTransport,
    RedirectPolicy,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    TransportError,
    UploadPart,
)
from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport, HttpxCatalogTransport

if TYPE_CHECKING:
    from types import TracebackType

    from datasluice.connectors.catalog.udata.services.activity_discussions import (
        AsyncActivityDiscussionsService as _AsyncActivityDiscussionsService,
    )
    from datasluice.connectors.catalog.udata.services.activity_discussions import (
        SyncActivityDiscussionsService as _SyncActivityDiscussionsService,
    )
    from datasluice.connectors.catalog.udata.services.auth_oauth import (
        AsyncAuthOAuthService as _AsyncAuthOAuthService,
    )
    from datasluice.connectors.catalog.udata.services.auth_oauth import (
        SyncAuthOAuthService as _SyncAuthOAuthService,
    )
    from datasluice.connectors.catalog.udata.services.contact_visualization import (
        AsyncContactVisualizationService as _AsyncContactVisualizationService,
    )
    from datasluice.connectors.catalog.udata.services.contact_visualization import (
        SyncContactVisualizationService as _SyncContactVisualizationService,
    )
    from datasluice.connectors.catalog.udata.services.dataservices import (
        AsyncDataservicesService as _AsyncDataservicesService,
    )
    from datasluice.connectors.catalog.udata.services.dataservices import (
        SyncDataservicesService as _SyncDataservicesService,
    )
    from datasluice.connectors.catalog.udata.services.datasets import (
        AsyncDatasetsService as _AsyncDatasetsService,
    )
    from datasluice.connectors.catalog.udata.services.datasets import (
        SyncDatasetsService as _SyncDatasetsService,
    )
    from datasluice.connectors.catalog.udata.services.extensions import (
        AsyncExtensionsService as _AsyncExtensionsService,
    )
    from datasluice.connectors.catalog.udata.services.extensions import (
        SyncExtensionsService as _SyncExtensionsService,
    )
    from datasluice.connectors.catalog.udata.services.harvest import (
        AsyncHarvestService as _AsyncHarvestService,
    )
    from datasluice.connectors.catalog.udata.services.harvest import (
        SyncHarvestService as _SyncHarvestService,
    )
    from datasluice.connectors.catalog.udata.services.jobs import (
        AsyncWorkerJobsService as _AsyncWorkerJobsService,
    )
    from datasluice.connectors.catalog.udata.services.jobs import (
        SyncWorkerJobsService as _SyncWorkerJobsService,
    )
    from datasluice.connectors.catalog.udata.services.organizations_memberships import (
        AsyncOrganizationsMembershipsService as _AsyncOrganizationsMembershipsService,
    )
    from datasluice.connectors.catalog.udata.services.organizations_memberships import (
        SyncOrganizationsMembershipsService as _SyncOrganizationsMembershipsService,
    )
    from datasluice.connectors.catalog.udata.services.posts_reports import (
        AsyncPostsReportsService as _AsyncPostsReportsService,
    )
    from datasluice.connectors.catalog.udata.services.posts_reports import (
        SyncPostsReportsService as _SyncPostsReportsService,
    )
    from datasluice.connectors.catalog.udata.services.resources import (
        AsyncResourcesService as _AsyncResourcesService,
    )
    from datasluice.connectors.catalog.udata.services.resources import (
        SyncResourcesService as _SyncResourcesService,
    )
    from datasluice.connectors.catalog.udata.services.reuses import (
        AsyncReusesService as _AsyncReusesService,
    )
    from datasluice.connectors.catalog.udata.services.reuses import (
        SyncReusesService as _SyncReusesService,
    )
    from datasluice.connectors.catalog.udata.services.root_profile import (
        AsyncRootProfileService as _AsyncRootProfileService,
    )
    from datasluice.connectors.catalog.udata.services.root_profile import (
        SyncRootProfileService as _SyncRootProfileService,
    )
    from datasluice.connectors.catalog.udata.services.spatial import (
        AsyncSpatialService as _AsyncSpatialService,
    )
    from datasluice.connectors.catalog.udata.services.spatial import (
        SyncSpatialService as _SyncSpatialService,
    )
    from datasluice.connectors.catalog.udata.services.taxonomies import (
        AsyncTaxonomiesService as _AsyncTaxonomiesService,
    )
    from datasluice.connectors.catalog.udata.services.taxonomies import (
        SyncTaxonomiesService as _SyncTaxonomiesService,
    )
    from datasluice.connectors.catalog.udata.services.topics import (
        AsyncTopicsService as _AsyncTopicsService,
    )
    from datasluice.connectors.catalog.udata.services.topics import (
        SyncTopicsService as _SyncTopicsService,
    )
    from datasluice.connectors.catalog.udata.services.transfers import (
        AsyncTransfersService as _AsyncTransfersService,
    )
    from datasluice.connectors.catalog.udata.services.transfers import (
        SyncTransfersService as _SyncTransfersService,
    )
    from datasluice.connectors.catalog.udata.services.users_tokens import (
        AsyncUsersTokensService as _AsyncUsersTokensService,
    )
    from datasluice.connectors.catalog.udata.services.users_tokens import (
        SyncUsersTokensService as _SyncUsersTokensService,
    )
    from datasluice.contracts.catalog.native.udata import UDataResultItem
    from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
    from datasluice.domain.catalog.models import ResultEnvelope
    from datasluice.domain.catalog.observability import TLSPolicy

_JSON_MEDIA_TYPE = "application/json"
_CONTROLLED_DISPATCH_AUTHORIZATION_IMMUTABLE = "Controlled dispatch authorization is immutable."
_CONTROLLED_DISPATCH_AUTHORIZATION_FACTORY_OWNED = "Controlled dispatch authorization is factory-owned."
_CONTROLLED_TRANSPORT_BINDINGS_FACTORY_OWNED = "Controlled transport bindings are factory-owned."
_CONTROLLED_UDATA_TRANSPORT_NOT_FACTORY_BOUND = "The controlled uData transport is not factory-bound."
_UDATA_SITE_PATCH_NOT_VERIFIED_CLIENT = "The uData site PATCH is not bound to the verified controlled client."
_INVALID_JSON_RESULT = "Catalog operation returned an invalid JSON result."
_SYNC_UDATA_CLIENT_CLOSED = "The synchronous uData client is closed."
_CATALOG_ORIGIN_CIRCUIT_OPEN = "The catalog origin circuit is open after consecutive transport failures."
_CATALOG_CIRCUIT_RETRY_ACTION = "Wait for the circuit cool-down or explicitly reset the circuit before retrying."
_ASYNC_UDATA_CLIENT_CLOSED = "The asynchronous uData client is closed."


def _reject_nonfinite_json(_: str) -> None:
    raise ValueError("Non-finite JSON constant.")


def _finite_json_float(value: str) -> float:
    result = float(value)
    if not math.isfinite(result):
        _reject_nonfinite_json(value)
    return result


_PROFILE_RESOURCE = "udata-17.6.json"
_PAGER_PARAMS = frozenset({"page", "page_size"})
_DATASET_PAGE_MAX_BYTES = 8 * 1024 * 1024
_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"
_CONTROLLED_SOURCE_COMMIT = "0546582058d84706812a1c37387576efc4e5ad1f"
_CONTROLLED_COMPOSE_SHA256 = "ca1fc88f7bd25bd0f049ffae245c496fb203cb8b57c2003f76dc93f67fa5fb39"
_CONTROLLED_DOCKERFILE_SHA256 = "8bb5a8bc32b3c78a5b359b5b567155df5f936b4818eea90d181312814fc0fb28"
_CONTROLLED_UDATA_LAYERS_SHA256 = "sha256:a91b7779e1225837c720078fff0fe151cfc2ca5da3c5a84d7c7563e819002bc6"
_CONTROLLED_DEPENDENCY_IMAGE_SPECS = (
    (
        "mongo",
        "mongo@sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47",
        "sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47",
        "mongo@sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47",
    ),
    (
        "redis",
        "redis@sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256",
        "sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256",
        "redis@sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256",
    ),
    (
        "search",
        "docker.elastic.co/elasticsearch/elasticsearch@sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
        "sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
        "docker.elastic.co/elasticsearch/elasticsearch@sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
    ),
    (
        "storage",
        "quay.io/minio/aistor/minio@sha256:a94e1fe399bd050fe6d6148839597a4995350c0e62c9e83f0cec8b5630ed0a1e",
        "sha256:a94e1fe399bd050fe6d6148839597a4995350c0e62c9e83f0cec8b5630ed0a1e",
        "quay.io/minio/aistor/minio@sha256:a94e1fe399bd050fe6d6148839597a4995350c0e62c9e83f0cec8b5630ed0a1e",
    ),
    (
        "mailpit",
        "axllent/mailpit@sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf",
        "sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf",
        "axllent/mailpit@sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf",
    ),
)
_CONTROLLED_IMAGE_DIGESTS = tuple(
    repository_digest for _, _, _, repository_digest in _CONTROLLED_DEPENDENCY_IMAGE_SPECS
)
_CONTROLLED_AUTHORITY_TTL_SECONDS = 60.0

_STATUS_RESPONSE_CLASSES = {
    401: ProbeResponseClass.UNAUTHORIZED,
    403: ProbeResponseClass.FORBIDDEN,
    423: ProbeResponseClass.DEPLOYMENT_DISABLED,
}
_AUTH_FAILURES = {
    "Invalid API token": "invalid",
    "Revoked API token": "revoked",
    "Expired API token": "expired",
    "Inactive user": "inactive-user",
}


def _auth_failure_reason(body: bytes) -> str | None:
    if len(body) > 512:
        return None
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("message"), str):
        return None
    return _AUTH_FAILURES.get(payload["message"])


def _settle_dataset_budget_error(
    error: BudgetExhaustedError, request: RuntimeRequest, response: RuntimeResponse | None
) -> None:
    if request.method not in {"GET", "HEAD", "OPTIONS"}:
        attach_catalog_metadata(
            error, {"ambiguous": True, "status_code": response.status_code if response is not None else 0}
        )


def _dataset_idempotency(
    policy: IdempotencyPolicy | None, method: str, files: tuple[UploadPart, ...]
) -> IdempotencyPolicy:
    """Return the retry policy for one request, never repeating a one-shot stream part."""
    if any(not isinstance(part.data, bytes) for part in files):
        return IdempotencyPolicy(safe=False)
    return policy or IdempotencyPolicy(safe=method == "GET")


def _dataset_retry_idempotency(
    form_body: bytes | Callable[[], bytes] | None,
    policy: IdempotencyPolicy | None,
    method: str,
    files: tuple[UploadPart, ...],
) -> IdempotencyPolicy:
    """Return the dataset retry policy, never repeating a streamed form body."""
    if callable(form_body):
        return IdempotencyPolicy(safe=False)
    return _dataset_idempotency(policy, method, files)


def _dataset_redirect_policy(redirect_mode: bool, files: tuple[UploadPart, ...]) -> RedirectPolicy:
    """Return the dataset redirect policy, never following redirects for uploads."""
    return RedirectPolicy.NO_FOLLOW if redirect_mode or files else RedirectPolicy.FOLLOW


def _stream_failure_outcome(error: BaseException) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    return "failed"


def _build_stream_request(
    *,
    owning_id: OperationId,
    origin: str,
    path: str,
    headers: Mapping[str, str] | None,
    resolved_credential: object,
    deadline: DeadlineMonitor,
    transport: object,
) -> tuple[RuntimeRequest, DeadlineMonitor, object]:
    """Build the no-follow stream request and prove the transport can stream it."""
    request_headers = dict(headers or {})
    request_headers.update(_auth_headers(resolved_credential))
    deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
    request = RuntimeRequest(
        method="GET",
        url=origin + path,
        headers=request_headers,
        redirect_policy=RedirectPolicy.NO_FOLLOW,
        deadline=deadline,
    )
    send_stream = getattr(transport, "send_stream", None)
    if not callable(send_stream):
        raise CatalogValidationError(
            "The uData export requires a streaming catalog transport.",
            operation=str(owning_id),
            platform=PLATFORM.value,
            safe_action="Use the default transport or inject one implementing send_stream.",
        )
    return request, deadline, send_stream


def _operation_id_from(value: str) -> OperationId:
    """Derive the internal OperationId from one pinned profile identity string."""
    platform, _, tail = value.partition("/")
    service, dot, method = tail.partition(".")
    if not dot:
        return OperationId(platform=platform, service="native", method=tail)
    return OperationId(platform=platform, service=service, method=method)


@lru_cache(maxsize=1)
def declared_udata_profile() -> DeclaredCapabilityProfile:
    """Load and pin the checked-in uData capability profile."""
    document = json.loads(
        resources.files("datasluice.contracts")
        .joinpath("catalog")
        .joinpath("profiles")
        .joinpath(_PROFILE_RESOURCE)
        .read_text(encoding="utf-8")
    )
    if document.get("platform") != PLATFORM.value:
        raise ValueError(f"The pinned {_PROFILE_RESOURCE} does not declare the uData platform.")
    operations: dict[OperationId, OperationSpec] = {}
    for item in document["operations"]:
        operation_id = _operation_id_from(item["id"])
        mutation = item["mutation"]
        operations[operation_id] = OperationSpec(
            id=operation_id,
            tier=OperationTier.NATIVE,
            request_type="CatalogOperationRequest",
            response_type="UDataResultItem",
            auth_class=AuthClass(item["authentication"]),
            mutation_class=MutationClass(mutation),
            idempotency=Idempotency.SAFE if mutation == "read" else Idempotency.CONDITIONAL,
            concurrency=ConcurrencyRequirement.NONE if mutation == "read" else ConcurrencyRequirement.OPTIONAL,
            atomicity=Atomicity.NONE if mutation == "read" else Atomicity.SINGLE_RESOURCE,
            capability_class=CapabilityClass(item["capability"]),
        )
    return DeclaredCapabilityProfile(
        profile_version=document["profile_version"],
        schema_version=document["schema_version"],
        platform_api_version=document["platform_api_version"],
        official_source_uri=document["official_source_uri"],
        source_accessed_at=date.fromisoformat(document["source_accessed_at"]),
        fixture_fingerprint=document["fixture_fingerprint"],
        operations=operations,
    )


def _origin_checked_runner(runner: ProbeRunner, origin: str) -> ProbeRunner:
    """Wrap a probe runner so only same-origin evidence is accepted."""

    class _Checked:
        def probe(self, operation_id: OperationId) -> ProbeEvidence:
            evidence = runner.probe(operation_id)
            if not evidence.deployment_url.startswith(origin + "/") and evidence.deployment_url != origin:
                raise CatalogValidationError(
                    "Capability probe evidence does not match the configured deployment origin.",
                    operation=str(operation_id),
                    platform=PLATFORM.value,
                    safe_action="Configure a probe runner scoped to the client origin.",
                )
            return evidence

    return _Checked()  # type: ignore[return-value]


def _origin_checked_async_runner(runner: AsyncProbeRunner, origin: str) -> AsyncProbeRunner:
    """Wrap an async probe runner so only same-origin evidence is accepted."""

    class _CheckedAsync:
        async def probe(self, operation_id: OperationId) -> ProbeEvidence:
            evidence = await runner.probe(operation_id)
            if not evidence.deployment_url.startswith(origin + "/") and evidence.deployment_url != origin:
                raise CatalogValidationError(
                    "Capability probe evidence does not match the configured deployment origin.",
                    operation=str(operation_id),
                    platform=PLATFORM.value,
                    safe_action="Configure a probe runner scoped to the client origin.",
                )
            return evidence

    return _CheckedAsync()  # type: ignore[return-value]


def _enforce_caller_guards(operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> None:
    if guard.operation_id != operation.operation_id:
        raise ValueError(
            f"Catalog operation guard operation_id {guard.operation_id} does not match request operation_id "
            f"{operation.operation_id}."
        )
    guard.require_allowed()


def _response_header(response: RuntimeResponse, name: str) -> str | None:
    """Read one response header without depending on its casing."""
    wanted = name.lower()
    for key, value in response.headers.items():
        if key.lower() == wanted:
            return value
    return None


def _decode_redirect_response(owning_id: OperationId, response: RuntimeResponse) -> tuple[int, dict[str, str]]:
    """Return the original redirect headers or raise one parity-stable route error."""
    location = _response_header(response, "location")
    if not location:
        raise NativeCatalogError(
            "The uData redirect response omits its Location header.",
            operation=str(owning_id),
            platform=PLATFORM.value,
            status_code=response.status_code,
        )
    return response.status_code, dict(response.headers)


def _page_request(
    *,
    origin: str,
    params: Mapping[str, int],
) -> RuntimeRequest:
    """Build one anonymous-safe paged dataset request; credentials attach later."""
    query = "&".join(f"{key}={value}" for key, value in sorted(params.items()))
    url = f"{origin}{_PAGED_PATH}" + (f"?{query}" if query else "")
    return RuntimeRequest(
        method="GET",
        url=url,
        headers={},
        body=None,
        max_response_bytes=_DATASET_PAGE_MAX_BYTES,
    )


@dataclass(frozen=True, slots=True)
class _ControlledStackEvidence:
    """Bounded non-secret facts observed from the controlled stack."""

    nonce_sha256: str = field(repr=False)
    site_id: str
    docker_endpoint_sha256: str = field(default="", repr=False)
    source_commit: str = field(default=_CONTROLLED_SOURCE_COMMIT, repr=False)
    compose_sha256: str = field(default=_CONTROLLED_COMPOSE_SHA256, repr=False)
    dockerfile_sha256: str = field(default=_CONTROLLED_DOCKERFILE_SHA256, repr=False)
    image_digests: tuple[str, ...] = field(default=_CONTROLLED_IMAGE_DIGESTS, repr=False)

    @property
    def digest(self) -> str:
        """Return a stable digest without exposing the stack nonce."""
        return hashlib.sha256(
            "|".join(
                (
                    self.source_commit,
                    self.compose_sha256,
                    self.dockerfile_sha256,
                    *self.image_digests,
                    self.nonce_sha256,
                    self.site_id,
                    self.docker_endpoint_sha256,
                )
            ).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class _ControlledSourceIdentity:
    nonce_sha256: str = field(repr=False)
    docker_endpoint: str = field(repr=False)
    udata_container_id: str = field(repr=False)
    image_identities: tuple[str, ...] = field(repr=False)


@dataclass(frozen=True, slots=True)
class _ControlledVerification:
    identity: _ControlledSourceIdentity = field(repr=False)
    evidence: _ControlledStackEvidence


def _controlled_error(message: str) -> CatalogValidationError:
    return CatalogValidationError(
        message,
        operation="udata/api-v1.set_site",
        platform=PLATFORM.value,
        safe_action="Use the verified disposable uData evidence stack.",
    )


class _ImmutableDispatchGateType(type):
    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object], **kwargs: object
    ) -> _ImmutableDispatchGateType:
        reserved = {
            "authorize_sync",
            "authorize_async",
            "dispatch_sync",
            "dispatch_async",
            "bind_sync_client",
            "bind_async_client",
        }
        inherited: set[str] = set()
        for base in bases:
            for ancestor in base.__mro__:
                inherited.update(attribute for attribute in reserved if attribute in ancestor.__dict__)
        if inherited.intersection(namespace):
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_IMMUTABLE)
        return super().__new__(cls, name, bases, namespace, **kwargs)

    def __setattr__(cls, name: str, value: object) -> None:
        if name in {
            "authorize_sync",
            "authorize_async",
            "dispatch_sync",
            "dispatch_async",
            "bind_sync_client",
            "bind_async_client",
        }:
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_IMMUTABLE)
        super().__setattr__(name, value)

    def __delattr__(cls, name: str) -> None:
        if name in {
            "authorize_sync",
            "authorize_async",
            "dispatch_sync",
            "dispatch_async",
            "bind_sync_client",
            "bind_async_client",
        }:
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_IMMUTABLE)
        super().__delattr__(name)


class _ImmutableClientType(type):
    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object], **kwargs: object
    ) -> _ImmutableClientType:
        reserved = {
            "_mutation_dispatch_gate",
            "_dataset_call",
            "_dataset_call_async",
            "_root_call",
            "_root_call_async",
            "__getattribute__",
            "__getattr__",
            "__setattr__",
        }
        inherited: set[str] = set()
        for base in bases:
            for ancestor in base.__mro__:
                inherited.update(attribute for attribute in reserved if attribute in ancestor.__dict__)
        if inherited.intersection(namespace):
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_FACTORY_OWNED)
        return super().__new__(cls, name, bases, namespace, **kwargs)

    def __setattr__(cls, name: str, value: object) -> None:
        if name in {
            "_mutation_dispatch_gate",
            "_dataset_call",
            "_dataset_call_async",
            "_root_call",
            "_root_call_async",
            "__getattribute__",
            "__getattr__",
            "__setattr__",
        }:
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_FACTORY_OWNED)
        super().__setattr__(name, value)

    def __delattr__(cls, name: str) -> None:
        if name in {
            "_mutation_dispatch_gate",
            "_dataset_call",
            "_dataset_call_async",
            "_root_call",
            "_root_call_async",
            "__getattribute__",
            "__getattr__",
            "__setattr__",
        }:
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_FACTORY_OWNED)
        super().__delattr__(name)


class _ImmutableTransportType(type):
    def __new__(
        cls, name: str, bases: tuple[type, ...], namespace: dict[str, object], **kwargs: object
    ) -> _ImmutableTransportType:
        reserved = {
            "__init__",
            "send",
            "send_stream",
            "close",
            "verify",
            "aclose",
            "__getattribute__",
            "__getattr__",
            "__setattr__",
        }
        inherited = any(reserved.intersection(ancestor.__dict__) for base in bases for ancestor in base.__mro__)
        if inherited and reserved.intersection(namespace):
            raise AttributeError(_CONTROLLED_TRANSPORT_BINDINGS_FACTORY_OWNED)
        return super().__new__(cls, name, bases, namespace, **kwargs)

    def __setattr__(cls, name: str, value: object) -> None:
        if name in {
            "__init__",
            "send",
            "send_stream",
            "close",
            "verify",
            "aclose",
            "__getattribute__",
            "__getattr__",
            "__setattr__",
        }:
            raise AttributeError(_CONTROLLED_TRANSPORT_BINDINGS_FACTORY_OWNED)
        super().__setattr__(name, value)

    def __delattr__(cls, name: str) -> None:
        if name in {
            "__init__",
            "send",
            "send_stream",
            "close",
            "verify",
            "aclose",
            "__getattribute__",
            "__getattr__",
            "__setattr__",
        }:
            raise AttributeError(_CONTROLLED_TRANSPORT_BINDINGS_FACTORY_OWNED)
        super().__delattr__(name)


class _IdentityRegistry[T]:
    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[int, tuple[weakref.ReferenceType[object], T]] = {}

    def set(self, key: object, value: T) -> None:
        key_id = id(key)

        def remove(reference: weakref.ReferenceType[object]) -> None:
            entry = self._entries.get(key_id)
            if entry is not None and entry[0] is reference:
                self._entries.pop(key_id, None)

        self._entries[key_id] = (weakref.ref(key, remove), value)

    def get(self, key: object) -> T | None:
        entry = self._entries.get(id(key))
        if entry is None or entry[0]() is not key:
            return None
        return entry[1]

    def discard(self, key: object) -> None:
        entry = self._entries.get(id(key))
        if entry is not None and entry[0]() is key:
            self._entries.pop(id(key), None)


@dataclass(frozen=True, slots=True)
class _ControlledSyncOperations:
    verify: Callable[..., _ControlledVerification]
    dispatch: Callable[..., RuntimeResponse]


@dataclass(frozen=True, slots=True)
class _ControlledAsyncOperations:
    verify: Callable[..., Awaitable[_ControlledVerification]]
    dispatch: Callable[..., Awaitable[RuntimeResponse]]


type _ControlledSyncState = tuple[CatalogTransport, _ControlledStackEvidence, float, _ControlledSyncOperations, str]
type _ControlledAsyncState = tuple[
    AsyncCatalogTransport, _ControlledStackEvidence | None, float, _ControlledAsyncOperations, str | None
]


def _registry_state[T](registry: _IdentityRegistry[T], value: object) -> T | None:
    try:
        return registry.get(value)
    except TypeError:
        return None


def _build_controlled_sync_transport(
    registry: _IdentityRegistry[_ControlledSyncState],
    trusted_sync_operations: _ControlledSyncOperations,
    clock: Callable[[], float],
) -> type:
    transport_type = HttpxCatalogTransport
    initializer = HttpxCatalogTransport.__init__
    send = HttpxCatalogTransport.send
    send_stream = HttpxCatalogTransport.send_stream
    close = HttpxCatalogTransport.close

    class _ControlledSyncTransport(metaclass=_ImmutableTransportType):
        __slots__ = ("__weakref__",)

        def __init__(self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None) -> None:
            transport = object.__new__(transport_type)
            try:
                cast("Callable[..., None]", initializer)(transport, tls_policy=tls_policy, budget=budget)
                verification = trusted_sync_operations.verify(transport)
            except SETTLEMENT_ERRORS:
                cast("Callable[[HttpxCatalogTransport], None]", close)(transport)
                raise
            registry.set(
                self,
                (
                    transport,
                    verification.evidence,
                    clock(),
                    trusted_sync_operations,
                    verification.identity.docker_endpoint,
                ),
            )

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            state = _registry_state(registry, self)
            if state is None:
                raise _controlled_error(_CONTROLLED_UDATA_TRANSPORT_NOT_FACTORY_BOUND)
            return cast("Callable[[HttpxCatalogTransport, RuntimeRequest], RuntimeResponse]", send)(
                cast("HttpxCatalogTransport", state[0]), request
            )

        def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
            state = _registry_state(registry, self)
            if state is None:
                raise _controlled_error(_CONTROLLED_UDATA_TRANSPORT_NOT_FACTORY_BOUND)
            return cast("Callable[[HttpxCatalogTransport, RuntimeRequest], RuntimeStreamResponse]", send_stream)(
                cast("HttpxCatalogTransport", state[0]), request
            )

        def close(self) -> None:
            state = _registry_state(registry, self)
            if state is not None:
                cast("Callable[[HttpxCatalogTransport], None]", close)(cast("HttpxCatalogTransport", state[0]))

    return _ControlledSyncTransport


def _build_controlled_async_transport(
    registry: _IdentityRegistry[_ControlledAsyncState],
    trusted_async_operations: _ControlledAsyncOperations,
    clock: Callable[[], float],
) -> type:
    transport_type = AsyncHttpxCatalogTransport
    initializer = AsyncHttpxCatalogTransport.__init__
    send = AsyncHttpxCatalogTransport.send
    send_stream = AsyncHttpxCatalogTransport.send_stream
    close = AsyncHttpxCatalogTransport.aclose

    class _ControlledAsyncTransport(metaclass=_ImmutableTransportType):
        __slots__ = ("__weakref__",)

        def __init__(self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None) -> None:
            transport = object.__new__(transport_type)
            cast("Callable[..., None]", initializer)(transport, tls_policy=tls_policy, budget=budget)
            registry.set(self, (transport, None, 0.0, trusted_async_operations, None))

        async def verify(self) -> None:
            state = _registry_state(registry, self)
            if state is None:
                return
            try:
                verification = await state[3].verify(state[0])
            except ASYNC_SETTLEMENT_ERRORS:
                await cast("Callable[[AsyncHttpxCatalogTransport], Awaitable[None]]", close)(
                    cast("AsyncHttpxCatalogTransport", state[0])
                )
                registry.discard(self)
                raise
            registry.set(
                self,
                (state[0], verification.evidence, clock(), state[3], verification.identity.docker_endpoint),
            )

        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            state = _registry_state(registry, self)
            if state is None or state[1] is None:
                raise _controlled_error(_CONTROLLED_UDATA_TRANSPORT_NOT_FACTORY_BOUND)
            return await cast(
                "Callable[[AsyncHttpxCatalogTransport, RuntimeRequest], Awaitable[RuntimeResponse]]", send
            )(cast("AsyncHttpxCatalogTransport", state[0]), request)

        async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
            state = _registry_state(registry, self)
            if state is None or state[1] is None:
                raise _controlled_error(_CONTROLLED_UDATA_TRANSPORT_NOT_FACTORY_BOUND)
            return await cast(
                "Callable[[AsyncHttpxCatalogTransport, RuntimeRequest], Awaitable[AsyncRuntimeStreamResponse]]",
                send_stream,
            )(cast("AsyncHttpxCatalogTransport", state[0]), request)

        async def aclose(self) -> None:
            state = _registry_state(registry, self)
            if state is not None:
                await cast("Callable[[AsyncHttpxCatalogTransport], Awaitable[None]]", close)(
                    cast("AsyncHttpxCatalogTransport", state[0])
                )

    return _ControlledAsyncTransport


def _build_controlled_sync_accessors(
    registry: _IdentityRegistry[_ControlledSyncState], clock: Callable[[], float]
) -> tuple[Callable[..., object], ...]:
    def site_id(value: object) -> str | None:
        state = _registry_state(registry, value)
        return state[1].site_id if state is not None else None

    def evidence_digest(value: object) -> str | None:
        state = _registry_state(registry, value)
        return state[1].digest if state is not None else None

    def revalidate(value: object, *, origin: str, site_id: str) -> bool:
        state = _registry_state(registry, value)
        if state is None:
            return False
        evidence = state[3].verify(state[0], docker_endpoint=state[4]).evidence
        return (
            origin == _CONTROLLED_ORIGIN
            and clock() - state[2] <= _CONTROLLED_AUTHORITY_TTL_SECONDS
            and evidence == state[1]
            and evidence.site_id == site_id
        )

    return site_id, evidence_digest, revalidate


def _build_controlled_async_accessors(
    registry: _IdentityRegistry[_ControlledAsyncState], clock: Callable[[], float]
) -> tuple[Callable[..., object], ...]:
    def site_id(value: object) -> str | None:
        state = _registry_state(registry, value)
        return state[1].site_id if state is not None and state[1] is not None else None

    def evidence_digest(value: object) -> str | None:
        state = _registry_state(registry, value)
        return state[1].digest if state is not None and state[1] is not None else None

    async def revalidate(value: object, *, origin: str, site_id: str) -> bool:
        state = _registry_state(registry, value)
        if state is None or state[1] is None:
            return False
        evidence = (await state[3].verify(state[0], docker_endpoint=state[4])).evidence
        return (
            origin == _CONTROLLED_ORIGIN
            and clock() - state[2] <= _CONTROLLED_AUTHORITY_TTL_SECONDS
            and evidence == state[1]
            and evidence.site_id == site_id
        )

    return site_id, evidence_digest, revalidate


def _build_controlled_accessors(
    sync_registry: _IdentityRegistry[_ControlledSyncState],
    async_registry: _IdentityRegistry[_ControlledAsyncState],
    clock: Callable[[], float],
) -> tuple[Callable[..., object], ...]:
    return (
        *_build_controlled_sync_accessors(sync_registry, clock),
        *_build_controlled_async_accessors(async_registry, clock),
    )


def _authorize_sync_dispatch(
    client_registry: _IdentityRegistry[object],
    transport_registry: _IdentityRegistry[_ControlledSyncState],
    client: object,
    transport: object,
    request: RuntimeRequest,
    origin: str,
    clock: Callable[[], float],
) -> bool:
    if client_registry.get(client) is not transport:
        return False
    if origin != _CONTROLLED_ORIGIN or request.method != "PATCH" or request.url != f"{_CONTROLLED_ORIGIN}/api/1/site/":
        return False
    state = _registry_state(transport_registry, transport)
    if state is None:
        return False
    try:
        evidence = state[3].verify(state[0], docker_endpoint=state[4]).evidence
    except Exception:
        return False
    return clock() - state[2] <= _CONTROLLED_AUTHORITY_TTL_SECONDS and evidence == state[1]


async def _authorize_async_dispatch(
    client_registry: _IdentityRegistry[object],
    transport_registry: _IdentityRegistry[_ControlledAsyncState],
    client: object,
    transport: object,
    request: RuntimeRequest,
    origin: str,
    clock: Callable[[], float],
) -> bool:
    if client_registry.get(client) is not transport:
        return False
    if origin != _CONTROLLED_ORIGIN or request.method != "PATCH" or request.url != f"{_CONTROLLED_ORIGIN}/api/1/site/":
        return False
    state = _registry_state(transport_registry, transport)
    if state is None or state[1] is None:
        return False
    try:
        evidence = (await state[3].verify(state[0], docker_endpoint=state[4])).evidence
    except Exception:
        return False
    return clock() - state[2] <= _CONTROLLED_AUTHORITY_TTL_SECONDS and evidence == state[1]


def _controlled_dispatch_state[T](
    client_registry: _IdentityRegistry[object],
    transport_registry: _IdentityRegistry[T],
    client: object,
    transport: object,
    request: RuntimeRequest,
    credential: object,
    body: object,
    origin: str,
) -> T:
    if client_registry.get(client) is not transport:
        raise _controlled_error(_UDATA_SITE_PATCH_NOT_VERIFIED_CLIENT)
    if origin != _CONTROLLED_ORIGIN or request.method != "PATCH" or request.url != f"{_CONTROLLED_ORIGIN}/api/1/site/":
        raise _controlled_error(_UDATA_SITE_PATCH_NOT_VERIFIED_CLIENT)
    state = _registry_state(transport_registry, transport)
    if state is None or not isinstance(credential, UDataCredential) or not isinstance(body, Mapping):
        raise _controlled_error("The controlled uData site PATCH requires a resolved credential and body.")
    return state


def _dispatch_sync_controlled(
    client_registry: _IdentityRegistry[object],
    transport_registry: _IdentityRegistry[_ControlledSyncState],
    client: object,
    transport: object,
    request: RuntimeRequest,
    credential: object,
    body: object,
    origin: str,
) -> RuntimeResponse:
    state = _controlled_dispatch_state(
        client_registry, transport_registry, client, transport, request, credential, body, origin
    )
    return state[3].dispatch(state[0], credential, body, docker_endpoint=state[4])


async def _dispatch_async_controlled(
    client_registry: _IdentityRegistry[object],
    transport_registry: _IdentityRegistry[_ControlledAsyncState],
    client: object,
    transport: object,
    request: RuntimeRequest,
    credential: object,
    body: object,
    origin: str,
) -> RuntimeResponse:
    state = _controlled_dispatch_state(
        client_registry, transport_registry, client, transport, request, credential, body, origin
    )
    if state[4] is None:
        raise _controlled_error(_CONTROLLED_UDATA_TRANSPORT_NOT_FACTORY_BOUND)
    return await state[3].dispatch(state[0], credential, body, docker_endpoint=state[4])


def _build_controlled_dispatch_gate(
    sync_registry: _IdentityRegistry[_ControlledSyncState],
    async_registry: _IdentityRegistry[_ControlledAsyncState],
    sync_client_registry: _IdentityRegistry[object],
    async_client_registry: _IdentityRegistry[object],
    clock: Callable[[], float],
) -> object:
    class _ControlledDispatchGate(metaclass=_ImmutableDispatchGateType):
        __slots__ = ()

        def __get__(self, instance: object | None, owner: type | None = None) -> _ControlledDispatchGate:
            return self

        def __set__(self, instance: object, value: object) -> None:
            raise AttributeError(_CONTROLLED_DISPATCH_AUTHORIZATION_FACTORY_OWNED)

        def bind_sync_client(self, client: object, transport: object) -> None:
            if _registry_state(sync_registry, transport) is not None:
                sync_client_registry.set(client, transport)

        def bind_async_client(self, client: object, transport: object) -> None:
            if _registry_state(async_registry, transport) is not None:
                async_client_registry.set(client, transport)

        def authorize_sync(self, client: object, transport: object, request: RuntimeRequest, *, origin: str) -> bool:
            return _authorize_sync_dispatch(
                sync_client_registry, sync_registry, client, transport, request, origin, clock
            )

        async def authorize_async(
            self, client: object, transport: object, request: RuntimeRequest, *, origin: str
        ) -> bool:
            return await _authorize_async_dispatch(
                async_client_registry, async_registry, client, transport, request, origin, clock
            )

        def dispatch_sync(
            self,
            client: object,
            transport: object,
            request: RuntimeRequest,
            credential: object,
            body: object,
            *,
            origin: str,
        ) -> RuntimeResponse:
            return _dispatch_sync_controlled(
                sync_client_registry, sync_registry, client, transport, request, credential, body, origin
            )

        async def dispatch_async(
            self,
            client: object,
            transport: object,
            request: RuntimeRequest,
            credential: object,
            body: object,
            *,
            origin: str,
        ) -> RuntimeResponse:
            return await _dispatch_async_controlled(
                async_client_registry, async_registry, client, transport, request, credential, body, origin
            )

    return _ControlledDispatchGate()


def _build_controlled_types():
    clock = monotonic
    sync_registry: _IdentityRegistry[_ControlledSyncState] = _IdentityRegistry()
    async_registry: _IdentityRegistry[_ControlledAsyncState] = _IdentityRegistry()
    sync_client_registry: _IdentityRegistry[object] = _IdentityRegistry()
    async_client_registry: _IdentityRegistry[object] = _IdentityRegistry()
    accessors = _build_controlled_accessors(sync_registry, async_registry, clock)
    gate = _build_controlled_dispatch_gate(
        sync_registry, async_registry, sync_client_registry, async_client_registry, clock
    )
    return sync_registry, async_registry, *accessors, gate


(
    _controlled_sync_registry,
    _controlled_async_registry,
    _controlled_sync_site_id,
    _controlled_sync_evidence_digest,
    _controlled_sync_revalidate,
    _controlled_async_site_id,
    _controlled_async_evidence_digest,
    _controlled_async_revalidate,
    _controlled_dispatch_gate,
) = _build_controlled_types()


@dataclass(slots=True)
class _StreamGuard[T]:
    client: _UDataClientCore
    owning_id: OperationId
    key: CircuitKey
    response: T
    deadline: DeadlineMonitor
    settled: bool = False
    consumed: bool = False
    _settlement_errors: ClassVar[tuple[type[BaseException], ...]] = SETTLEMENT_ERRORS

    def settle_failure(self, error: BaseException) -> None:
        if self.settled:
            return
        self.settled = True
        if isinstance(error, TransportError):
            before = self.client._breakers.inspect(self.key)
            after = self.client._breakers.record_transport_failure(self.key)
            self.client._emit_breaker_change(self.owning_id, before.open, after.open)
        if isinstance(error, BudgetExhaustedError):
            used = max(0.0, self.client._budget.total - self.deadline.remaining())
            self.client._emit(self.owning_id, "budget_exhausted", budget_usage=used)
        elif error.__class__.__name__ == "CancelledError":
            self.client._emit(self.owning_id, "cancelled")
        else:
            self.client._emit(self.owning_id, "failed")

    def settle_success(self) -> None:
        if self.settled:
            return
        try:
            self.deadline.assert_dispatchable(str(self.owning_id), PLATFORM.value)
        except self._settlement_errors as error:
            self.settle_failure(error)
            raise
        self.settled = True
        before = self.client._breakers.inspect(self.key)
        after = self.client._breakers.record_success(self.key)
        self.client._emit_breaker_change(self.owning_id, before.open, after.open)
        self.client._emit(self.owning_id, "succeeded")


@dataclass(slots=True)
class _SyncStreamGuard(_StreamGuard[RuntimeStreamResponse]):
    stream_chunks: Generator[bytes] | None = None

    def chunks(self) -> Generator[bytes]:
        try:
            for chunk in self.response:
                self.deadline.assert_dispatchable(str(self.owning_id), PLATFORM.value)
                yield chunk
        except GeneratorExit:
            raise
        except SETTLEMENT_ERRORS as error:
            self.settle_failure(error)
            raise
        else:
            self.consumed = True

    def close(self) -> None:
        if self.stream_chunks is None:
            raise RuntimeError("The catalog stream was not initialized.")
        primary_error: BaseException | None = None
        try:
            self.stream_chunks.close()
        except Exception as error:
            primary_error = error
            self.settle_failure(error)
        try:
            self.response.close()
        except Exception as cleanup_error:
            if primary_error is not None:
                raise primary_error from cleanup_error
            self.settle_failure(cleanup_error)
            raise
        if primary_error is not None:
            raise primary_error
        if not self.settled and not self.consumed:
            self.settle_failure(RuntimeError("The catalog stream was closed before completion."))

    def response_wrapper(self) -> RuntimeStreamResponse:
        self.stream_chunks = self.chunks()
        return RuntimeStreamResponse(
            status_code=self.response.status_code,
            headers=self.response.headers,
            chunks=self.stream_chunks,
            close_callback=self.close,
            retry_after=self.response.retry_after,
            failure_callback=self.settle_failure,
            completion_callback=self.settle_success,
        )


@dataclass(slots=True)
class _AsyncStreamGuard(_StreamGuard[AsyncRuntimeStreamResponse]):
    stream_chunks: AsyncGenerator[bytes] | None = None
    _settlement_errors = ASYNC_SETTLEMENT_ERRORS

    async def chunks(self) -> AsyncGenerator[bytes]:
        try:
            async for chunk in self.response:
                self.deadline.assert_dispatchable(str(self.owning_id), PLATFORM.value)
                yield chunk
        except ASYNC_SETTLEMENT_ERRORS as error:
            self.settle_failure(error)
            raise
        else:
            self.consumed = True

    async def close(self) -> None:
        if self.stream_chunks is None:
            raise RuntimeError("The catalog stream was not initialized.")
        primary_error: BaseException | None = None
        try:
            await self.stream_chunks.aclose()
        except (Exception, asyncio.CancelledError) as error:
            primary_error = error
            self.settle_failure(error)
        try:
            await self.response.aclose()
        except (Exception, asyncio.CancelledError) as cleanup_error:
            if primary_error is not None:
                raise primary_error from cleanup_error
            self.settle_failure(cleanup_error)
            raise
        if primary_error is not None:
            raise primary_error
        if not self.settled and not self.consumed:
            self.settle_failure(RuntimeError("The catalog stream was closed before completion."))

    def response_wrapper(self) -> AsyncRuntimeStreamResponse:
        self.stream_chunks = self.chunks()
        return AsyncRuntimeStreamResponse(
            status_code=self.response.status_code,
            headers=self.response.headers,
            chunks=self.stream_chunks,
            close_callback=self.close,
            retry_after=self.response.retry_after,
            failure_callback=self.settle_failure,
            completion_callback=self.settle_success,
        )


@dataclass(slots=True)
class _RequestAttempt:
    client: _UDataClientCore
    owning_id: OperationId
    key: CircuitKey
    request: RuntimeRequest
    recorded: bool = False
    attempts: int = 0
    response: RuntimeResponse | None = None

    @contextmanager
    def dispatch(self, form_body: Callable[[], bytes] | None = None) -> Generator[RuntimeRequest]:
        self.attempts += 1
        self.recorded = False
        self.response = None
        before = self.client._breakers.inspect(self.key)
        request = replace(self.request, body=form_body()) if form_body is not None else self.request
        if request.deadline is not None:
            request.deadline.assert_dispatchable()
        completed = False
        try:
            yield request
            if request.deadline is not None:
                request.deadline.assert_dispatchable()
            completed = True
        except BudgetExhaustedError as error:
            _settle_dataset_budget_error(error, request, self.response)
            raise
        except TransportError:
            self.recorded = True
            after = self.client._breakers.record_transport_failure(self.key)
            self.client._emit_breaker_change(self.owning_id, before.open, after.open)
            raise
        finally:
            if completed:
                assert self.response is not None
                after = self.client._breakers.record_response(self.key, self.response.status_code)
                self.recorded = True
                self.client._emit_breaker_change(self.owning_id, before.open, after.open)

    def release_trial(self) -> None:
        if not self.recorded:
            self.client._breakers.release_trial(self.key)


class _UDataClientOptions(TypedDict, total=False):
    budget: TimeBudget | None
    breakers: BreakerRegistry | None
    max_attempts: int
    capability_cache_ttl: float
    root_export_max_bytes: int


class _DatasetCallOverrides(TypedDict, total=False):
    raw_text: bool
    redirect_mode: bool
    emit_success: bool
    accept_status: Callable[[int], bool] | None
    omit_credential: bool
    deadline: DeadlineMonitor | None


class _SyncUDataClientOptions(_UDataClientOptions, total=False):
    probe_runner: ProbeRunner | None


class _AsyncUDataClientOptions(_UDataClientOptions, total=False):
    async_probe_runner: AsyncProbeRunner | None


class _UDataClientCore(metaclass=_ImmutableClientType):
    """Shared strict-gate state for the sync and async uData clients."""

    def __init__(
        self,
        transport: CatalogTransport | AsyncCatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        settings: UDataClientSettings,
        credentials: object | None,
        *,
        clock: Callable[[], float] = monotonic,
        emitter: EventEmitter | None = None,
        owns_transport: bool = True,
        site_gate: SiteVersionGate | AsyncSiteVersionGate | None = None,
        async_gate: bool = False,
    ) -> None:
        self._transport = transport
        self._credential_scope = _credential_scope(credentials)
        self._owns_transport = owns_transport
        self._origin = settings.base_url
        checked_origin = self._origin
        self._capabilities = EffectiveCapabilityCache(
            profile,
            probe_runner=(
                _origin_checked_runner(settings.probe_runner, checked_origin) if settings.probe_runner else None
            ),
            async_probe_runner=(
                _origin_checked_async_runner(settings.async_probe_runner, checked_origin)
                if settings.async_probe_runner
                else None
            ),
            namespace=checked_origin,
            deployment_origin=checked_origin if checked_origin.startswith("https://") else None,
            ttl_seconds=settings.capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        self._route_gate = UDataRouteGate(self._origin, transport, settings.capability_cache_ttl, clock)
        if site_gate is not None:
            self._site_gate: SiteVersionGate | AsyncSiteVersionGate = site_gate
        elif not async_gate:
            self._site_gate = SiteVersionGate(
                origin=self._origin,
                transport=cast("CatalogTransport", transport),
                ttl_seconds=settings.capability_cache_ttl,
                clock=clock,
            )
        else:
            self._site_gate = AsyncSiteVersionGate(
                origin=self._origin,
                transport=cast("AsyncCatalogTransport", transport),
                ttl_seconds=settings.capability_cache_ttl,
                clock=clock,
            )
        self._credentials = credentials
        self._budget = settings.budget or TimeBudget()
        self._breakers = settings.breakers or BreakerRegistry(
            failure_threshold=DEFAULT_BREAKER_FAILURE_THRESHOLD,
            cooldown=DEFAULT_BREAKER_COOLDOWN_SECONDS,
            clock=clock,
        )
        self._max_attempts = settings.max_attempts
        self._root_export_max_bytes = settings.root_export_max_bytes
        self._clock = clock
        self._emitter = emitter or EventEmitter()
        self._closed = False

    @property
    def transport(self) -> CatalogTransport | AsyncCatalogTransport:
        """Expose the underlying transport as the public introspection seam."""
        return self._transport

    def _resolved_credential(self) -> object | None:
        """Resolve the current credential once for pre-dispatch validation."""
        return _refreshed_credential(self._credentials)

    async def _resolved_credential_async(self) -> object | None:
        """Resolve the current credential asynchronously for pre-dispatch validation."""
        return await _refreshed_credential_async(self._credentials)

    def _require_route_operation(self, owning_id: OperationId) -> None:
        if owning_id not in self._profile.declared_profile.operations:
            raise UnsupportedCapabilityError(
                "The uData profile does not declare this operation.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unsupported",
                safe_action="Use an operation declared by the uData profile.",
            )
        if not self._controlled_origin() and self._declared_mutation_class(owning_id) is not MutationClass.READ:
            raise CatalogValidationError(
                "uData mutations, administration, and uploads require a controlled local deployment.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                safe_action="Point the client at a loopback uData deployment before dispatching this route.",
            )

    def _require_dispatchable(
        self,
        owning_id: OperationId,
        effective: EffectiveCapabilityProfile,
        permissions: EffectivePermissions | None = None,
    ) -> None:
        self._require_route_operation(owning_id)
        build_catalog_operation_guard(owning_id, effective, permissions=permissions).require_allowed()

    def _controlled_origin(self) -> bool:
        """Return whether the configured origin addresses a loopback deployment."""
        return urlsplit(self._origin).hostname in LOOPBACK_HOSTS

    def _declared_mutation_class(self, owning_id: OperationId) -> MutationClass | None:
        """Return the pinned profile mutation class, or None when the route is undeclared."""
        operation = self._profile.declared_profile.operations.get(owning_id)
        return operation.mutation_class if operation is not None else None

    @property
    def credentials(self) -> object | None:
        """Expose the injected caller-owned credential resolver or provider."""
        return self._credentials

    def _emit(self, owning_id: OperationId | str, outcome: str, **metadata: object) -> None:
        self._emitter.record(
            operation_id=str(owning_id),
            platform=PLATFORM.value,
            outcome=outcome,
            metadata=metadata,
        )

    def _emit_breaker_change(self, owning_id: OperationId, before: bool, after: bool) -> None:
        if before != after:
            self._emit(owning_id, "breaker_state_change", breaker_open=after)

    def _refresh_credential_scope(self, credential: object | None) -> str:
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        return scope

    def _dataset_deadline(
        self, owning_operation: str, overrides: _DatasetCallOverrides, closed_error: str
    ) -> tuple[OperationId, DeadlineMonitor]:
        unknown = set(overrides) - set(_DatasetCallOverrides.__annotations__)
        if unknown:
            raise TypeError(f"Unknown dataset call overrides: {sorted(unknown)}.")
        if self._closed:
            raise RuntimeError(closed_error)
        owning_id = _operation_id_from(owning_operation)
        deadline = overrides.get("deadline") or DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        return owning_id, deadline

    def _dataset_request(
        self,
        owning_id: OperationId,
        deadline: DeadlineMonitor,
        *,
        method: str,
        path: str,
        headers: Mapping[str, str] | None,
        credential: object | None,
        json_body: object,
        form_body: bytes | Callable[[], bytes] | None,
        files: tuple[UploadPart, ...],
        idempotency_policy: IdempotencyPolicy | None,
        max_response_bytes: int | None,
        overrides: _DatasetCallOverrides,
    ) -> RuntimeRequest:
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        body = self._dataset_body(json_body, form_body, files, owning_id)
        return RuntimeRequest(
            method=method,
            url=self._origin + path,
            headers=self._request_headers(
                headers, credential, idempotency_policy, body, json_media_fallback=form_body is None and not files
            ),
            body=body,
            files=files,
            redirect_policy=_dataset_redirect_policy(overrides.get("redirect_mode", False), files),
            max_response_bytes=1_048_576 if max_response_bytes is None else max_response_bytes,
            deadline=deadline,
        )

    @contextmanager
    def _dataset_retry(self, attempt: _RequestAttempt, *, emit_success: bool) -> Generator[None]:
        try:
            yield
        except Exception as error:
            self._emit_dataset_failure(attempt.owning_id, error, emit_success=emit_success)
            raise
        finally:
            attempt.release_trial()

    def _finish_dataset_call(
        self,
        owning_id: OperationId,
        response: RuntimeResponse,
        request: RuntimeRequest,
        scope: str,
        overrides: _DatasetCallOverrides,
    ) -> tuple[int, object, RuntimeResponse]:
        emit_success = overrides.get("emit_success", True)
        try:
            result = self._decode_dataset_response(
                owning_id,
                response,
                redirect_mode=overrides.get("redirect_mode", False),
                raw_text=overrides.get("raw_text", False),
                max_response_bytes=request.max_response_bytes,
                method=request.method,
                credential_scope=scope,
                accept_status=overrides.get("accept_status"),
            )
        except Exception as error:
            self._emit_dataset_failure(owning_id, error, emit_success=emit_success)
            raise
        if emit_success:
            self._emit(owning_id, "succeeded")
        return result

    def _dataset_body(
        self,
        json_body: object,
        form_body: bytes | Callable[[], bytes] | None,
        files: tuple[UploadPart, ...],
        owning_id: OperationId,
    ) -> bytes | None:
        if form_body is not None and (json_body is not None or files):
            raise NativeCatalogError(
                "Catalog requests cannot combine a form body with a JSON body or multipart parts.",
                operation=str(owning_id),
                platform=PLATFORM.value,
            )
        if json_body is not None and files:
            raise NativeCatalogError(
                "Catalog dataset requests cannot combine a JSON body with multipart parts.",
                operation=str(owning_id),
                platform=PLATFORM.value,
            )
        if files:
            return None
        if callable(form_body):
            return None
        return form_body if form_body is not None else self._json_body(json_body, owning_id)

    @staticmethod
    def _json_body(json_body: object, owning_id: OperationId) -> bytes | None:
        if json_body is None:
            return None
        try:
            return json.dumps(json_body, allow_nan=False).encode()
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog mutation input could not be serialized as JSON.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                metadata={"phase": "serialization"},
            ) from exc

    @staticmethod
    def _request_headers(
        headers: Mapping[str, str] | None,
        credential: object | None,
        idempotency_policy: IdempotencyPolicy | None,
        body: bytes | None,
        json_media_fallback: bool = False,
    ) -> dict[str, str]:
        request_headers = dict(headers or {})
        request_headers.update(_auth_headers(credential))
        if idempotency_policy is not None and idempotency_policy.key is not None:
            request_headers["Idempotency-Key"] = idempotency_policy.key
        if (
            body is not None
            and json_media_fallback
            and not any(key.lower() == "content-type" for key in request_headers)
        ):
            request_headers["Content-Type"] = _JSON_MEDIA_TYPE
        return request_headers

    def _admit_request(self, owning_id: OperationId, request: RuntimeRequest) -> CircuitKey:
        key = _circuit_key(request, self._credentials)
        if self._breakers.admit(key):
            return key
        self._emit(owning_id, "breaker_open")
        raise CatalogUnavailableError(
            _CATALOG_ORIGIN_CIRCUIT_OPEN,
            operation=str(owning_id),
            platform=PLATFORM.value,
            capability_state="unavailable",
            safe_action=_CATALOG_CIRCUIT_RETRY_ACTION,
        )

    def _decode_dataset_response(
        self,
        owning_id: OperationId,
        response: RuntimeResponse,
        *,
        redirect_mode: bool,
        raw_text: bool,
        max_response_bytes: int | None,
        method: str,
        credential_scope: str,
        accept_status: Callable[[int], bool] | None = None,
    ) -> tuple[int, object, RuntimeResponse]:
        self._validate_status(
            owning_id,
            response,
            redirect_mode=redirect_mode,
            credential_scope=credential_scope,
            accept_status=accept_status,
        )
        if max_response_bytes is not None and len(response.body) > max_response_bytes:
            raise NativeCatalogError(
                "Catalog operation returned a response larger than its configured byte limit.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                status_code=response.status_code,
            )
        if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
            status_code, response_headers = _decode_redirect_response(owning_id, response)
            return status_code, response_headers, response
        if raw_text:
            return response.status_code, response.body, response
        if not response.body:
            return response.status_code, None, response
        invalid_payload = False
        try:
            payload = json.loads(response.body, parse_constant=_reject_nonfinite_json, parse_float=_finite_json_float)
        except (TypeError, ValueError):
            invalid_payload = True
            payload = None
        if invalid_payload:
            raise NativeCatalogError(
                _INVALID_JSON_RESULT,
                operation=str(owning_id),
                platform=PLATFORM.value,
                status_code=response.status_code,
                metadata={"ambiguous": method != "GET"},
            )
        return response.status_code, payload, response

    def _emit_dataset_failure(self, owning_id: OperationId, error: BaseException, *, emit_success: bool) -> None:
        if emit_success:
            self._emit(owning_id, "budget_exhausted" if isinstance(error, BudgetExhaustedError) else "failed")

    def _record_stream_response(self, owning_id: OperationId, key: CircuitKey, status_code: int) -> bool:
        before = self._breakers.inspect(key)
        if status_code >= 500:
            after = self._breakers.record_transport_failure(key)
        elif status_code >= 400:
            after = self._breakers.record_response(key, status_code)
        elif status_code >= 300:
            after = self._breakers.record_success(key)
        else:
            return False
        self._emit_breaker_change(owning_id, before.open, after.open)
        return True

    def _validate_page_params(self, operation: CatalogOperationRequest) -> dict[str, int]:
        unknown = set(operation.payload) - _PAGER_PARAMS
        if unknown:
            raise CatalogValidationError(
                f"The tracer dataset list accepts only {sorted(_PAGER_PARAMS)} parameters.",
                operation=str(operation.operation_id),
                platform=PLATFORM.value,
                safe_action="Pass optional positive-integer page and page_size values only.",
            )
        params: dict[str, int] = {}
        for key in _PAGER_PARAMS:
            value = operation.payload.get(key)
            if value is None:
                continue
            if type(value) is not int or value < 1:
                raise CatalogValidationError(
                    f"The uData dataset list parameter {key!r} must be a positive integer.",
                    operation=str(operation.operation_id),
                    platform=PLATFORM.value,
                    safe_action=f"Pass {key!r} as a positive integer.",
                )
            params[key] = value
        return params

    def _validate_status(
        self,
        owning_id: OperationId,
        response: RuntimeResponse | RuntimeStreamResponse | AsyncRuntimeStreamResponse,
        *,
        redirect_mode: bool = False,
        credential_scope: str = "anonymous",
        accept_status: Callable[[int], bool] | None = None,
    ) -> None:
        if owning_id.service == "oauth" and response.status_code in {404, 405}:
            raise UnsupportedCapabilityError(
                "The uData deployment does not expose this OAuth operation.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unsupported",
                metadata={"route_state": "absent", "api_version": "oauth", "status_code": response.status_code},
                safe_action="Use an OAuth operation exposed by this deployment.",
            )
        if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
            return
        if accept_status is not None and accept_status(response.status_code):
            return
        if 200 <= response.status_code < 300:
            return
        if response.status_code in {401, 403, 423}:
            self._capabilities.record_response(
                owning_id, _STATUS_RESPONSE_CLASSES[response.status_code], credential_scope=credential_scope
            )
        metadata = None
        if response.status_code == 401 and isinstance(response, RuntimeResponse):
            reason = _auth_failure_reason(response.body)
            if reason is not None:
                metadata = {"reason_code": reason}
        raise map_catalog_error(
            NativeCatalogError(
                "Catalog operation returned an unsuccessful HTTP status.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                status_code=response.status_code,
                retry_after=response.retry_after,
                metadata=metadata,
            )
        )

    def _decode(
        self, owning_id: OperationId, response: RuntimeResponse, *, credential_scope: str = "anonymous"
    ) -> ResultEnvelope[UDataResultItem]:
        if not 200 <= response.status_code < 300:
            if response.status_code in _STATUS_RESPONSE_CLASSES:
                self._capabilities.record_response(
                    owning_id, _STATUS_RESPONSE_CLASSES[response.status_code], credential_scope=credential_scope
                )
            raise map_catalog_error(
                NativeCatalogError(
                    "Catalog operation returned an unsuccessful HTTP status.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                    retry_after=response.retry_after,
                )
            )
        invalid_payload = False
        try:
            payload = json.loads(response.body, parse_constant=_reject_nonfinite_json, parse_float=_finite_json_float)
        except (TypeError, ValueError):
            invalid_payload = True
            payload = None
        if invalid_payload:
            raise NativeCatalogError(
                _INVALID_JSON_RESULT,
                operation=str(owning_id),
                platform=PLATFORM.value,
            )
        return shape_dataset_page(parse_native_page(payload, operation=str(owning_id)), operation=str(owning_id))

    def capability(self, operation_id: str) -> str:
        """Return the cached effective classification without dispatching transport I/O."""
        operation = _operation_id_from(operation_id)
        state = (
            self._capabilities.peek(operation, credential_scope=_credential_scope(self._credentials))
            .guard(operation)
            .state
        )
        return _capability_value(state)

    def invalidate(self, operation_id: str | OperationId | None = None) -> None:
        """Discard all effective capability state or one operation's state."""
        target = None
        if isinstance(operation_id, OperationId):
            target = operation_id
        elif operation_id is not None:
            target = _operation_id_from(operation_id)
        self._capabilities.invalidate(target)
        self._site_gate.invalidate()
        self._route_gate.invalidate()

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe pinned-profile metadata."""
        declared = self._profile.declared_profile
        return {"platform": next(iter(declared.operations)).platform, "profile_version": declared.profile_version}


class SyncUDataClient(_UDataClientCore):
    """Synchronous strict-version uData client: one anonymous probe, one dataset read."""

    def __init__(
        self,
        transport: CatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], None] = sleep,
        emitter: EventEmitter | None = None,
        owns_transport: bool = True,
        **options: Unpack[_SyncUDataClientOptions],
    ) -> None:
        """Build the shared sync core over the caller-owned or borrowed transport."""
        self._retry_sleep = retry_sleep
        settings = UDataClientSettings(base_url=origin, retry_sleep=retry_sleep, **options)
        if settings.breakers is None:
            settings = replace(
                settings,
                breakers=BreakerRegistry(
                    failure_threshold=breaker_failure_threshold,
                    cooldown=breaker_cooldown,
                    clock=clock,
                ),
            )
        super().__init__(
            transport,
            profile,
            settings,
            credentials,
            clock=clock,
            emitter=emitter,
            owns_transport=owns_transport,
        )

    def site_version(self) -> SiteVersion:
        """Run (or reuse) the anonymous exact-version site probe."""
        if self._closed:
            raise RuntimeError(_SYNC_UDATA_CLIENT_CLOSED)
        return self._require_site_version()

    @cached_property
    def datasets(self) -> _SyncDatasetsService:
        """Expose the complete typed dataset service."""
        return SyncDatasetsService(self)

    @cached_property
    def root_profile(self) -> _SyncRootProfileService:
        """Expose the complete typed root-profile service."""
        return SyncRootProfileService(self)

    @cached_property
    def resources(self) -> _SyncResourcesService:
        """Expose the complete typed resource service."""
        return SyncResourcesService(self)

    @cached_property
    def dataservices(self) -> _SyncDataservicesService:
        """Expose the complete typed dataservice and dataservice-follower service."""
        return SyncDataservicesService(self)

    @cached_property
    def reuses(self) -> _SyncReusesService:
        """Expose the complete typed reuse and reuse-follower service."""
        return SyncReusesService(self)

    @cached_property
    def posts_reports(self) -> _SyncPostsReportsService:
        """Expose the complete typed post, report, and notification service."""
        return SyncPostsReportsService(self)

    @cached_property
    def spatial(self) -> _SyncSpatialService:
        """Expose the complete typed spatial zone and coverage service."""
        return SyncSpatialService(self)

    @cached_property
    def taxonomies(self) -> _SyncTaxonomiesService:
        """Expose the complete typed taxonomy service."""
        return SyncTaxonomiesService(self)

    @cached_property
    def topics(self) -> _SyncTopicsService:
        """Expose the complete typed topic and topic-element service."""
        return SyncTopicsService(self)

    @cached_property
    def transfers(self) -> _SyncTransfersService:
        """Expose the complete typed dataset ownership-transfer service."""
        return SyncTransfersService(self)

    @cached_property
    def activity_discussions(self) -> _SyncActivityDiscussionsService:
        """Expose the complete typed activity and discussion service."""
        return SyncActivityDiscussionsService(self)

    @cached_property
    def organizations_memberships(self) -> _SyncOrganizationsMembershipsService:
        """Expose the complete typed organization and membership service."""
        return SyncOrganizationsMembershipsService(self)

    @cached_property
    def users_tokens(self) -> _SyncUsersTokensService:
        """Expose the typed user, invitation, and API-token service."""
        return SyncUsersTokensService(self)

    @cached_property
    def auth_oauth(self) -> _SyncAuthOAuthService:
        """Expose the typed OAuth and authentication service."""
        return SyncAuthOAuthService(self)

    @cached_property
    def harvest_moderation_admin(self) -> _SyncHarvestService:
        """Expose the typed harvest source, job, and bounded admin bulk service."""
        return SyncHarvestService(self)

    @cached_property
    def contact_visualization(self) -> _SyncContactVisualizationService:
        """Expose the typed contact-point and visualization service."""
        return SyncContactVisualizationService(self)

    @cached_property
    def worker_jobs(self) -> _SyncWorkerJobsService:
        """Expose the typed worker job, task, and budgeted task-wait service."""
        return SyncWorkerJobsService(self)

    @cached_property
    def extensions(self) -> _SyncExtensionsService:
        """Expose the typed stock extension and deployment-probe service."""
        return SyncExtensionsService(self)

    def _require_site_version(self, *, deadline: DeadlineMonitor | None = None) -> SiteVersion:
        gate = self._site_gate
        if isinstance(gate, SiteVersionGate):
            return gate.require_current(self._credentials, deadline=deadline)
        raise RuntimeError("The synchronous uData client requires a synchronous site gate.")

    def _admit_route(
        self, owning_id: OperationId, method: str, path: str, *, deadline: DeadlineMonitor | None = None
    ) -> None:
        self._require_route_operation(owning_id)
        observed = self._require_site_version(deadline=deadline)
        self._route_gate.require(method, path, str(owning_id), observed.version, deadline)

    def datasets_list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[UDataResultItem]:
        """Execute the single bounded dataset list read behind the version gate."""
        owning_id = _operation_id_from(_DATASETS_OPERATION_ID)
        return self._dispatch(operation, guard, owning_id=owning_id)

    def _send_dataset_attempt(
        self,
        attempt: _RequestAttempt,
        credential: object | None,
        json_body: object,
        controlled: bool,
        form_body: Callable[[], bytes] | None = None,
        _dispatch_sync: Callable[..., RuntimeResponse] = _controlled_dispatch_gate.dispatch_sync,
    ) -> RuntimeResponse:
        with attempt.dispatch(form_body) as request:
            if controlled:
                attempt.response = _dispatch_sync(
                    self, self._transport, request, credential, json_body, origin=self._origin
                )
            else:
                attempt.response = cast("CatalogTransport", self._transport).send(request)
        return cast("RuntimeResponse", attempt.response)

    def _dataset_call(
        self,
        *,
        method: str,
        path: str,
        owning_operation: str,
        headers: Mapping[str, str] | None = None,
        json_body: object = None,
        permissions: EffectivePermissions | None = None,
        credential: object | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
        max_response_bytes: int | None = 1_048_576,
        files: tuple[UploadPart, ...] = (),
        form_body: bytes | Callable[[], bytes] | None = None,
        **overrides: Unpack[_DatasetCallOverrides],
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one guarded dataset request scoped to its owning route operation."""
        owning_id, deadline = self._dataset_deadline(owning_operation, overrides, _SYNC_UDATA_CLIENT_CLOSED)
        self._admit_route(owning_id, method, path, deadline=deadline)
        if overrides.get("omit_credential", False):
            resolved_credential = None
        elif credential is not None:
            resolved_credential = credential
        else:
            resolved_credential = _refreshed_credential(self._credentials)
        scope = self._refresh_credential_scope(resolved_credential)
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        self._require_dispatchable(owning_id, effective, permissions)
        request = self._dataset_request(
            owning_id,
            deadline,
            method=method,
            path=path,
            headers=headers,
            credential=resolved_credential,
            json_body=json_body,
            form_body=form_body,
            files=files,
            idempotency_policy=idempotency_policy,
            max_response_bytes=max_response_bytes,
            overrides=overrides,
        )
        attempt = _RequestAttempt(self, owning_id, self._admit_request(owning_id, request), request)
        send = partial(
            self._send_dataset_attempt,
            attempt,
            resolved_credential,
            json_body,
            owning_operation == SET_SITE_OPERATION,
            form_body if callable(form_body) else None,
        )
        with self._dataset_retry(attempt, emit_success=overrides.get("emit_success", True)):
            response = RetryLoop(
                budget=self._budget,
                idempotency=_dataset_retry_idempotency(form_body, idempotency_policy, method, files),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._retry_sleep,
            ).run(send)
        return self._finish_dataset_call(owning_id, response, request, scope, overrides)

    def _root_call(
        self,
        *,
        method: str,
        path: str,
        owning_operation: str,
        headers: Mapping[str, str] | None = None,
        json_body: object = None,
        raw_text: bool = False,
        redirect_mode: bool = False,
        permissions: EffectivePermissions | None = None,
        credential: object | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
        max_response_bytes: int | None = None,
        emit_success: bool = True,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one root-profile request through the shared guarded transport seam."""
        return SyncUDataClient._dataset_call(
            self,
            method=method,
            path=path,
            owning_operation=owning_operation,
            headers=headers,
            json_body=json_body,
            raw_text=raw_text,
            redirect_mode=redirect_mode,
            permissions=permissions,
            credential=credential,
            idempotency_policy=idempotency_policy,
            max_response_bytes=max_response_bytes,
            emit_success=emit_success,
        )

    def _root_stream_call(
        self,
        *,
        path: str,
        owning_operation: str,
        headers: Mapping[str, str] | None = None,
        credential: object | None = None,
    ) -> RuntimeStreamResponse:
        """Open one guarded no-follow root response without buffering its bytes."""
        owning_id = _operation_id_from(owning_operation)
        try:
            if self._closed:
                raise RuntimeError(_SYNC_UDATA_CLIENT_CLOSED)
            deadline = DeadlineMonitor(self._budget, clock=self._clock)
            self._admit_route(owning_id, "GET", path, deadline=deadline)
            resolved_credential = credential if credential is not None else _refreshed_credential(self._credentials)
            scope = _credential_scope(resolved_credential)
            if scope != self._credential_scope:
                self._capabilities.invalidate()
                self._site_gate.invalidate()
                self._credential_scope = scope
            effective = self._capabilities.resolve(owning_id, credential_scope=scope)
            self._require_dispatchable(owning_id, effective)
            request, deadline, send_stream = _build_stream_request(
                owning_id=owning_id,
                origin=self._origin,
                path=path,
                headers=headers,
                resolved_credential=resolved_credential,
                deadline=deadline,
                transport=self._transport,
            )
            send = cast("Callable[[RuntimeRequest], RuntimeStreamResponse]", send_stream)
        except BudgetExhaustedError:
            self._emit(owning_id, "budget_exhausted")
            raise
        except SETTLEMENT_ERRORS as error:
            self._emit(owning_id, _stream_failure_outcome(error))
            raise
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                _CATALOG_ORIGIN_CIRCUIT_OPEN,
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action=_CATALOG_CIRCUIT_RETRY_ACTION,
            )
        settled = False
        try:
            before = self._breakers.inspect(key)
            try:
                response = cast("RuntimeStreamResponse", send(request))
            except TransportError:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                settled = True
                self._emit(owning_id, "failed")
                raise
            settled = self._record_stream_response(owning_id, key, response.status_code)
            redirected = self._settle_stream_redirect(owning_id, response, scope)
            if redirected is not None:
                return redirected
        finally:
            if not settled:
                self._breakers.release_trial(key)
        return _SyncStreamGuard(self, owning_id, key, response, deadline).response_wrapper()

    def _settle_stream_redirect(
        self, owning_id: OperationId, response: RuntimeStreamResponse, scope: str
    ) -> RuntimeStreamResponse | None:
        """Validate one streamed response; return it when a redirect short-circuits the read."""
        try:
            self._validate_status(owning_id, response, redirect_mode=True, credential_scope=scope)
            if response.status_code in {301, 302, 303, 307, 308}:
                _decode_redirect_response(owning_id, cast("RuntimeResponse", response))
                return response
        except SETTLEMENT_ERRORS as error:
            self._emit(owning_id, "failed")
            try:
                response.close()
            except Exception as cleanup_error:
                raise error from cleanup_error
            raise error
        return None

    def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        owning_id: OperationId,
    ) -> ResultEnvelope[UDataResultItem]:
        if self._closed:
            raise RuntimeError(_SYNC_UDATA_CLIENT_CLOSED)
        if operation.operation_id not in self._profile.declared_profile.operations:
            self._require_route_operation(operation.operation_id)
        _enforce_caller_guards(operation, guard)
        if operation.operation_id != owning_id:
            raise unimplemented_family(str(operation.operation_id))
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        self._require_site_version(deadline=deadline)
        params = self._validate_page_params(operation)
        request = _page_request(origin=self._origin, params=params)
        self._admit_route(owning_id, request.method, request.url, deadline=deadline)
        credential = _refreshed_credential(self._credentials)
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        self._require_dispatchable(owning_id, effective)
        if credential is not None:
            request = RuntimeRequest(
                method=request.method,
                url=request.url,
                headers=_auth_headers(credential),
                body=request.body,
                max_response_bytes=request.max_response_bytes,
            )
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        sync_transport = cast("CatalogTransport", self._transport)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                _CATALOG_ORIGIN_CIRCUIT_OPEN,
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action=_CATALOG_CIRCUIT_RETRY_ACTION,
            )
        attempts = 0

        recorded = False

        def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            nonlocal recorded
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = sync_transport.send(request)
            except TransportError:
                recorded = True
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            recorded = True
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            response = RetryLoop(
                budget=self._budget,
                idempotency=IdempotencyPolicy(safe=True),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._retry_sleep,
            ).run(send)
            result = self._decode(owning_id, response, credential_scope=scope)
        except BudgetExhaustedError:
            self._emit(
                owning_id,
                "budget_exhausted",
                budget_usage=max(0.0, self._budget.total - deadline.remaining()),
            )
            raise
        except Exception:
            self._emit(owning_id, "failed", retry_count=max(0, attempts - 1))
            raise
        finally:
            if not recorded:
                self._breakers.release_trial(key)
        self._emit(
            owning_id,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        return result

    def close(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            self._site_gate.invalidate()
            self._route_gate.invalidate()
            if self._owns_transport:
                cast("CatalogTransport", self._transport).close()

    def __enter__(self) -> Self:
        """Enter the client context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the client context."""
        self.close()


class AsyncUDataClient(_UDataClientCore):
    """Asynchronous strict-version uData client: one anonymous probe, one dataset read."""

    def __init__(
        self,
        transport: AsyncCatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        emitter: EventEmitter | None = None,
        owns_transport: bool = True,
        **options: Unpack[_AsyncUDataClientOptions],
    ) -> None:
        """Build the shared async core over the caller-owned or borrowed transport."""
        self._retry_sleep = retry_sleep
        settings = UDataClientSettings(base_url=origin, async_retry_sleep=retry_sleep, **options)
        if settings.breakers is None:
            settings = replace(
                settings,
                breakers=BreakerRegistry(
                    failure_threshold=breaker_failure_threshold,
                    cooldown=breaker_cooldown,
                    clock=clock,
                ),
            )
        super().__init__(
            transport,
            profile,
            settings,
            credentials,
            clock=clock,
            emitter=emitter,
            owns_transport=owns_transport,
            async_gate=True,
        )

    async def site_version(self, *, deadline: DeadlineMonitor | None = None) -> SiteVersion:
        """Run (or reuse) the anonymous exact-version site probe."""
        if self._closed:
            raise RuntimeError(_ASYNC_UDATA_CLIENT_CLOSED)
        gate = self._site_gate
        if isinstance(gate, AsyncSiteVersionGate):
            return await gate.require_current_async(self._credentials, deadline=deadline)
        raise RuntimeError("The asynchronous uData client requires an asynchronous site gate.")

    @cached_property
    def datasets(self) -> _AsyncDatasetsService:
        """Expose the complete typed dataset service."""
        return AsyncDatasetsService(self)

    @cached_property
    def root_profile(self) -> _AsyncRootProfileService:
        """Expose the complete typed root-profile service."""
        return AsyncRootProfileService(self)

    @cached_property
    def resources(self) -> _AsyncResourcesService:
        """Expose the complete typed resource service."""
        return AsyncResourcesService(self)

    @cached_property
    def dataservices(self) -> _AsyncDataservicesService:
        """Expose the complete typed dataservice and dataservice-follower service."""
        return AsyncDataservicesService(self)

    @cached_property
    def reuses(self) -> _AsyncReusesService:
        """Expose the complete typed reuse and reuse-follower service."""
        return AsyncReusesService(self)

    @cached_property
    def posts_reports(self) -> _AsyncPostsReportsService:
        """Expose the complete typed post, report, and notification service."""
        return AsyncPostsReportsService(self)

    @cached_property
    def spatial(self) -> _AsyncSpatialService:
        """Expose the complete typed spatial zone and coverage service."""
        return AsyncSpatialService(self)

    @cached_property
    def taxonomies(self) -> _AsyncTaxonomiesService:
        """Expose the complete typed taxonomy service."""
        return AsyncTaxonomiesService(self)

    @cached_property
    def topics(self) -> _AsyncTopicsService:
        """Expose the complete typed topic and topic-element service."""
        return AsyncTopicsService(self)

    @cached_property
    def transfers(self) -> _AsyncTransfersService:
        """Expose the complete typed dataset ownership-transfer service."""
        return AsyncTransfersService(self)

    @cached_property
    def activity_discussions(self) -> _AsyncActivityDiscussionsService:
        """Expose the complete typed activity and discussion service."""
        return AsyncActivityDiscussionsService(self)

    @cached_property
    def organizations_memberships(self) -> _AsyncOrganizationsMembershipsService:
        """Expose the complete typed organization and membership service."""
        return AsyncOrganizationsMembershipsService(self)

    @cached_property
    def users_tokens(self) -> _AsyncUsersTokensService:
        """Expose the typed user, invitation, and API-token service."""
        return AsyncUsersTokensService(self)

    @cached_property
    def auth_oauth(self) -> _AsyncAuthOAuthService:
        """Expose the typed OAuth and authentication service."""
        return AsyncAuthOAuthService(self)

    @cached_property
    def harvest_moderation_admin(self) -> _AsyncHarvestService:
        """Expose the typed harvest source, job, and bounded admin bulk service."""
        return AsyncHarvestService(self)

    @cached_property
    def contact_visualization(self) -> _AsyncContactVisualizationService:
        """Expose the typed contact-point and visualization service."""
        return AsyncContactVisualizationService(self)

    @cached_property
    def worker_jobs(self) -> _AsyncWorkerJobsService:
        """Expose the typed worker job, task, and budgeted task-wait service."""
        return AsyncWorkerJobsService(self)

    @cached_property
    def extensions(self) -> _AsyncExtensionsService:
        """Expose the typed stock extension and deployment-probe service."""
        return AsyncExtensionsService(self)

    async def _admit_route_async(
        self, owning_id: OperationId, method: str, path: str, *, deadline: DeadlineMonitor | None = None
    ) -> None:
        self._require_route_operation(owning_id)
        observed = await self.site_version(deadline=deadline)
        await self._route_gate.require_async(method, path, str(owning_id), observed.version, deadline)

    async def datasets_list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[UDataResultItem]:
        """Execute the single bounded dataset list read behind the version gate."""
        owning_id = _operation_id_from(_DATASETS_OPERATION_ID)
        return await self._dispatch(operation, guard, owning_id=owning_id)

    async def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        owning_id: OperationId,
    ) -> ResultEnvelope[UDataResultItem]:
        if self._closed:
            raise RuntimeError(_ASYNC_UDATA_CLIENT_CLOSED)
        if operation.operation_id not in self._profile.declared_profile.operations:
            self._require_route_operation(operation.operation_id)
        _enforce_caller_guards(operation, guard)
        if operation.operation_id != owning_id:
            raise unimplemented_family(str(operation.operation_id))
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        await self.site_version(deadline=deadline)
        params = self._validate_page_params(operation)
        request = _page_request(origin=self._origin, params=params)
        await self._admit_route_async(owning_id, request.method, request.url, deadline=deadline)
        credential = await _refreshed_credential_async(self._credentials)
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        self._require_dispatchable(owning_id, effective)
        if credential is not None:
            request = RuntimeRequest(
                method=request.method,
                url=request.url,
                headers=_auth_headers(credential),
                body=request.body,
                max_response_bytes=request.max_response_bytes,
            )
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        async_transport = cast("AsyncCatalogTransport", self._transport)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                _CATALOG_ORIGIN_CIRCUIT_OPEN,
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action=_CATALOG_CIRCUIT_RETRY_ACTION,
            )
        attempts = 0

        recorded = False

        async def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            nonlocal recorded
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = await async_transport.send(request)
            except TransportError:
                recorded = True
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            recorded = True
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            response = await RetryLoop(
                budget=self._budget,
                idempotency=IdempotencyPolicy(safe=True),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=lambda _: None,
            ).run_async(send, sleep=self._retry_sleep)
            result = self._decode(owning_id, response, credential_scope=scope)
        except BudgetExhaustedError:
            self._emit(
                owning_id,
                "budget_exhausted",
                budget_usage=max(0.0, self._budget.total - deadline.remaining()),
            )
            raise
        except Exception:
            self._emit(owning_id, "failed", retry_count=max(0, attempts - 1))
            raise
        finally:
            if not recorded:
                self._breakers.release_trial(key)
        self._emit(
            owning_id,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        return result

    async def _send_dataset_attempt(
        self,
        owning_id: OperationId,
        key: CircuitKey,
        request: RuntimeRequest,
        credential: object | None,
        json_body: object,
        controlled: bool,
        recorded: list[bool],
        form_body: Callable[[], bytes] | None = None,
        _dispatch_async: Callable[..., Awaitable[RuntimeResponse]] = _controlled_dispatch_gate.dispatch_async,
    ) -> RuntimeResponse:
        recorded[0] = False
        before = self._breakers.inspect(key)
        if form_body is not None:
            request = replace(request, body=form_body())
        if request.deadline is not None:
            request.deadline.assert_dispatchable()
        response = None
        try:
            if controlled:
                response = await _dispatch_async(
                    self, self._transport, request, credential, json_body, origin=self._origin
                )
            else:
                response = await cast("AsyncCatalogTransport", self._transport).send(request)
            if request.deadline is not None:
                request.deadline.assert_dispatchable()
        except BudgetExhaustedError as error:
            _settle_dataset_budget_error(error, request, response)
            raise
        except TransportError:
            recorded[0] = True
            after = self._breakers.record_transport_failure(key)
            self._emit_breaker_change(owning_id, before.open, after.open)
            raise
        after = self._breakers.record_response(key, response.status_code)
        recorded[0] = True
        self._emit_breaker_change(owning_id, before.open, after.open)
        return response

    async def _dataset_call_async(
        self,
        *,
        method: str,
        path: str,
        owning_operation: str,
        headers: Mapping[str, str] | None = None,
        json_body: object = None,
        permissions: EffectivePermissions | None = None,
        credential: object | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
        max_response_bytes: int | None = 1_048_576,
        files: tuple[UploadPart, ...] = (),
        form_body: bytes | Callable[[], bytes] | None = None,
        **overrides: Unpack[_DatasetCallOverrides],
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one guarded async dataset request scoped to its owning route operation."""
        unknown = set(overrides) - set(_DatasetCallOverrides.__annotations__)
        if unknown:
            raise TypeError(f"Unknown dataset call overrides: {sorted(unknown)}.")
        raw_text = overrides.get("raw_text", False)
        redirect_mode = overrides.get("redirect_mode", False)
        emit_success = overrides.get("emit_success", True)
        accept_status = overrides.get("accept_status")
        omit_credential = overrides.get("omit_credential", False)
        if max_response_bytes is None:
            max_response_bytes = 1_048_576
        if self._closed:
            raise RuntimeError(_ASYNC_UDATA_CLIENT_CLOSED)
        owning_id = _operation_id_from(owning_operation)
        deadline = overrides.get("deadline") or DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        await self._admit_route_async(owning_id, method, path, deadline=deadline)
        if omit_credential:
            resolved_credential = None
        elif credential is not None:
            resolved_credential = credential
        else:
            resolved_credential = await _refreshed_credential_async(self._credentials)
        scope = self._refresh_credential_scope(resolved_credential)
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        self._require_dispatchable(owning_id, effective, permissions)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        body = self._dataset_body(json_body, form_body, files, owning_id)
        request = RuntimeRequest(
            method=method,
            url=self._origin + path,
            headers=self._request_headers(
                headers,
                resolved_credential,
                idempotency_policy,
                body,
                json_media_fallback=form_body is None and not files,
            ),
            body=body,
            files=files,
            redirect_policy=_dataset_redirect_policy(redirect_mode, files),
            max_response_bytes=max_response_bytes,
            deadline=deadline,
        )
        key = self._admit_request(owning_id, request)
        recorded = [False]
        send = partial(
            self._send_dataset_attempt,
            owning_id,
            key,
            request,
            resolved_credential,
            json_body,
            owning_operation == SET_SITE_OPERATION,
            recorded,
            form_body if callable(form_body) else None,
        )
        try:
            response = await RetryLoop(
                budget=self._budget,
                idempotency=_dataset_retry_idempotency(form_body, idempotency_policy, method, files),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=lambda _: None,
            ).run_async(send, sleep=self._retry_sleep)
        except Exception as error:
            self._emit_dataset_failure(owning_id, error, emit_success=emit_success)
            raise
        finally:
            if not recorded[0]:
                self._breakers.release_trial(key)
        try:
            result = self._decode_dataset_response(
                owning_id,
                response,
                redirect_mode=redirect_mode,
                raw_text=raw_text,
                max_response_bytes=max_response_bytes,
                method=method,
                credential_scope=scope,
                accept_status=accept_status,
            )
        except Exception as error:
            self._emit_dataset_failure(owning_id, error, emit_success=emit_success)
            raise
        if emit_success:
            self._emit(owning_id, "succeeded")
        return result

    async def _root_call_async(
        self,
        *,
        method: str,
        path: str,
        owning_operation: str,
        headers: Mapping[str, str] | None = None,
        json_body: object = None,
        raw_text: bool = False,
        redirect_mode: bool = False,
        permissions: EffectivePermissions | None = None,
        credential: object | None = None,
        idempotency_policy: IdempotencyPolicy | None = None,
        max_response_bytes: int | None = None,
        emit_success: bool = True,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one async root-profile request through the shared transport seam."""
        return await AsyncUDataClient._dataset_call_async(
            self,
            method=method,
            path=path,
            owning_operation=owning_operation,
            headers=headers,
            json_body=json_body,
            raw_text=raw_text,
            redirect_mode=redirect_mode,
            permissions=permissions,
            credential=credential,
            idempotency_policy=idempotency_policy,
            max_response_bytes=max_response_bytes,
            emit_success=emit_success,
        )

    async def _root_stream_call_async(
        self,
        *,
        path: str,
        owning_operation: str,
        headers: Mapping[str, str] | None = None,
        credential: object | None = None,
    ) -> AsyncRuntimeStreamResponse:
        """Open one guarded no-follow async root response without buffering its bytes."""
        owning_id = _operation_id_from(owning_operation)
        try:
            if self._closed:
                raise RuntimeError(_ASYNC_UDATA_CLIENT_CLOSED)
            deadline = DeadlineMonitor(self._budget, clock=self._clock)
            await self._admit_route_async(owning_id, "GET", path, deadline=deadline)
            resolved_credential = (
                credential if credential is not None else await _refreshed_credential_async(self._credentials)
            )
            scope = _credential_scope(resolved_credential)
            if scope != self._credential_scope:
                self._capabilities.invalidate()
                self._site_gate.invalidate()
                self._credential_scope = scope
            effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
            self._require_dispatchable(owning_id, effective)
            request, deadline, send_stream = _build_stream_request(
                owning_id=owning_id,
                origin=self._origin,
                path=path,
                headers=headers,
                resolved_credential=resolved_credential,
                deadline=deadline,
                transport=self._transport,
            )
            send = cast("Callable[[RuntimeRequest], Awaitable[AsyncRuntimeStreamResponse]]", send_stream)
        except BudgetExhaustedError:
            self._emit(owning_id, "budget_exhausted")
            raise
        except ASYNC_SETTLEMENT_ERRORS as error:
            self._emit(owning_id, _stream_failure_outcome(error))
            raise
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                _CATALOG_ORIGIN_CIRCUIT_OPEN,
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action=_CATALOG_CIRCUIT_RETRY_ACTION,
            )
        settled = False
        try:
            before = self._breakers.inspect(key)
            try:
                response = cast("AsyncRuntimeStreamResponse", await send(request))
            except TransportError:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                settled = True
                self._emit(owning_id, "failed")
                raise
            settled = self._record_stream_response(owning_id, key, response.status_code)
            redirected = await self._asettle_stream_redirect(owning_id, response, scope)
            if redirected is not None:
                return redirected
        finally:
            if not settled:
                self._breakers.release_trial(key)
        return _AsyncStreamGuard(self, owning_id, key, response, deadline).response_wrapper()

    async def _asettle_stream_redirect(
        self, owning_id: OperationId, response: AsyncRuntimeStreamResponse, scope: str
    ) -> AsyncRuntimeStreamResponse | None:
        """Validate one streamed async response; return it when a redirect short-circuits the read."""
        try:
            self._validate_status(owning_id, response, redirect_mode=True, credential_scope=scope)
            if response.status_code in {301, 302, 303, 307, 308}:
                _decode_redirect_response(owning_id, cast("RuntimeResponse", response))
                return response
        except ASYNC_SETTLEMENT_ERRORS as error:
            self._emit(owning_id, "failed")
            try:
                await response.aclose()
            except (Exception, asyncio.CancelledError) as cleanup_error:
                raise error from cleanup_error
            raise error
        return None

    async def aclose(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            self._site_gate.invalidate()
            self._route_gate.invalidate()
            if self._owns_transport:
                await cast("AsyncCatalogTransport", self._transport).aclose()

    async def __aenter__(self) -> Self:
        """Enter the async client context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the async client context."""
        await self.aclose()


def create_sync_client(settings: UDataClientSettings) -> SyncUDataClient:
    """Construct one synchronous uData client from immutable settings."""
    require_extra("udata")
    override = settings.sync_transport
    if override is None:
        transport = create_default_sync_transport(tls_policy=settings.tls_policy, budget=settings.budget)
        owns_transport = True
    elif hasattr(override, "send"):
        transport = cast("CatalogTransport", override)
        owns_transport = False
    else:
        factory = cast("Callable[[], CatalogTransport]", override)
        transport = factory()
        owns_transport = True
    return SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.retry_sleep if settings.retry_sleep is not None else sleep,
        capability_cache_ttl=settings.capability_cache_ttl,
        root_export_max_bytes=settings.root_export_max_bytes,
        owns_transport=owns_transport,
        probe_runner=settings.probe_runner,
    )


def create_async_client(settings: UDataClientSettings) -> AsyncUDataClient:
    """Construct one asynchronous uData client from immutable settings."""
    require_extra("udata")
    transport: AsyncCatalogTransport
    override = settings.async_transport
    if override is None:
        transport = create_default_async_transport(tls_policy=settings.tls_policy, budget=settings.budget)
        owns_transport = True
    elif hasattr(override, "send"):
        transport = cast("AsyncCatalogTransport", override)
        owns_transport = False
    else:
        factory = cast("Callable[[], AsyncCatalogTransport]", override)
        transport = factory()
        owns_transport = True
    return AsyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.async_retry_sleep if settings.async_retry_sleep is not None else asyncio.sleep,
        capability_cache_ttl=settings.capability_cache_ttl,
        root_export_max_bytes=settings.root_export_max_bytes,
        owns_transport=owns_transport,
        async_probe_runner=settings.async_probe_runner,
    )


def _load_services():
    from datasluice.connectors.catalog.udata.services.activity_discussions import (
        AsyncActivityDiscussionsService,
        SyncActivityDiscussionsService,
    )
    from datasluice.connectors.catalog.udata.services.auth_oauth import AsyncAuthOAuthService, SyncAuthOAuthService
    from datasluice.connectors.catalog.udata.services.contact_visualization import (
        AsyncContactVisualizationService,
        SyncContactVisualizationService,
    )
    from datasluice.connectors.catalog.udata.services.dataservices import (
        AsyncDataservicesService,
        SyncDataservicesService,
    )
    from datasluice.connectors.catalog.udata.services.datasets import AsyncDatasetsService, SyncDatasetsService
    from datasluice.connectors.catalog.udata.services.extensions import (
        AsyncExtensionsService,
        SyncExtensionsService,
    )
    from datasluice.connectors.catalog.udata.services.harvest import (
        AsyncHarvestService,
        SyncHarvestService,
    )
    from datasluice.connectors.catalog.udata.services.jobs import (
        AsyncWorkerJobsService,
        SyncWorkerJobsService,
    )
    from datasluice.connectors.catalog.udata.services.organizations_memberships import (
        AsyncOrganizationsMembershipsService,
        SyncOrganizationsMembershipsService,
    )
    from datasluice.connectors.catalog.udata.services.posts_reports import (
        AsyncPostsReportsService,
        SyncPostsReportsService,
    )
    from datasluice.connectors.catalog.udata.services.resources import AsyncResourcesService, SyncResourcesService
    from datasluice.connectors.catalog.udata.services.reuses import AsyncReusesService, SyncReusesService
    from datasluice.connectors.catalog.udata.services.root_profile import (
        AsyncRootProfileService,
        SyncRootProfileService,
    )
    from datasluice.connectors.catalog.udata.services.spatial import (
        AsyncSpatialService,
        SyncSpatialService,
    )
    from datasluice.connectors.catalog.udata.services.taxonomies import (
        AsyncTaxonomiesService,
        SyncTaxonomiesService,
    )
    from datasluice.connectors.catalog.udata.services.topics import (
        AsyncTopicsService,
        SyncTopicsService,
    )
    from datasluice.connectors.catalog.udata.services.transfers import (
        AsyncTransfersService,
        SyncTransfersService,
    )
    from datasluice.connectors.catalog.udata.services.users_tokens import (
        AsyncUsersTokensService,
        SyncUsersTokensService,
    )

    return (
        AsyncActivityDiscussionsService,
        SyncActivityDiscussionsService,
        AsyncTaxonomiesService,
        SyncTaxonomiesService,
        AsyncDatasetsService,
        SyncDatasetsService,
        AsyncRootProfileService,
        SyncRootProfileService,
        AsyncResourcesService,
        SyncResourcesService,
        AsyncReusesService,
        SyncReusesService,
        AsyncDataservicesService,
        SyncDataservicesService,
        AsyncOrganizationsMembershipsService,
        SyncOrganizationsMembershipsService,
        AsyncPostsReportsService,
        SyncPostsReportsService,
        AsyncUsersTokensService,
        SyncUsersTokensService,
        AsyncSpatialService,
        SyncSpatialService,
        AsyncAuthOAuthService,
        SyncAuthOAuthService,
        AsyncContactVisualizationService,
        SyncContactVisualizationService,
        AsyncTransfersService,
        SyncTransfersService,
        AsyncTopicsService,
        SyncTopicsService,
        AsyncHarvestService,
        SyncHarvestService,
        AsyncWorkerJobsService,
        SyncWorkerJobsService,
        AsyncExtensionsService,
        SyncExtensionsService,
    )


(
    AsyncActivityDiscussionsService,
    SyncActivityDiscussionsService,
    AsyncTaxonomiesService,
    SyncTaxonomiesService,
    AsyncDatasetsService,
    SyncDatasetsService,
    AsyncRootProfileService,
    SyncRootProfileService,
    AsyncResourcesService,
    SyncResourcesService,
    AsyncReusesService,
    SyncReusesService,
    AsyncDataservicesService,
    SyncDataservicesService,
    AsyncOrganizationsMembershipsService,
    SyncOrganizationsMembershipsService,
    AsyncPostsReportsService,
    SyncPostsReportsService,
    AsyncUsersTokensService,
    SyncUsersTokensService,
    AsyncSpatialService,
    SyncSpatialService,
    AsyncAuthOAuthService,
    SyncAuthOAuthService,
    AsyncContactVisualizationService,
    SyncContactVisualizationService,
    AsyncTransfersService,
    SyncTransfersService,
    AsyncTopicsService,
    SyncTopicsService,
    AsyncHarvestService,
    SyncHarvestService,
    AsyncWorkerJobsService,
    SyncWorkerJobsService,
    AsyncExtensionsService,
    SyncExtensionsService,
) = _load_services()
