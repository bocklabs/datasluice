"""Transport-backed dual-surface sync and async CKAN Action API clients."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import date
from functools import lru_cache
from importlib import resources
from time import monotonic, sleep
from types import MappingProxyType, TracebackType
from typing import TYPE_CHECKING, Self, cast

from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS, ActionEntry, ActionInventory
from datasluice.connectors.catalog.ckan.mapping import parse_action_envelope, shape_result_envelope
from datasluice.connectors.catalog.ckan.rate_limits import resolve_rate_policy
from datasluice.connectors.catalog.ckan.results import require_mutation_tier
from datasluice.connectors.catalog.ckan.settings import CKANClientSettings, normalize_origin
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import (
    DatasetRecord,
    NativeRecord,
    OrganizationRecord,
    ResourceRecord,
    ResultEnvelope,
    _thaw_json,
)
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
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile, EffectiveCapabilityProfile, ProbeResponseClass
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogUnavailableError,
    CatalogValidationError,
    NativeCatalogError,
    UnsupportedCapabilityError,
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
    _circuit_key,
    _refreshed_credential,
    _refreshed_credential_async,
)
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
)
from datasluice.runtime.defaults import create_default_async_transport, create_default_sync_transport
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.extras import require_extra
from datasluice.runtime.resilience import BreakerRegistry, DeadlineMonitor, RetryLoop
from datasluice.runtime.transport.base import (
    CatalogTransport,
    RuntimeRequest,
    RuntimeResponse,
    TransportFailure,
    UploadPart,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.services.datasets import AsyncDatasetsService, SyncDatasetsService
    from datasluice.connectors.catalog.ckan.services.filestore import AsyncFilestoreService, SyncFilestoreService
    from datasluice.connectors.catalog.ckan.services.groups import AsyncGroupsService, SyncGroupsService
    from datasluice.connectors.catalog.ckan.services.organizations import (
        AsyncOrganizationsService,
        SyncOrganizationsService,
    )
    from datasluice.connectors.catalog.ckan.services.relationships_activity import (
        AsyncRelationshipsActivityService,
        SyncRelationshipsActivityService,
    )
    from datasluice.connectors.catalog.ckan.services.resources import AsyncResourcesService, SyncResourcesService
    from datasluice.connectors.catalog.ckan.services.users import AsyncUsersService, SyncUsersService
    from datasluice.connectors.catalog.ckan.services.views import AsyncViewsService, SyncViewsService
    from datasluice.connectors.catalog.ckan.services.vocabularies_licenses import (
        AsyncVocabulariesLicensesService,
        SyncVocabulariesLicensesService,
    )

PLATFORM = CatalogPlatform.CKAN
_ACTION_PATH = "/api/3/action/"
_PROFILE_RESOURCE = "ckan-2.11.json"
_STATUS_RESPONSE_CLASSES = {401: ProbeResponseClass.UNAUTHORIZED, 403: ProbeResponseClass.FORBIDDEN}

_NORMALIZED_BACKING: Mapping[tuple[str, str], str] = MappingProxyType(
    {
        ("datasets", "get"): "package_show",
        ("datasets", "list"): "current_package_list_with_resources",
        ("resources", "get"): "resource_show",
        ("resources", "list"): "resource_search",
        ("organizations", "get"): "organization_show",
        ("organizations", "list"): "organization_list_for_user",
    }
)


def _dispatch_request(
    *,
    origin: str,
    entry: ActionEntry,
    params: Mapping[str, object],
    credential: object,
    files: tuple[UploadPart, ...],
) -> RuntimeRequest:
    """Build one JSON or multipart Action API request for the dispatch pipeline."""
    url = f"{origin}{_ACTION_PATH}{entry.name}"
    headers = _auth_headers(credential)
    if files:
        return RuntimeRequest(
            method="POST",
            url=url,
            headers={"Content-Type": "multipart/form-data", **headers},
            body=None,
            files=files,
        )
    return RuntimeRequest(
        method="POST",
        url=url,
        headers={"Content-Type": "application/json", **headers},
        body=json.dumps(dict(params)).encode("utf-8"),
    )


def _operation_id_from(value: str) -> OperationId:
    """Derive the internal OperationId from one pinned profile identity string."""
    platform, _, tail = value.partition("/")
    service, dot, method = tail.partition(".")
    if not dot:
        return OperationId(platform=platform, service="native", method=tail)
    return OperationId(platform=platform, service=service, method=method)


@lru_cache(maxsize=1)
def declared_ckan_profile() -> DeclaredCapabilityProfile:
    """Load and pin the checked-in CKAN capability profile."""
    document = json.loads(
        resources.files("datasluice.contracts")
        .joinpath("catalog")
        .joinpath("profiles")
        .joinpath(_PROFILE_RESOURCE)
        .read_text(encoding="utf-8")
    )
    if document.get("platform") != PLATFORM.value:
        raise ValueError(f"The pinned {_PROFILE_RESOURCE} does not declare the CKAN platform.")
    operations: dict[OperationId, OperationSpec] = {}
    for item in document["operations"]:
        operation_id = _operation_id_from(item["id"])
        mutation = item["mutation"]
        operations[operation_id] = OperationSpec(
            id=operation_id,
            tier=OperationTier.NATIVE,
            request_type="ActionAPIRequest",
            response_type="ActionAPIResult",
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


class SyncCKANClient:
    """Synchronous dual-surface CKAN client: normalized projections plus native action groups."""

    def __init__(
        self,
        transport: CatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], None] = sleep,
        emitter: EventEmitter | None = None,
        probe_runner: ProbeRunner | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        owns_transport: bool = True,
        inventory: ActionInventory = CKAN_ACTIONS,
        probe_policy: str = "auto",
        rate_policy: object | None = None,
        max_upload_bytes: int | None = None,
    ) -> None:
        self._transport = transport
        self._owns_transport = owns_transport
        self._origin = normalize_origin(origin)
        self._capabilities = EffectiveCapabilityCache(
            profile,
            probe_runner=probe_runner,
            ttl_seconds=capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        self._probe_runner = probe_runner
        self._probe_policy = probe_policy
        self._credentials = credentials
        self._budget = budget or TimeBudget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._retry_sleep = retry_sleep
        self._emitter = emitter or EventEmitter()
        self._inventory = inventory
        self._rate_policy = rate_policy
        self._max_upload_bytes = max_upload_bytes
        self._closed = False

    @property
    def transport(self) -> CatalogTransport:
        """Expose the underlying transport as the public introspection seam."""
        return self._transport

    @property
    def credentials(self) -> object | None:
        """Expose the injected caller-owned credential resolver or provider."""
        return self._credentials

    @property
    def rate_policy(self) -> object | None:
        """Expose the portal-derived rate policy attached at construction."""
        return self._rate_policy

    @property
    def datasets(self) -> SyncDatasetsService:
        """Return the synchronous dataset projection carrying both surfaces."""
        from datasluice.connectors.catalog.ckan.services.datasets import SyncDatasetsService

        return SyncDatasetsService(self, "datasets", DatasetRecord.from_dict)

    @property
    def resources(self) -> SyncResourcesService:
        """Return the synchronous resource projection carrying both surfaces."""
        from datasluice.connectors.catalog.ckan.services.resources import SyncResourcesService

        return SyncResourcesService(self, "resources", ResourceRecord.from_dict)

    @property
    def organizations(self) -> SyncOrganizationsService:
        """Return the synchronous organization projection carrying both surfaces."""
        from datasluice.connectors.catalog.ckan.services.organizations import SyncOrganizationsService

        return SyncOrganizationsService(self, "organizations", OrganizationRecord.from_dict)

    @property
    def action_discovery(self) -> _SyncDiscoveryService:
        """Return the synchronous action-discovery group."""
        return _SyncDiscoveryService(self, "action_discovery")

    @property
    def groups(self) -> SyncGroupsService:
        """Return the synchronous native group group."""
        from datasluice.connectors.catalog.ckan.services.groups import SyncGroupsService

        return SyncGroupsService(self)

    @property
    def users(self) -> SyncUsersService:
        """Return the synchronous user group."""
        from datasluice.connectors.catalog.ckan.services.users import SyncUsersService

        return SyncUsersService(self)

    @property
    def vocabularies_licenses(self) -> SyncVocabulariesLicensesService:
        """Return the synchronous vocabulary and license group."""
        from datasluice.connectors.catalog.ckan.services.vocabularies_licenses import SyncVocabulariesLicensesService

        return SyncVocabulariesLicensesService(self)

    @property
    def relationships_activity(self) -> SyncRelationshipsActivityService:
        """Return the synchronous relationship and activity group."""
        from datasluice.connectors.catalog.ckan.services.relationships_activity import SyncRelationshipsActivityService

        return SyncRelationshipsActivityService(self)

    @property
    def views(self) -> SyncViewsService:
        """Return the synchronous resource-view group."""
        from datasluice.connectors.catalog.ckan.services.views import SyncViewsService

        return SyncViewsService(self)

    @property
    def datastore(self) -> _SyncNativeService:
        """Return the synchronous datastore group."""
        return _SyncNativeService(self, "datastore")

    @property
    def filestore(self) -> SyncFilestoreService:
        """Return the synchronous filestore projection routing to the resource paths."""
        from datasluice.connectors.catalog.ckan.services.filestore import SyncFilestoreService

        return SyncFilestoreService(self)

    @property
    def extensions(self) -> _SyncNativeService:
        """Return the synchronous extension-probe group."""
        return _SyncNativeService(self, "extensions")

    def _require_optional_evidence(self, owning_id: OperationId) -> None:
        if self._probe_policy != "auto":
            return
        declared = self._profile.declared_profile.operations.get(owning_id)
        if declared is None or declared.capability_class is not CapabilityClass.OPTIONAL:
            return
        if self._probe_runner is not None:
            return
        raise UnsupportedCapabilityError(
            f"{owning_id} is an optional CKAN capability and requires probe evidence before dispatch.",
            operation=str(owning_id),
            platform=PLATFORM.value,
            capability_state="optional",
            safe_action=(
                "Attach a probe runner (probe_runner= or async_probe_runner=) or select declared-baseline "
                "probing explicitly via CKANClientSettings(probe_policy='declared-baseline')."
            ),
        )

    def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        entry: ActionEntry,
        decoder: Callable[[CKANResultItem], object] | None = None,
        files: tuple[UploadPart, ...] = (),
    ) -> ResultEnvelope[object]:
        if self._closed:
            raise RuntimeError("The synchronous CKAN client is closed.")
        _enforce_caller_guards(operation, guard)
        owning_id = _operation_id_from(entry.owning_operation_id)
        self._require_optional_evidence(owning_id)
        effective = self._capabilities.resolve(owning_id)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        policy = require_mutation_tier(entry.mutation_class, owning_id, operation.mutation_policy)
        idempotency = (
            policy.idempotency if policy is not None else IdempotencyPolicy(safe=entry.mutation_class == "read")
        )
        credential = _refreshed_credential(self._credentials)
        params = {key: value for key, value in operation.payload.items() if key != "action"}
        request = _dispatch_request(
            origin=self._origin,
            entry=entry,
            params=params,
            credential=credential,
            files=files,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                "The catalog origin circuit is open after consecutive transport failures.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
            )

        attempts = 0
        recorded = False

        def send() -> RuntimeResponse:
            nonlocal attempts, recorded
            attempts += 1
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = self._transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                recorded = True
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            recorded = True
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            try:
                response = RetryLoop(
                    budget=self._budget,
                    idempotency=idempotency,
                    deadline=deadline,
                    max_attempts=self._max_attempts,
                    sleep=self._retry_sleep,
                ).run(send)
                result = self._decode(owning_id, entry, response)
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
                self._breakers.record_transport_failure(key)
        self._emit(
            owning_id,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        if decoder is not None:
            items = tuple(decoder(item) for item in result.items)
            return ResultEnvelope(items=cast("tuple[object]", items), page=result.page)
        return cast("ResultEnvelope[object]", result)

    def _decode(
        self, owning_id: OperationId, entry: ActionEntry, response: RuntimeResponse
    ) -> ResultEnvelope[CKANResultItem]:
        if not 200 <= response.status_code < 300:
            response_class = _STATUS_RESPONSE_CLASSES.get(response.status_code)
            if response_class is not None:
                self._capabilities.record_response(owning_id, response_class)
            raise map_catalog_error(
                NativeCatalogError(
                    "Catalog operation returned an unsuccessful HTTP status.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                    retry_after=response.retry_after,
                )
            )
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
                operation=str(owning_id),
                platform=PLATFORM.value,
            ) from exc
        bare = parse_action_envelope(payload, operation=str(owning_id), platform=PLATFORM)
        return shape_result_envelope(entry.name, bare)

    def _registry_entry(self, action: object, group: str, declared_operation_id: str) -> ActionEntry:
        if not isinstance(action, str) or not action:
            raise CatalogValidationError(
                "CKAN native operations require the manifest-registered action name in the operation payload.",
                operation=declared_operation_id,
                platform=PLATFORM.value,
                safe_action="Pass the registered action name under the 'action' payload key.",
            )
        entry = self._inventory.lookup(action)
        if entry.group != group or entry.owning_operation_id != declared_operation_id:
            raise CatalogValidationError(
                f"The action {action!r} is not registered under {declared_operation_id} in group {group!r}.",
                operation=declared_operation_id,
                platform=PLATFORM.value,
                safe_action="Dispatch the action through its owning native group projection.",
            )
        return entry

    def _emit(self, owning_id: OperationId, outcome: str, **metadata: object) -> None:
        self._emitter.record(
            operation_id=str(owning_id),
            platform=PLATFORM.value,
            outcome=outcome,
            metadata=metadata,
        )

    def _emit_breaker_change(self, owning_id: OperationId, before: bool, after: bool) -> None:
        if before != after:
            self._emit(owning_id, "breaker_state_change", breaker_open=after)

    def capability(self, operation_id: str) -> str:
        """Return the cached effective classification without dispatching transport I/O."""
        operation = _peek_operation_id(operation_id, self._profile)
        state = self._capabilities.peek(operation).guard(operation).state
        return _capability_value(state)

    def invalidate(self, operation_id: str | OperationId | None = None) -> None:
        """Discard all effective capability state or one operation's state."""
        target = None
        if isinstance(operation_id, OperationId):
            target = operation_id
        elif operation_id is not None:
            target = _peek_operation_id(operation_id, self._profile)
        self._capabilities.invalidate(target)

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe pinned-profile metadata."""
        declared = self._profile.declared_profile
        return {"platform": next(iter(declared.operations)).platform, "profile_version": declared.profile_version}

    def close(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            if self._owns_transport:
                self._transport.close()

    def __enter__(self) -> Self:
        """Enter the client context."""
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Close the client context."""
        self.close()


class AsyncCKANClient:
    """Asynchronous dual-surface CKAN client: normalized projections plus native action groups."""

    def __init__(
        self,
        transport: AsyncCatalogTransport,
        profile: DeclaredCapabilityProfile | EffectiveCapabilityProfile,
        *,
        origin: str,
        credentials: object | None = None,
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        emitter: EventEmitter | None = None,
        probe_runner: AsyncProbeRunner | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        owns_transport: bool = True,
        inventory: ActionInventory = CKAN_ACTIONS,
        probe_policy: str = "auto",
        rate_policy: object | None = None,
        max_upload_bytes: int | None = None,
    ) -> None:
        self._transport = transport
        self._owns_transport = owns_transport
        self._origin = normalize_origin(origin)
        self._capabilities = EffectiveCapabilityCache(
            profile,
            async_probe_runner=probe_runner,
            ttl_seconds=capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        self._probe_runner = probe_runner
        self._probe_policy = probe_policy
        self._credentials = credentials
        self._budget = budget or TimeBudget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        self._clock = clock
        self._retry_sleep = retry_sleep
        self._emitter = emitter or EventEmitter()
        self._inventory = inventory
        self._rate_policy = rate_policy
        self._max_upload_bytes = max_upload_bytes
        self._closed = False

    @property
    def transport(self) -> AsyncCatalogTransport:
        """Expose the underlying transport as the public introspection seam."""
        return self._transport

    @property
    def credentials(self) -> object | None:
        """Expose the injected caller-owned credential resolver or provider."""
        return self._credentials

    @property
    def rate_policy(self) -> object | None:
        """Expose the portal-derived rate policy attached at construction."""
        return self._rate_policy

    @property
    def datasets(self) -> AsyncDatasetsService:
        """Return the asynchronous dataset projection carrying both surfaces."""
        from datasluice.connectors.catalog.ckan.services.datasets import AsyncDatasetsService

        return AsyncDatasetsService(self, "datasets", DatasetRecord.from_dict)

    @property
    def resources(self) -> AsyncResourcesService:
        """Return the asynchronous resource projection carrying both surfaces."""
        from datasluice.connectors.catalog.ckan.services.resources import AsyncResourcesService

        return AsyncResourcesService(self, "resources", ResourceRecord.from_dict)

    @property
    def organizations(self) -> AsyncOrganizationsService:
        """Return the asynchronous organization projection carrying both surfaces."""
        from datasluice.connectors.catalog.ckan.services.organizations import AsyncOrganizationsService

        return AsyncOrganizationsService(self, "organizations", OrganizationRecord.from_dict)

    @property
    def action_discovery(self) -> _AsyncDiscoveryService:
        """Return the asynchronous action-discovery group."""
        return _AsyncDiscoveryService(self, "action_discovery")

    @property
    def groups(self) -> AsyncGroupsService:
        """Return the asynchronous native group group."""
        from datasluice.connectors.catalog.ckan.services.groups import AsyncGroupsService

        return AsyncGroupsService(self)

    @property
    def users(self) -> AsyncUsersService:
        """Return the asynchronous user group."""
        from datasluice.connectors.catalog.ckan.services.users import AsyncUsersService

        return AsyncUsersService(self)

    @property
    def vocabularies_licenses(self) -> AsyncVocabulariesLicensesService:
        """Return the asynchronous vocabulary and license group."""
        from datasluice.connectors.catalog.ckan.services.vocabularies_licenses import (
            AsyncVocabulariesLicensesService,
        )

        return AsyncVocabulariesLicensesService(self)

    @property
    def relationships_activity(self) -> AsyncRelationshipsActivityService:
        """Return the asynchronous relationship and activity group."""
        from datasluice.connectors.catalog.ckan.services.relationships_activity import (
            AsyncRelationshipsActivityService,
        )

        return AsyncRelationshipsActivityService(self)

    @property
    def views(self) -> AsyncViewsService:
        """Return the asynchronous resource-view group."""
        from datasluice.connectors.catalog.ckan.services.views import AsyncViewsService

        return AsyncViewsService(self)

    @property
    def datastore(self) -> _AsyncNativeService:
        """Return the asynchronous datastore group."""
        return _AsyncNativeService(self, "datastore")

    @property
    def filestore(self) -> AsyncFilestoreService:
        """Return the asynchronous filestore projection routing to the resource paths."""
        from datasluice.connectors.catalog.ckan.services.filestore import AsyncFilestoreService

        return AsyncFilestoreService(self)

    @property
    def extensions(self) -> _AsyncNativeService:
        """Return the asynchronous extension-probe group."""
        return _AsyncNativeService(self, "extensions")

    def _require_optional_evidence(self, owning_id: OperationId) -> None:
        if self._probe_policy != "auto":
            return
        declared = self._profile.declared_profile.operations.get(owning_id)
        if declared is None or declared.capability_class is not CapabilityClass.OPTIONAL:
            return
        if self._probe_runner is not None:
            return
        raise UnsupportedCapabilityError(
            f"{owning_id} is an optional CKAN capability and requires probe evidence before dispatch.",
            operation=str(owning_id),
            platform=PLATFORM.value,
            capability_state="optional",
            safe_action=(
                "Attach a probe runner (probe_runner= or async_probe_runner=) or select declared-baseline "
                "probing explicitly via CKANClientSettings(probe_policy='declared-baseline')."
            ),
        )

    async def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        entry: ActionEntry,
        decoder: Callable[[CKANResultItem], object] | None = None,
        files: tuple[UploadPart, ...] = (),
    ) -> ResultEnvelope[object]:
        if self._closed:
            raise RuntimeError("The asynchronous CKAN client is closed.")
        _enforce_caller_guards(operation, guard)
        owning_id = _operation_id_from(entry.owning_operation_id)
        self._require_optional_evidence(owning_id)
        effective = await self._capabilities.resolve_async(owning_id)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        policy = require_mutation_tier(entry.mutation_class, owning_id, operation.mutation_policy)
        idempotency = (
            policy.idempotency if policy is not None else IdempotencyPolicy(safe=entry.mutation_class == "read")
        )
        credential = await _refreshed_credential_async(self._credentials)
        params = {key: value for key, value in operation.payload.items() if key != "action"}
        request = _dispatch_request(
            origin=self._origin,
            entry=entry,
            params=params,
            credential=credential,
            files=files,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        key = _circuit_key(request, self._credentials)
        if not self._breakers.admit(key):
            self._emit(owning_id, "breaker_open")
            raise CatalogUnavailableError(
                "The catalog origin circuit is open after consecutive transport failures.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                capability_state="unavailable",
                safe_action="Wait for the circuit cool-down or explicitly reset the circuit before retrying.",
            )

        attempts = 0
        recorded = False

        async def send() -> RuntimeResponse:
            nonlocal attempts, recorded
            attempts += 1
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = await self._transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                recorded = True
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
            recorded = True
            self._emit_breaker_change(owning_id, before.open, after.open)
            return response

        try:
            try:
                response = await RetryLoop(
                    budget=self._budget,
                    idempotency=idempotency,
                    deadline=deadline,
                    max_attempts=self._max_attempts,
                    sleep=lambda _: None,
                ).run_async(send, sleep=self._retry_sleep)
                result = self._decode(owning_id, entry, response)
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
                self._breakers.record_transport_failure(key)
        self._emit(
            owning_id,
            "succeeded",
            retry_count=max(0, attempts - 1),
            budget_usage=max(0.0, self._budget.total - deadline.remaining()),
        )
        if decoder is not None:
            items = tuple(decoder(item) for item in result.items)
            return ResultEnvelope(items=cast("tuple[object]", items), page=result.page)
        return cast("ResultEnvelope[object]", result)

    def _decode(
        self, owning_id: OperationId, entry: ActionEntry, response: RuntimeResponse
    ) -> ResultEnvelope[CKANResultItem]:
        if not 200 <= response.status_code < 300:
            response_class = _STATUS_RESPONSE_CLASSES.get(response.status_code)
            if response_class is not None:
                self._capabilities.record_response(owning_id, response_class)
            raise map_catalog_error(
                NativeCatalogError(
                    "Catalog operation returned an unsuccessful HTTP status.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                    retry_after=response.retry_after,
                )
            )
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
                operation=str(owning_id),
                platform=PLATFORM.value,
            ) from exc
        bare = parse_action_envelope(payload, operation=str(owning_id), platform=PLATFORM)
        return shape_result_envelope(entry.name, bare)

    def _registry_entry(self, action: object, group: str, declared_operation_id: str) -> ActionEntry:
        if not isinstance(action, str) or not action:
            raise CatalogValidationError(
                "CKAN native operations require the manifest-registered action name in the operation payload.",
                operation=declared_operation_id,
                platform=PLATFORM.value,
                safe_action="Pass the registered action name under the 'action' payload key.",
            )
        entry = self._inventory.lookup(action)
        if entry.group != group or entry.owning_operation_id != declared_operation_id:
            raise CatalogValidationError(
                f"The action {action!r} is not registered under {declared_operation_id} in group {group!r}.",
                operation=declared_operation_id,
                platform=PLATFORM.value,
                safe_action="Dispatch the action through its owning native group projection.",
            )
        return entry

    def _emit(self, owning_id: OperationId, outcome: str, **metadata: object) -> None:
        self._emitter.record(
            operation_id=str(owning_id),
            platform=PLATFORM.value,
            outcome=outcome,
            metadata=metadata,
        )

    def _emit_breaker_change(self, owning_id: OperationId, before: bool, after: bool) -> None:
        if before != after:
            self._emit(owning_id, "breaker_state_change", breaker_open=after)

    def capability(self, operation_id: str) -> str:
        """Return the cached effective classification without dispatching transport I/O."""
        operation = _peek_operation_id(operation_id, self._profile)
        state = self._capabilities.peek(operation).guard(operation).state
        return _capability_value(state)

    def invalidate(self, operation_id: str | OperationId | None = None) -> None:
        """Discard all effective capability state or one operation's state."""
        target = None
        if isinstance(operation_id, OperationId):
            target = operation_id
        elif operation_id is not None:
            target = _peek_operation_id(operation_id, self._profile)
        self._capabilities.invalidate(target)

    def platform_metadata(self) -> Mapping[str, object]:
        """Return safe pinned-profile metadata."""
        declared = self._profile.declared_profile
        return {"platform": next(iter(declared.operations)).platform, "profile_version": declared.profile_version}

    async def aclose(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            if self._owns_transport:
                await self._transport.aclose()

    async def __aenter__(self) -> Self:
        """Enter the asynchronous client context."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Close the asynchronous client context."""
        await self.aclose()


class _SyncFamilyService[T]:
    """Synchronous dual-role family service: normalized decoding plus native umbrella dispatch."""

    __slots__ = ("_client", "_family", "_decoder")

    def __init__(self, client: SyncCKANClient, family: str, decoder: Callable[[object], T]) -> None:
        self._client = client
        self._family = family
        self._decoder = decoder

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""
        return NativeCatalogError

    def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[T]:
        """Dispatch a normalized get through the owning client."""
        return cast(
            ResultEnvelope[T],
            self._client._dispatch(
                operation,
                guard,
                entry=self._backing("get"),
                decoder=_normalized_decoder(self._family, self._decoder),
            ),
        )

    def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[T]:
        """Dispatch a normalized list through the owning client."""
        return cast(
            ResultEnvelope[T],
            self._client._dispatch(
                operation,
                guard,
                entry=self._backing("list"),
                decoder=_normalized_decoder(self._family, self._decoder),
            ),
        )

    def _invoke(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        entry = self._client._registry_entry(operation.payload.get("action"), self._family, str(operation.operation_id))
        return cast(ResultEnvelope[CKANResultItem], self._client._dispatch(operation, guard, entry=entry))

    def _backing(self, verb: str) -> ActionEntry:
        action = _NORMALIZED_BACKING[(self._family, verb)]
        entry = self._client._inventory.lookup(action)
        if entry.group != self._family or entry.mutation_class != "read":
            raise CatalogValidationError(
                f"The normalized backing action {action!r} must be a manifest-registered read.",
                operation=f"{PLATFORM.value}/{self._family}",
                platform=PLATFORM.value,
                safe_action="Keep the checked-in action manifest aligned with the normalized backing table.",
            )
        return entry


class _SyncDatasetService(_SyncFamilyService[DatasetRecord]):
    """Synchronous dataset projection satisfying the normalized and native contracts."""

    __slots__ = ()

    def list_show_search(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, show, or search datasets."""
        return self._invoke(operation, guard)

    def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Mutate or purge a dataset."""
        return self._invoke(operation, guard)


class _SyncResourceService(_SyncFamilyService[ResourceRecord]):
    """Synchronous resource projection satisfying the normalized and native contracts."""

    __slots__ = ()

    def list_show_create_update_patch_delete_upload(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, read, mutate, or upload a resource."""
        return self._invoke(operation, guard)


class _SyncOrganizationService(_SyncFamilyService[OrganizationRecord]):
    """Synchronous organization projection satisfying the normalized and native contracts."""

    __slots__ = ()

    def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage organizations and membership."""
        return self._invoke(operation, guard)


class _AsyncFamilyService[T]:
    """Asynchronous dual-role family service: normalized decoding plus native umbrella dispatch."""

    __slots__ = ("_client", "_family", "_decoder")

    def __init__(self, client: AsyncCKANClient, family: str, decoder: Callable[[object], T]) -> None:
        self._client = client
        self._family = family
        self._decoder = decoder

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""
        return NativeCatalogError

    async def get(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[T]:
        """Dispatch a normalized get through the owning client."""
        return cast(
            ResultEnvelope[T],
            await self._client._dispatch(
                operation,
                guard,
                entry=self._backing("get"),
                decoder=_normalized_decoder(self._family, self._decoder),
            ),
        )

    async def list(self, operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> ResultEnvelope[T]:
        """Dispatch a normalized list through the owning client."""
        return cast(
            ResultEnvelope[T],
            await self._client._dispatch(
                operation,
                guard,
                entry=self._backing("list"),
                decoder=_normalized_decoder(self._family, self._decoder),
            ),
        )

    async def _invoke(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        entry = self._client._registry_entry(operation.payload.get("action"), self._family, str(operation.operation_id))
        return cast(ResultEnvelope[CKANResultItem], await self._client._dispatch(operation, guard, entry=entry))

    def _backing(self, verb: str) -> ActionEntry:
        action = _NORMALIZED_BACKING[(self._family, verb)]
        entry = self._client._inventory.lookup(action)
        if entry.group != self._family or entry.mutation_class != "read":
            raise CatalogValidationError(
                f"The normalized backing action {action!r} must be a manifest-registered read.",
                operation=f"{PLATFORM.value}/{self._family}",
                platform=PLATFORM.value,
                safe_action="Keep the checked-in action manifest aligned with the normalized backing table.",
            )
        return entry


class _AsyncDatasetService(_AsyncFamilyService[DatasetRecord]):
    """Asynchronous dataset projection satisfying the normalized and native contracts."""

    __slots__ = ()

    async def list_show_search(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, show, or search datasets."""
        return await self._invoke(operation, guard)

    async def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Mutate or purge a dataset."""
        return await self._invoke(operation, guard)


class _AsyncResourceService(_AsyncFamilyService[ResourceRecord]):
    """Asynchronous resource projection satisfying the normalized and native contracts."""

    __slots__ = ()

    async def list_show_create_update_patch_delete_upload(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, read, mutate, or upload a resource."""
        return await self._invoke(operation, guard)


class _AsyncOrganizationService(_AsyncFamilyService[OrganizationRecord]):
    """Asynchronous organization projection satisfying the normalized and native contracts."""

    __slots__ = ()

    async def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage organizations and membership."""
        return await self._invoke(operation, guard)


class _SyncNativeService:
    """Synchronous native group projection validating every payload action against the registry."""

    __slots__ = ("_client", "_group")

    def __init__(self, client: SyncCKANClient, group: str) -> None:
        self._client = client
        self._group = group

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""
        return NativeCatalogError

    def discovery_help_and_status(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Probe Action API help and status."""
        return self._invoke(operation, guard)

    def list_show_search(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, show, or search datasets."""
        return self._invoke(operation, guard)

    def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Mutate or purge a dataset."""
        return self._invoke(operation, guard)

    def list_show_create_update_patch_delete_upload(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, read, mutate, or upload a resource."""
        return self._invoke(operation, guard)

    def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage organizations or groups and membership."""
        return self._invoke(operation, guard)

    def list_show_create_update_delete_token_management(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage users and API tokens."""
        return self._invoke(operation, guard)

    def tags_vocabularies_and_licenses(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Read or manage tags, vocabularies, and licenses."""
        return self._invoke(operation, guard)

    def relationships_followers_and_activity(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage relationships, followers, and activity."""
        return self._invoke(operation, guard)

    def resource_views(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage resource views."""
        return self._invoke(operation, guard)

    def query_and_record_crud(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Query or mutate datastore records."""
        return self._invoke(operation, guard)

    def upload_and_resource_file_replacement(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Upload or replace resource files."""
        return self._invoke(operation, guard)

    def extension_probes(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Probe deployment-provided actions and extensions."""
        return self._invoke(operation, guard)

    def _invoke(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        entry = self._client._registry_entry(operation.payload.get("action"), self._group, str(operation.operation_id))
        return cast(ResultEnvelope[CKANResultItem], self._client._dispatch(operation, guard, entry=entry))


class _SyncDiscoveryService(_SyncNativeService):
    """Synchronous action-discovery projection with typed help and status methods."""

    __slots__ = ()

    def status_show(self) -> ResultEnvelope[CKANResultItem]:
        """Return the deployment status mapping through the typed discovery method."""
        return self._invoke_typed("status_show", {})

    def help_show(self) -> ResultEnvelope[CKANResultItem]:
        """Return the Action API help value through the typed discovery method."""
        return self._invoke_typed("help_show", {})

    def _invoke_typed(self, action: str, payload: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._client._inventory.lookup(action)
        if entry.group != self._group:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {self._group!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=payload)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=self._client._profile)
        return cast(ResultEnvelope[CKANResultItem], self._client._dispatch(operation, guard, entry=entry))


class _AsyncNativeService:
    """Asynchronous native group projection validating every payload action against the registry."""

    __slots__ = ("_client", "_group")

    def __init__(self, client: AsyncCKANClient, group: str) -> None:
        self._client = client
        self._group = group

    @property
    def error_type(self) -> type[NativeCatalogError]:
        """Return the native CKAN error type."""
        return NativeCatalogError

    async def discovery_help_and_status(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Probe Action API help and status."""
        return await self._invoke(operation, guard)

    async def list_show_search(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, show, or search datasets."""
        return await self._invoke(operation, guard)

    async def create_update_patch_delete_purge(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Mutate or purge a dataset."""
        return await self._invoke(operation, guard)

    async def list_show_create_update_patch_delete_upload(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """List, read, mutate, or upload a resource."""
        return await self._invoke(operation, guard)

    async def list_show_create_update_delete_members(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage organizations or groups and membership."""
        return await self._invoke(operation, guard)

    async def list_show_create_update_delete_token_management(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage users and API tokens."""
        return await self._invoke(operation, guard)

    async def tags_vocabularies_and_licenses(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Read or manage tags, vocabularies, and licenses."""
        return await self._invoke(operation, guard)

    async def relationships_followers_and_activity(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage relationships, followers, and activity."""
        return await self._invoke(operation, guard)

    async def resource_views(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Manage resource views."""
        return await self._invoke(operation, guard)

    async def query_and_record_crud(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Query or mutate datastore records."""
        return await self._invoke(operation, guard)

    async def upload_and_resource_file_replacement(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Upload or replace resource files."""
        return await self._invoke(operation, guard)

    async def extension_probes(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Probe deployment-provided actions and extensions."""
        return await self._invoke(operation, guard)

    async def _invoke(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        entry = self._client._registry_entry(operation.payload.get("action"), self._group, str(operation.operation_id))
        return cast(ResultEnvelope[CKANResultItem], await self._client._dispatch(operation, guard, entry=entry))


class _AsyncDiscoveryService(_AsyncNativeService):
    """Asynchronous action-discovery projection with typed help and status methods."""

    __slots__ = ()

    async def status_show(self) -> ResultEnvelope[CKANResultItem]:
        """Return the deployment status mapping through the typed discovery method."""
        return await self._invoke_typed("status_show", {})

    async def help_show(self) -> ResultEnvelope[CKANResultItem]:
        """Return the Action API help value through the typed discovery method."""
        return await self._invoke_typed("help_show", {})

    async def _invoke_typed(self, action: str, payload: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._client._inventory.lookup(action)
        if entry.group != self._group:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {self._group!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=payload)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=self._client._profile)
        return cast(ResultEnvelope[CKANResultItem], await self._client._dispatch(operation, guard, entry=entry))


def _enforce_caller_guards(operation: CatalogOperationRequest, guard: CatalogOperationGuard) -> None:
    if guard.operation_id != operation.operation_id:
        raise ValueError(
            f"Catalog operation guard operation_id {guard.operation_id} does not match request operation_id "
            f"{operation.operation_id}."
        )
    guard.require_allowed()


def _normalized_decoder(family: str, record_decoder: Callable[[object], object]) -> Callable[[CKANResultItem], object]:
    converter = _CONVERTERS[family]

    def decode(item: CKANResultItem) -> object:
        return record_decoder(converter(item))

    return decode


def _native_of(item: CKANResultItem, kind: ResourceKind) -> NativeRecord:
    if isinstance(item, NativeRecord) and item.resource_kind is kind:
        return item
    raise NativeCatalogError(
        "The CKAN result carried no native record where a normalized record was expected.",
        operation=f"{PLATFORM.value}/{kind.value}",
        platform=PLATFORM.value,
    )


def _payload_of(record: NativeRecord) -> dict[str, object]:
    thawed = _thaw_json(record.payload)
    if not isinstance(thawed, dict):
        raise NativeCatalogError(
            "The CKAN native record payload was not an object.",
            operation=f"{PLATFORM.value}/{record.resource_kind.value}",
            platform=PLATFORM.value,
        )
    return thawed


def _text_or_none(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _dataset_envelope(item: CKANResultItem) -> dict[str, object]:
    record = _native_of(item, ResourceKind.DATASET)
    payload = _payload_of(record)
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        raise NativeCatalogError(
            "The CKAN package payload carries no usable dataset name.",
            operation=f"{PLATFORM.value}/datasets",
            platform=PLATFORM.value,
        )
    description = _text_or_none(payload.get("notes"))
    if description is None:
        description = _text_or_none(payload.get("description"))
    return {
        "schema_version": 1,
        "kind": "dataset",
        "id": record.id.to_dict(),
        "name": name,
        "description": description,
        "extensions": {},
    }


def _resource_envelope(item: CKANResultItem) -> dict[str, object]:
    record = _native_of(item, ResourceKind.RESOURCE)
    payload = _payload_of(record)
    package_id = payload.get("package_id")
    if not isinstance(package_id, str) or not package_id:
        raise NativeCatalogError(
            "The CKAN resource payload carries no package identifier.",
            operation=f"{PLATFORM.value}/resources",
            platform=PLATFORM.value,
        )
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        name = record.id.value
    return {
        "schema_version": 1,
        "kind": "resource",
        "id": record.id.to_dict(),
        "dataset_id": CatalogId(PLATFORM, ResourceKind.DATASET, package_id).to_dict(),
        "name": name,
        "url": _text_or_none(payload.get("url")),
        "extensions": {},
    }


def _organization_envelope(item: CKANResultItem) -> dict[str, object]:
    record = _native_of(item, ResourceKind.ORGANIZATION)
    payload = _payload_of(record)
    name = payload.get("name")
    if not isinstance(name, str) or not name:
        raise NativeCatalogError(
            "The CKAN organization payload carries no usable organization name.",
            operation=f"{PLATFORM.value}/organizations",
            platform=PLATFORM.value,
        )
    description = _text_or_none(payload.get("description"))
    if description is None:
        description = _text_or_none(payload.get("notes"))
    return {
        "schema_version": 1,
        "kind": "organization",
        "id": record.id.to_dict(),
        "name": name,
        "description": description,
        "extensions": {},
    }


_CONVERTERS: Mapping[str, Callable[[CKANResultItem], dict[str, object]]] = MappingProxyType(
    {
        "datasets": _dataset_envelope,
        "resources": _resource_envelope,
        "organizations": _organization_envelope,
    }
)


def _peek_operation_id(value: str, profile: EffectiveCapabilityProfile) -> OperationId:
    return next(
        (operation_id for operation_id in profile.capabilities if str(operation_id) == value),
        OperationId("unknown", "unknown", "unknown"),
    )


def _capability_value(state: object) -> str:
    available_states = {"core", "optional", "authenticated", "admin"}
    text = getattr(state, "value", state)
    return "available" if text in available_states else str(text)


def _default_probe_runners(
    settings: CKANClientSettings,
    transport: CatalogTransport | AsyncCatalogTransport,
) -> tuple[ProbeRunner | None, AsyncProbeRunner | None]:
    """Select default concrete probe runners for HTTPS origins when settings supply none.

    Explicit settings overrides always win. Non-HTTPS origins refuse under the
    auto policy: controlled loopback stacks opt into evidence through their own
    runners or the declared-baseline policy, never a silent validator bypass
    (D-07 completion; review Plan 06 MEDIUM closed). Construction performs no
    network I/O — runners share the factory-owned transport lazily.
    """
    if settings.probe_runner is not None or settings.async_probe_runner is not None:
        return settings.probe_runner, settings.async_probe_runner
    if settings.probe_policy != "auto":
        return None, None
    origin = normalize_origin(settings.base_url)
    if not origin.startswith("https://"):
        raise UnsupportedCapabilityError(
            f"The non-HTTPS origin {origin} receives no default probe runners.",
            operation=f"{PLATFORM.value}/probes",
            platform=PLATFORM.value,
            capability_state="optional",
            safe_action=(
                "Attach explicit probe runners for the controlled stack or select "
                "CKANClientSettings(probe_policy='declared-baseline') to consume the declared profile trustingly."
            ),
        )
    from datasluice.connectors.catalog.ckan.probes import CKANAsyncProbeRunner, CKANProbeRunner

    profile = declared_ckan_profile()
    if inspect.iscoroutinefunction(getattr(transport, "send", None)):
        return None, CKANAsyncProbeRunner(transport, origin, profile)  # ty: ignore[invalid-argument-type]
    return CKANProbeRunner(transport, origin, profile), None  # ty: ignore[invalid-argument-type]


def create_sync_client(settings: CKANClientSettings) -> SyncCKANClient:
    """Construct one synchronous CKAN client from immutable settings."""
    require_extra("ckan")
    override = settings.sync_transport
    if override is None:
        transport = create_default_sync_transport(tls_policy=settings.tls_policy, budget=settings.budget)
        owns_transport = True
    elif hasattr(override, "send"):
        transport = cast(CatalogTransport, override)
        owns_transport = False
    else:
        factory = cast("Callable[[], CatalogTransport]", override)
        transport = factory()
        owns_transport = True
    sync_probe_runner, _ = _default_probe_runners(settings, transport)
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.retry_sleep if settings.retry_sleep is not None else sleep,
        probe_runner=sync_probe_runner,
        capability_cache_ttl=settings.capability_cache_ttl,
        owns_transport=owns_transport,
        rate_policy=resolve_rate_policy(settings),
        probe_policy=settings.probe_policy,
        max_upload_bytes=settings.max_upload_bytes,
    )


def create_async_client(settings: CKANClientSettings) -> AsyncCKANClient:
    """Construct one asynchronous CKAN client from immutable settings."""
    require_extra("ckan")
    override = settings.async_transport
    if override is None:
        transport = create_default_async_transport(tls_policy=settings.tls_policy, budget=settings.budget)
        owns_transport = True
    elif hasattr(override, "send"):
        transport = cast(AsyncCatalogTransport, override)
        owns_transport = False
    else:
        factory = cast("Callable[[], AsyncCatalogTransport]", override)
        transport = factory()
        owns_transport = True
    _, async_probe_runner = _default_probe_runners(settings, transport)
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=settings.base_url,
        credentials=settings.credential,
        budget=settings.budget,
        breakers=settings.breakers,
        max_attempts=settings.max_attempts,
        retry_sleep=settings.async_retry_sleep if settings.async_retry_sleep is not None else asyncio.sleep,
        probe_runner=async_probe_runner,
        capability_cache_ttl=settings.capability_cache_ttl,
        owns_transport=owns_transport,
        rate_policy=resolve_rate_policy(settings),
        probe_policy=settings.probe_policy,
        max_upload_bytes=settings.max_upload_bytes,
    )
