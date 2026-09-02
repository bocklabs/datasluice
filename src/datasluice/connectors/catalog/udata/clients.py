"""Transport-backed strict-version sync and async uData clients."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import select
import signal
import weakref
from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from dataclasses import dataclass, field
from datetime import date
from functools import lru_cache
from importlib import resources
from pathlib import Path
from time import monotonic, sleep
from types import TracebackType
from typing import Self, cast

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
from datasluice.connectors.catalog.udata.services.datasets import AsyncDatasetsService, SyncDatasetsService
from datasluice.connectors.catalog.udata.services.root_profile import AsyncRootProfileService, SyncRootProfileService
from datasluice.connectors.catalog.udata.settings import UDataClientSettings, normalize_origin
from datasluice.contracts.catalog.native.udata import UDataResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.auth import credential_scope as _credential_scope
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.observability import TLSPolicy
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
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.domain.catalog.safety import IdempotencyPolicy
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogUnavailableError,
    CatalogValidationError,
    NativeCatalogError,
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
    AsyncStreamingCatalogTransport,
    _auth_headers,
    _capability_value,
    _circuit_key,
    _refreshed_credential,
    _refreshed_credential_async,
)
from datasluice.runtime.constants import (
    DEFAULT_BREAKER_COOLDOWN_SECONDS,
    DEFAULT_BREAKER_FAILURE_THRESHOLD,
    DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
    DEFAULT_ROOT_EXPORT_MAX_BYTES,
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
    StreamingCatalogTransport,
    TransportFailure,
)

_PROFILE_RESOURCE = "udata-17.6.json"
_PAGER_PARAMS = frozenset({"page", "page_size"})
_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"
_CONTROLLED_SOURCE_COMMIT = "0546582058d84706812a1c37387576efc4e5ad1f"
_CONTROLLED_COMPOSE_SHA256 = "f7acbcd1ea2f88f7b9361cbfadbd46e62be82bd707538f22f0615796e8bf09a3"
_CONTROLLED_DOCKERFILE_SHA256 = "6c21f02c3a287f1c1a2b42db392e767a484792bb763827a65bce5fcdd0d97e3b"
_CONTROLLED_IMAGE_DIGESTS = (
    "mongo@sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47",
    "redis@sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256",
    "elasticsearch/elasticsearch@sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
    "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e",
    "axllent/mailpit@sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf",
)
_CONTROLLED_EVIDENCE_ROOT = Path(__file__).resolve().parents[5] / "dev" / "udata-evidence"
_CONTROLLED_COMPOSE_FILE = _CONTROLLED_EVIDENCE_ROOT / "compose.yaml"
_CONTROLLED_ENV_FILE = _CONTROLLED_EVIDENCE_ROOT / ".env"
_CONTROLLED_AUTHORITY_TTL_SECONDS = 60.0
_CONTROLLED_COMMAND_TIMEOUT_SECONDS = 15.0
_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES = 65536

_STATUS_RESPONSE_CLASSES = {
    401: ProbeResponseClass.UNAUTHORIZED,
    403: ProbeResponseClass.FORBIDDEN,
    423: ProbeResponseClass.DEPLOYMENT_DISABLED,
}


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
    return RuntimeRequest(method="GET", url=url, headers={}, body=None)


@dataclass(frozen=True, slots=True)
class _ControlledStackEvidence:
    """Bounded non-secret facts observed from the controlled stack."""

    nonce_sha256: str = field(repr=False)
    site_id: str

    @property
    def digest(self) -> str:
        """Return a stable digest without exposing the stack nonce."""
        return hashlib.sha256(
            "|".join(
                (
                    _CONTROLLED_SOURCE_COMMIT,
                    _CONTROLLED_COMPOSE_SHA256,
                    _CONTROLLED_DOCKERFILE_SHA256,
                    *_CONTROLLED_IMAGE_DIGESTS,
                    self.nonce_sha256,
                    self.site_id,
                )
            ).encode()
        ).hexdigest()


def _controlled_error(message: str) -> CatalogValidationError:
    return CatalogValidationError(
        message,
        operation="udata/api-v1.set_site",
        platform=PLATFORM.value,
        safe_action="Use the verified disposable uData evidence stack.",
    )


def _compose_read(*args: str) -> str:
    if not _CONTROLLED_COMPOSE_FILE.is_file() or not _CONTROLLED_ENV_FILE.is_file():
        raise _controlled_error("The controlled uData evidence configuration is unavailable.")
    command = [
        "docker",
        "compose",
        "--env-file",
        str(_CONTROLLED_ENV_FILE),
        "-f",
        str(_CONTROLLED_COMPOSE_FILE),
        *args,
    ]
    read_fd, write_fd = os.pipe()
    process_id: int | None = None
    try:
        file_actions = (
            (os.POSIX_SPAWN_DUP2, write_fd, 1),
            (os.POSIX_SPAWN_DUP2, write_fd, 2),
            (os.POSIX_SPAWN_CLOSE, read_fd),
            (os.POSIX_SPAWN_CLOSE, write_fd),
        )
        environment = {key: os.environ[key] for key in ("PATH", "DOCKER_HOST", "DOCKER_CONTEXT") if key in os.environ}
        process_id = os.posix_spawnp(command[0], command, environment, file_actions=file_actions)
        os.close(write_fd)
        write_fd = -1
        deadline = monotonic() + _CONTROLLED_COMMAND_TIMEOUT_SECONDS
        chunks: list[bytes] = []
        output_size = 0
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0 or not select.select([read_fd], [], [], remaining)[0]:
                os.kill(process_id, signal.SIGKILL)
                os.waitpid(process_id, 0)
                raise _controlled_error("The controlled uData stack identity check timed out.")
            chunk = os.read(read_fd, 65536)
            if not chunk:
                break
            output_size += len(chunk)
            if output_size > _CONTROLLED_COMMAND_MAX_OUTPUT_BYTES:
                os.kill(process_id, signal.SIGKILL)
                os.waitpid(process_id, 0)
                raise _controlled_error("The controlled uData stack identity check returned too much output.")
            chunks.append(chunk)
        while True:
            completed_pid, wait_status = os.waitpid(process_id, os.WNOHANG)
            if completed_pid == process_id:
                break
            remaining = deadline - monotonic()
            if remaining <= 0:
                os.kill(process_id, signal.SIGKILL)
                os.waitpid(process_id, 0)
                raise _controlled_error("The controlled uData stack identity check timed out.")
            select.select([], [], [], min(remaining, 0.05))
    except (AttributeError, OSError) as error:
        if process_id is not None:
            try:
                os.kill(process_id, signal.SIGKILL)
            except OSError:
                pass
            try:
                os.waitpid(process_id, 0)
            except OSError:
                pass
        raise _controlled_error("The controlled uData stack cannot be verified.") from error
    finally:
        os.close(read_fd)
        if write_fd >= 0:
            os.close(write_fd)
    if os.waitstatus_to_exitcode(wait_status) != 0:
        raise _controlled_error("The controlled uData stack identity check failed.")
    try:
        return b"".join(chunks).decode().strip()
    except UnicodeDecodeError:
        raise _controlled_error("The controlled uData stack identity check returned invalid output.") from None


def _verify_controlled_source_and_nonce() -> str:
    nonce = os.environ.get("UDATA_EVIDENCE_STACK_NONCE")
    if not nonce:
        raise _controlled_error("The controlled uData stack nonce is unavailable.")
    try:
        compose_text = _CONTROLLED_COMPOSE_FILE.read_text(encoding="utf-8")
        dockerfile_text = _CONTROLLED_EVIDENCE_ROOT.joinpath("Dockerfile").read_text(encoding="utf-8")
    except OSError as error:
        raise _controlled_error("The controlled uData source identity is unavailable.") from error
    if hashlib.sha256(compose_text.encode()).hexdigest() != _CONTROLLED_COMPOSE_SHA256:
        raise _controlled_error("The controlled uData compose identity does not match the approved source.")
    if hashlib.sha256(dockerfile_text.encode()).hexdigest() != _CONTROLLED_DOCKERFILE_SHA256:
        raise _controlled_error("The controlled uData image identity does not match the approved source.")
    services = _compose_read("ps", "--status", "running", "--services").splitlines()
    expected_services = {"udata", "mongo", "redis", "search", "storage", "mailpit"}
    if len(services) != len(expected_services) or set(services) != expected_services:
        raise _controlled_error("The running services do not match the controlled uData stack.")
    if _compose_read("port", "udata", "7000") != "127.0.0.1:5640":
        raise _controlled_error("The controlled uData service is not bound to the approved loopback port.")
    if (
        _compose_read("exec", "-T", "udata", "git", "-C", "/opt/udata", "rev-parse", "HEAD")
        != _CONTROLLED_SOURCE_COMMIT
    ):
        raise _controlled_error("The controlled uData service source does not match the approved commit.")
    if _compose_read("exec", "-T", "udata", "printenv", "UDATA_EVIDENCE_STACK_NONCE") != nonce:
        raise _controlled_error("The controlled uData service nonce does not match the local evidence nonce.")
    return hashlib.sha256(nonce.encode()).hexdigest()


def _controlled_peer_evidence(response: RuntimeResponse) -> str:
    content_type = _response_header(response, "content-type")
    if (
        not 200 <= response.status_code < 300
        or content_type is None
        or content_type.split(";", 1)[0] != "application/json"
    ):
        raise _controlled_error("The controlled uData peer did not return its expected site identity.")
    try:
        payload = json.loads(response.body)
    except (TypeError, ValueError):
        raise _controlled_error("The controlled uData peer returned an invalid site identity.") from None
    if (
        not isinstance(payload, Mapping)
        or not isinstance(payload.get("id"), str)
        or not payload["id"]
        or payload.get("version") != "17.6.0"
    ):
        raise _controlled_error("The controlled uData peer did not match the seeded site identity.")
    return payload["id"]


def _verify_controlled_sync_stack(transport: CatalogTransport) -> _ControlledStackEvidence:
    nonce_sha256 = _verify_controlled_source_and_nonce()
    response = transport.send(
        RuntimeRequest(
            method="GET",
            url=f"{_CONTROLLED_ORIGIN}/api/1/site/",
            headers={},
            redirect_policy=RedirectPolicy.NO_FOLLOW,
            max_response_bytes=8192,
        )
    )
    return _ControlledStackEvidence(nonce_sha256=nonce_sha256, site_id=_controlled_peer_evidence(response))


async def _verify_controlled_async_stack(transport: AsyncCatalogTransport) -> _ControlledStackEvidence:
    nonce_sha256 = _verify_controlled_source_and_nonce()
    response = await transport.send(
        RuntimeRequest(
            method="GET",
            url=f"{_CONTROLLED_ORIGIN}/api/1/site/",
            headers={},
            redirect_policy=RedirectPolicy.NO_FOLLOW,
            max_response_bytes=8192,
        )
    )
    return _ControlledStackEvidence(nonce_sha256=nonce_sha256, site_id=_controlled_peer_evidence(response))


def _build_controlled_transport_types():
    sync_builder = create_default_sync_transport
    sync_verifier = _verify_controlled_sync_stack
    async_builder = create_default_async_transport
    async_verifier = _verify_controlled_async_stack
    type SyncState = tuple[CatalogTransport, _ControlledStackEvidence, float]
    type AsyncState = tuple[AsyncCatalogTransport, _ControlledStackEvidence | None, float]
    sync_registry: weakref.WeakKeyDictionary[object, SyncState] = weakref.WeakKeyDictionary()
    async_registry: weakref.WeakKeyDictionary[object, AsyncState] = weakref.WeakKeyDictionary()

    def sync_state(value: object) -> SyncState | None:
        try:
            return sync_registry.get(value)
        except TypeError:
            return None

    def async_state(value: object) -> AsyncState | None:
        try:
            return async_registry.get(value)
        except TypeError:
            return None

    class _ControlledSyncTransport:
        """Own a stock transport after live controlled-stack verification."""

        __slots__ = ("__weakref__",)

        def __init__(self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None) -> None:
            transport = sync_builder(tls_policy=tls_policy, budget=budget)
            try:
                evidence = sync_verifier(transport)
            except BaseException:
                transport.close()
                raise
            sync_registry[self] = (transport, evidence, monotonic())

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            state = sync_state(self)
            if state is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            return state[0].send(request)

        def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
            state = sync_state(self)
            if state is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            return cast(StreamingCatalogTransport, state[0]).send_stream(request)

        def close(self) -> None:
            state = sync_state(self)
            if state is not None:
                state[0].close()

        def revalidate(self, *, origin: str, site_id: str) -> bool:
            """Revalidate this exact binding immediately before a controlled PATCH."""
            state = sync_state(self)
            if state is None:
                return False
            evidence = sync_verifier(state[0])
            return (
                origin == _CONTROLLED_ORIGIN
                and monotonic() - state[2] <= _CONTROLLED_AUTHORITY_TTL_SECONDS
                and evidence == state[1]
                and evidence.site_id == site_id
            )

        @property
        def evidence_digest(self) -> str:
            """Return only the redacted controlled-stack evidence digest."""
            state = sync_state(self)
            return state[1].digest if state is not None else ""

        @property
        def site_id(self) -> str:
            """Return the peer identity bound during the initial live verification."""
            state = sync_state(self)
            return state[1].site_id if state is not None else ""

    class _ControlledAsyncTransport:
        """Own a stock asynchronous transport after live controlled-stack verification."""

        __slots__ = ("__weakref__",)

        def __init__(self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None) -> None:
            async_registry[self] = (async_builder(tls_policy=tls_policy, budget=budget), None, 0.0)

        async def verify(self) -> None:
            """Complete live verification before the transport is used for mutations."""
            state = async_state(self)
            if state is None:
                return
            try:
                evidence = await async_verifier(state[0])
            except BaseException:
                await state[0].aclose()
                async_registry.pop(self, None)
                raise
            async_registry[self] = (state[0], evidence, monotonic())

        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            state = async_state(self)
            if state is None or state[1] is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            return await state[0].send(request)

        async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
            state = async_state(self)
            if state is None or state[1] is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            return await cast(AsyncStreamingCatalogTransport, state[0]).send_stream(request)

        async def aclose(self) -> None:
            state = async_state(self)
            if state is not None:
                await state[0].aclose()

        async def revalidate(self, *, origin: str, site_id: str) -> bool:
            """Revalidate this exact binding immediately before a controlled PATCH."""
            state = async_state(self)
            if state is None or state[1] is None:
                return False
            evidence = await async_verifier(state[0])
            return (
                origin == _CONTROLLED_ORIGIN
                and monotonic() - state[2] <= _CONTROLLED_AUTHORITY_TTL_SECONDS
                and evidence == state[1]
                and evidence.site_id == site_id
            )

        @property
        def evidence_digest(self) -> str:
            """Return only the redacted controlled-stack evidence digest."""
            state = async_state(self)
            return state[1].digest if state is not None and state[1] is not None else ""

        @property
        def site_id(self) -> str:
            """Return the peer identity bound during the initial live verification."""
            state = async_state(self)
            return state[1].site_id if state is not None and state[1] is not None else ""

    return _ControlledSyncTransport, _ControlledAsyncTransport


_ControlledSyncTransport, _ControlledAsyncTransport = _build_controlled_transport_types()


class _UDataClientCore:
    """Shared strict-gate state for the sync and async uData clients."""

    def __init__(
        self,
        transport: CatalogTransport | AsyncCatalogTransport,
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
        emitter: EventEmitter | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        root_export_max_bytes: int = DEFAULT_ROOT_EXPORT_MAX_BYTES,
        owns_transport: bool = True,
        site_gate: SiteVersionGate | AsyncSiteVersionGate | None = None,
        probe_runner: ProbeRunner | None = None,
        async_probe_runner: AsyncProbeRunner | None = None,
        async_gate: bool = False,
    ) -> None:
        self._transport = transport
        self._credential_scope = _credential_scope(credentials)
        self._owns_transport = owns_transport
        self._origin = normalize_origin(origin)
        checked_origin = self._origin
        self._capabilities = EffectiveCapabilityCache(
            profile,
            probe_runner=_origin_checked_runner(probe_runner, checked_origin) if probe_runner else None,
            async_probe_runner=(
                _origin_checked_async_runner(async_probe_runner, checked_origin) if async_probe_runner else None
            ),
            namespace=checked_origin,
            deployment_origin=checked_origin if checked_origin.startswith("https://") else None,
            ttl_seconds=capability_cache_ttl,
            clock=clock,
        )
        self._profile = self._capabilities.baseline_profile
        pinned = self._profile.declared_profile.profile_version
        if site_gate is not None:
            self._site_gate: SiteVersionGate | AsyncSiteVersionGate = site_gate
        elif not async_gate:
            self._site_gate = SiteVersionGate(
                pinned_version=pinned,
                origin=self._origin,
                transport=cast(CatalogTransport, transport),
                ttl_seconds=capability_cache_ttl,
                clock=clock,
            )
        else:
            self._site_gate = AsyncSiteVersionGate(
                pinned_version=pinned,
                origin=self._origin,
                transport=cast(AsyncCatalogTransport, transport),
                ttl_seconds=capability_cache_ttl,
                clock=clock,
            )
        self._credentials = credentials
        self._budget = budget or TimeBudget()
        self._breakers = breakers or BreakerRegistry(
            failure_threshold=breaker_failure_threshold, cooldown=breaker_cooldown, clock=clock
        )
        self._max_attempts = max_attempts
        if type(root_export_max_bytes) is not int or root_export_max_bytes < 1:
            raise ValueError("uData root export byte limits must be positive integers.")
        self._root_export_max_bytes = root_export_max_bytes
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

    @property
    def credentials(self) -> object | None:
        """Expose the injected caller-owned credential resolver or provider."""
        return self._credentials

    def _has_controlled_stack_authority(
        self,
        sync_type: type = _ControlledSyncTransport,
        async_type: type = _ControlledAsyncTransport,
    ) -> bool:
        """Return whether this client came from the controlled evidence factory."""
        return isinstance(self._transport, (sync_type, async_type))

    def _controlled_evidence_digest(
        self,
        sync_type: type = _ControlledSyncTransport,
        async_type: type = _ControlledAsyncTransport,
    ) -> str | None:
        """Return the bound redacted evidence digest without exposing control internals."""
        if isinstance(self._transport, sync_type):
            return cast(_ControlledSyncTransport, self._transport).evidence_digest
        if isinstance(self._transport, async_type):
            return cast(_ControlledAsyncTransport, self._transport).evidence_digest
        return None

    def _controlled_site_id(
        self,
        sync_type: type = _ControlledSyncTransport,
        async_type: type = _ControlledAsyncTransport,
    ) -> str | None:
        """Return the internally bound controlled peer identity when present."""
        if isinstance(self._transport, sync_type):
            return cast(_ControlledSyncTransport, self._transport).site_id
        if isinstance(self._transport, async_type):
            return cast(_ControlledAsyncTransport, self._transport).site_id
        return None

    def _revalidate_controlled_sync_stack(
        self,
        *,
        site_id: str,
        transport_type: type = _ControlledSyncTransport,
    ) -> bool:
        """Revalidate the synchronous authority at the mutation dispatch boundary."""
        if not isinstance(self._transport, transport_type):
            return False
        return cast(_ControlledSyncTransport, self._transport).revalidate(origin=self._origin, site_id=site_id)

    async def _revalidate_controlled_async_stack(
        self,
        *,
        site_id: str,
        transport_type: type = _ControlledAsyncTransport,
    ) -> bool:
        """Revalidate the asynchronous authority at the mutation dispatch boundary."""
        if not isinstance(self._transport, transport_type):
            return False
        return await cast(_ControlledAsyncTransport, self._transport).revalidate(origin=self._origin, site_id=site_id)

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
    ) -> None:
        if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
            return
        if 200 <= response.status_code < 300:
            return
        if response.status_code in {401, 403, 423}:
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
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
                operation=str(owning_id),
                platform=PLATFORM.value,
            ) from exc
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
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], None] = sleep,
        emitter: EventEmitter | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        root_export_max_bytes: int = DEFAULT_ROOT_EXPORT_MAX_BYTES,
        owns_transport: bool = True,
        probe_runner: ProbeRunner | None = None,
    ) -> None:
        """Build the shared sync core over the caller-owned or borrowed transport."""
        self._retry_sleep = retry_sleep
        super().__init__(
            transport,
            profile,
            origin=origin,
            credentials=credentials,
            budget=budget,
            breakers=breakers,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown=breaker_cooldown,
            max_attempts=max_attempts,
            clock=clock,
            emitter=emitter,
            capability_cache_ttl=capability_cache_ttl,
            root_export_max_bytes=root_export_max_bytes,
            owns_transport=owns_transport,
            probe_runner=probe_runner,
        )

    def site_version(self) -> SiteVersion:
        """Run (or reuse) the anonymous exact-version site probe."""
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        return self._require_site_version()

    @property
    def datasets(self) -> SyncDatasetsService:
        """Expose the complete typed dataset service."""
        return SyncDatasetsService(self)

    @property
    def root_profile(self) -> SyncRootProfileService:
        """Expose the complete typed root-profile service."""
        return SyncRootProfileService(self)

    def _require_site_version(self) -> SiteVersion:
        gate = self._site_gate
        if isinstance(gate, SiteVersionGate):
            return gate.require_current(self._credentials)
        raise RuntimeError("The synchronous uData client requires a synchronous site gate.")

    def datasets_list(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[UDataResultItem]:
        """Execute the single bounded dataset list read behind the version gate."""
        owning_id = _operation_id_from(_DATASETS_OPERATION_ID)
        return self._dispatch(operation, guard, owning_id=owning_id)

    def _dataset_call(
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
        allow_retry: bool = False,
        max_response_bytes: int | None = None,
        emit_success: bool = True,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one guarded dataset request scoped to its owning route operation."""
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        owning_id = _operation_id_from(owning_operation)
        self._require_site_version()
        resolved_credential = credential if credential is not None else _refreshed_credential(self._credentials)
        scope = _credential_scope(resolved_credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        guard = build_catalog_operation_guard(owning_id, effective, permissions=permissions)
        guard.require_allowed()
        request_headers = dict(headers or {})
        request_headers.update(_auth_headers(resolved_credential))
        if idempotency_policy is not None and idempotency_policy.key is not None:
            request_headers["Idempotency-Key"] = idempotency_policy.key
        try:
            body = json.dumps(json_body, allow_nan=False).encode() if json_body is not None else None
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog mutation input could not be serialized as JSON.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                metadata={"phase": "serialization"},
            ) from exc
        if body is not None:
            request_headers = {"Content-Type": "application/json", **request_headers}
        request = RuntimeRequest(
            method=method,
            url=self._origin + path,
            headers=request_headers,
            body=body,
            redirect_policy=RedirectPolicy.NO_FOLLOW if redirect_mode else RedirectPolicy.FOLLOW,
            max_response_bytes=max_response_bytes,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        sync_transport = cast(CatalogTransport, self._transport)
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

        recorded = False

        def send() -> RuntimeResponse:
            nonlocal recorded
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = sync_transport.send(request)
            except TransportFailure:
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
                idempotency=idempotency_policy
                or IdempotencyPolicy(safe=method == "GET", explicit_retry_opt_in=allow_retry),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=self._retry_sleep,
            ).run(send)
        except BudgetExhaustedError:
            if emit_success:
                self._emit(owning_id, "budget_exhausted")
            raise
        except Exception:
            if emit_success:
                self._emit(owning_id, "failed")
            raise
        finally:
            if not recorded:
                self._breakers.release_trial(key)
        try:
            self._validate_status(owning_id, response, redirect_mode=redirect_mode, credential_scope=scope)
            if max_response_bytes is not None and len(response.body) > max_response_bytes:
                raise NativeCatalogError(
                    "Catalog operation returned a response larger than its configured byte limit.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                )
            if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
                status_code, response_headers = _decode_redirect_response(owning_id, response)
                result = status_code, response_headers, response
            elif raw_text:
                result = response.status_code, response.body, response
            elif not response.body:
                result = response.status_code, None, response
            else:
                try:
                    payload = json.loads(response.body)
                except (TypeError, ValueError) as exc:
                    raise NativeCatalogError(
                        "Catalog operation returned an invalid JSON result.",
                        operation=str(owning_id),
                        platform=PLATFORM.value,
                        status_code=response.status_code,
                        metadata={"ambiguous": json_body is not None and method != "GET"},
                    ) from exc
                result = response.status_code, payload, response
        except BudgetExhaustedError:
            if emit_success:
                self._emit(owning_id, "budget_exhausted")
            raise
        except Exception:
            if emit_success:
                self._emit(owning_id, "failed")
            raise
        if emit_success:
            self._emit(owning_id, "succeeded")
        return result

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
        allow_retry: bool = False,
        max_response_bytes: int | None = None,
        emit_success: bool = True,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one root-profile request through the shared guarded transport seam."""
        return self._dataset_call(
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
            allow_retry=allow_retry,
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
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        owning_id = _operation_id_from(owning_operation)
        self._require_site_version()
        resolved_credential = credential if credential is not None else _refreshed_credential(self._credentials)
        scope = _credential_scope(resolved_credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        request_headers = dict(headers or {})
        request_headers.update(_auth_headers(resolved_credential))
        request = RuntimeRequest(
            method="GET",
            url=self._origin + path,
            headers=request_headers,
            redirect_policy=RedirectPolicy.NO_FOLLOW,
            max_response_bytes=self._root_export_max_bytes,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        sync_transport = cast(CatalogTransport, self._transport)
        send_stream = getattr(sync_transport, "send_stream", None)
        if not callable(send_stream):
            raise CatalogValidationError(
                "The uData export requires a streaming catalog transport.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                safe_action="Use the default transport or inject one implementing send_stream.",
            )
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
        settled = False
        try:
            before = self._breakers.inspect(key)
            try:
                response = cast(RuntimeStreamResponse, send_stream(request))
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                settled = True
                self._emit(owning_id, "failed")
                raise
            if response.status_code >= 500:
                after = self._breakers.record_transport_failure(key)
                settled = True
            elif response.status_code >= 400:
                after = self._breakers.record_response(key, response.status_code)
                settled = True
            elif response.status_code >= 300:
                after = self._breakers.record_success(key)
                settled = True
            else:
                after = before
            self._emit_breaker_change(owning_id, before.open, after.open)
            try:
                self._validate_status(owning_id, response, redirect_mode=True, credential_scope=scope)
                if response.status_code in {301, 302, 303, 307, 308}:
                    _decode_redirect_response(owning_id, cast(RuntimeResponse, response))
                    return response
            except BaseException:
                response.close()
                self._emit(owning_id, "failed")
                raise
        finally:
            if not settled:
                self._breakers.release_trial(key)

        settled = False
        consumed = False

        def settle_failure(error: BaseException) -> None:
            nonlocal settled
            if settled:
                return
            settled = True
            if isinstance(error, TransportFailure):
                failure_before = self._breakers.inspect(key)
                failure_after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, failure_before.open, failure_after.open)
            if isinstance(error, BudgetExhaustedError):
                self._emit(
                    owning_id, "budget_exhausted", budget_usage=max(0.0, self._budget.total - deadline.remaining())
                )
            elif error.__class__.__name__ == "CancelledError":
                self._emit(owning_id, "cancelled")
            else:
                self._emit(owning_id, "failed")

        def settle_success() -> None:
            nonlocal settled
            if not settled:
                try:
                    deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
                except BaseException as error:
                    settle_failure(error)
                    raise
                settled = True
                success_before = self._breakers.inspect(key)
                success_after = self._breakers.record_success(key)
                self._emit_breaker_change(owning_id, success_before.open, success_after.open)
                self._emit(owning_id, "succeeded")

        def guarded_chunks() -> Generator[bytes, None, None]:
            nonlocal consumed
            try:
                for chunk in response:
                    deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
                    yield chunk
            except GeneratorExit:
                raise
            except BaseException as error:
                settle_failure(error)
                raise
            else:
                consumed = True

        stream_chunks = guarded_chunks()

        def close() -> None:
            try:
                stream_chunks.close()
                response.close()
            except BaseException as error:
                settle_failure(error)
                raise
            if not settled and not consumed:
                settle_failure(RuntimeError("The catalog stream was closed before completion."))

        return RuntimeStreamResponse(
            status_code=response.status_code,
            headers=response.headers,
            chunks=stream_chunks,
            close_callback=close,
            retry_after=response.retry_after,
            failure_callback=settle_failure,
            completion_callback=settle_success,
        )

    def _dispatch(
        self,
        operation: CatalogOperationRequest,
        guard: CatalogOperationGuard,
        *,
        owning_id: OperationId,
    ) -> ResultEnvelope[UDataResultItem]:
        if self._closed:
            raise RuntimeError("The synchronous uData client is closed.")
        _enforce_caller_guards(operation, guard)
        if operation.operation_id != owning_id:
            raise unimplemented_family(str(operation.operation_id))
        self._require_site_version()
        credential = _refreshed_credential(self._credentials)
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = self._capabilities.resolve(owning_id, credential_scope=scope)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        params = self._validate_page_params(operation)
        request = _page_request(origin=self._origin, params=params)
        if credential is not None:
            request = RuntimeRequest(
                method=request.method,
                url=request.url,
                headers=_auth_headers(credential),
                body=request.body,
            )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        sync_transport = cast(CatalogTransport, self._transport)
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

        def send() -> RuntimeResponse:
            nonlocal attempts
            attempts += 1
            before = self._breakers.inspect(key)
            try:
                response = sync_transport.send(request)
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                raise
            after = self._breakers.record_response(key, response.status_code)
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
            if self._owns_transport:
                cast(CatalogTransport, self._transport).close()

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
        budget: TimeBudget | None = None,
        breakers: BreakerRegistry | None = None,
        breaker_failure_threshold: int = DEFAULT_BREAKER_FAILURE_THRESHOLD,
        breaker_cooldown: float = DEFAULT_BREAKER_COOLDOWN_SECONDS,
        max_attempts: int = 3,
        clock: Callable[[], float] = monotonic,
        retry_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        emitter: EventEmitter | None = None,
        capability_cache_ttl: float = DEFAULT_CAPABILITY_CACHE_TTL_SECONDS,
        root_export_max_bytes: int = DEFAULT_ROOT_EXPORT_MAX_BYTES,
        owns_transport: bool = True,
        async_probe_runner: AsyncProbeRunner | None = None,
    ) -> None:
        """Build the shared async core over the caller-owned or borrowed transport."""
        self._retry_sleep = retry_sleep
        super().__init__(
            transport,
            profile,
            origin=origin,
            credentials=credentials,
            budget=budget,
            breakers=breakers,
            breaker_failure_threshold=breaker_failure_threshold,
            breaker_cooldown=breaker_cooldown,
            max_attempts=max_attempts,
            clock=clock,
            emitter=emitter,
            capability_cache_ttl=capability_cache_ttl,
            root_export_max_bytes=root_export_max_bytes,
            owns_transport=owns_transport,
            async_probe_runner=async_probe_runner,
            async_gate=True,
        )

    async def site_version(self) -> SiteVersion:
        """Run (or reuse) the anonymous exact-version site probe."""
        if self._closed:
            raise RuntimeError("The asynchronous uData client is closed.")
        gate = self._site_gate
        if isinstance(gate, AsyncSiteVersionGate):
            return await gate.require_current_async(self._credentials)
        raise RuntimeError("The asynchronous uData client requires an asynchronous site gate.")

    @property
    def datasets(self) -> AsyncDatasetsService:
        """Expose the complete typed dataset service."""
        return AsyncDatasetsService(self)

    @property
    def root_profile(self) -> AsyncRootProfileService:
        """Expose the complete typed root-profile service."""
        return AsyncRootProfileService(self)

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
            raise RuntimeError("The asynchronous uData client is closed.")
        _enforce_caller_guards(operation, guard)
        if operation.operation_id != owning_id:
            raise unimplemented_family(str(operation.operation_id))
        await self.site_version()
        credential = await _refreshed_credential_async(self._credentials)
        scope = _credential_scope(credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        params = self._validate_page_params(operation)
        request = _page_request(origin=self._origin, params=params)
        if credential is not None:
            request = RuntimeRequest(
                method=request.method,
                url=request.url,
                headers=_auth_headers(credential),
                body=request.body,
            )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        async_transport = cast(AsyncCatalogTransport, self._transport)
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
            nonlocal attempts
            attempts += 1
            nonlocal recorded
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = await async_transport.send(request)
            except TransportFailure:
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

    async def _dataset_call_async(
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
        allow_retry: bool = False,
        max_response_bytes: int | None = None,
        emit_success: bool = True,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one guarded async dataset request scoped to its owning route operation."""
        if self._closed:
            raise RuntimeError("The asynchronous uData client is closed.")
        owning_id = _operation_id_from(owning_operation)
        await self.site_version()
        resolved_credential = (
            credential if credential is not None else await _refreshed_credential_async(self._credentials)
        )
        scope = _credential_scope(resolved_credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        guard = build_catalog_operation_guard(owning_id, effective, permissions=permissions)
        guard.require_allowed()
        request_headers = dict(headers or {})
        request_headers.update(_auth_headers(resolved_credential))
        if idempotency_policy is not None and idempotency_policy.key is not None:
            request_headers["Idempotency-Key"] = idempotency_policy.key
        try:
            body = json.dumps(json_body, allow_nan=False).encode() if json_body is not None else None
        except (TypeError, ValueError) as exc:
            raise NativeCatalogError(
                "Catalog mutation input could not be serialized as JSON.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                metadata={"phase": "serialization"},
            ) from exc
        if body is not None:
            request_headers = {"Content-Type": "application/json", **request_headers}
        request = RuntimeRequest(
            method=method,
            url=self._origin + path,
            headers=request_headers,
            body=body,
            redirect_policy=RedirectPolicy.NO_FOLLOW if redirect_mode else RedirectPolicy.FOLLOW,
            max_response_bytes=max_response_bytes,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        async_transport = cast(AsyncCatalogTransport, self._transport)
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

        recorded = False

        async def send() -> RuntimeResponse:
            nonlocal recorded
            recorded = False
            before = self._breakers.inspect(key)
            try:
                response = await async_transport.send(request)
            except TransportFailure:
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
                idempotency=idempotency_policy
                or IdempotencyPolicy(safe=method == "GET", explicit_retry_opt_in=allow_retry),
                deadline=deadline,
                max_attempts=self._max_attempts,
                sleep=lambda _: None,
            ).run_async(send, sleep=self._retry_sleep)
        except BudgetExhaustedError:
            if emit_success:
                self._emit(owning_id, "budget_exhausted")
            raise
        except Exception:
            if emit_success:
                self._emit(owning_id, "failed")
            raise
        finally:
            if not recorded:
                self._breakers.release_trial(key)
        try:
            self._validate_status(owning_id, response, redirect_mode=redirect_mode, credential_scope=scope)
            if max_response_bytes is not None and len(response.body) > max_response_bytes:
                raise NativeCatalogError(
                    "Catalog operation returned a response larger than its configured byte limit.",
                    operation=str(owning_id),
                    platform=PLATFORM.value,
                    status_code=response.status_code,
                )
            if redirect_mode and response.status_code in {301, 302, 303, 307, 308}:
                status_code, response_headers = _decode_redirect_response(owning_id, response)
                result = status_code, response_headers, response
            elif raw_text:
                result = response.status_code, response.body, response
            elif not response.body:
                result = response.status_code, None, response
            else:
                try:
                    payload = json.loads(response.body)
                except (TypeError, ValueError) as exc:
                    raise NativeCatalogError(
                        "Catalog operation returned an invalid JSON result.",
                        operation=str(owning_id),
                        platform=PLATFORM.value,
                        status_code=response.status_code,
                        metadata={"ambiguous": json_body is not None and method != "GET"},
                    ) from exc
                result = response.status_code, payload, response
        except BudgetExhaustedError:
            if emit_success:
                self._emit(owning_id, "budget_exhausted")
            raise
        except Exception:
            if emit_success:
                self._emit(owning_id, "failed")
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
        allow_retry: bool = False,
        max_response_bytes: int | None = None,
        emit_success: bool = True,
    ) -> tuple[int, object, RuntimeResponse]:
        """Run one async root-profile request through the shared transport seam."""
        return await self._dataset_call_async(
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
            allow_retry=allow_retry,
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
        if self._closed:
            raise RuntimeError("The asynchronous uData client is closed.")
        owning_id = _operation_id_from(owning_operation)
        await self.site_version()
        resolved_credential = (
            credential if credential is not None else await _refreshed_credential_async(self._credentials)
        )
        scope = _credential_scope(resolved_credential)
        if scope != self._credential_scope:
            self._capabilities.invalidate()
            self._site_gate.invalidate()
            self._credential_scope = scope
        effective = await self._capabilities.resolve_async(owning_id, credential_scope=scope)
        build_catalog_operation_guard(owning_id, effective).require_allowed()
        request_headers = dict(headers or {})
        request_headers.update(_auth_headers(resolved_credential))
        request = RuntimeRequest(
            method="GET",
            url=self._origin + path,
            headers=request_headers,
            redirect_policy=RedirectPolicy.NO_FOLLOW,
            max_response_bytes=self._root_export_max_bytes,
        )
        deadline = DeadlineMonitor(self._budget, clock=self._clock)
        deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
        async_transport = cast(AsyncCatalogTransport, self._transport)
        send_stream = getattr(async_transport, "send_stream", None)
        if not callable(send_stream):
            raise CatalogValidationError(
                "The uData export requires a streaming catalog transport.",
                operation=str(owning_id),
                platform=PLATFORM.value,
                safe_action="Use the default transport or inject one implementing send_stream.",
            )
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
        settled = False
        try:
            before = self._breakers.inspect(key)
            try:
                response = cast(AsyncRuntimeStreamResponse, await send_stream(request))
            except TransportFailure:
                after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, before.open, after.open)
                settled = True
                self._emit(owning_id, "failed")
                raise
            if response.status_code >= 500:
                after = self._breakers.record_transport_failure(key)
                settled = True
            elif response.status_code >= 400:
                after = self._breakers.record_response(key, response.status_code)
                settled = True
            elif response.status_code >= 300:
                after = self._breakers.record_success(key)
                settled = True
            else:
                after = before
            self._emit_breaker_change(owning_id, before.open, after.open)
            try:
                self._validate_status(owning_id, response, redirect_mode=True, credential_scope=scope)
                if response.status_code in {301, 302, 303, 307, 308}:
                    _decode_redirect_response(owning_id, cast(RuntimeResponse, response))
                    return response
            except BaseException:
                await response.aclose()
                self._emit(owning_id, "failed")
                raise
        finally:
            if not settled:
                self._breakers.release_trial(key)

        settled = False
        consumed = False

        def settle_failure(error: BaseException) -> None:
            nonlocal settled
            if settled:
                return
            settled = True
            if isinstance(error, TransportFailure):
                failure_before = self._breakers.inspect(key)
                failure_after = self._breakers.record_transport_failure(key)
                self._emit_breaker_change(owning_id, failure_before.open, failure_after.open)
            if isinstance(error, BudgetExhaustedError):
                self._emit(
                    owning_id, "budget_exhausted", budget_usage=max(0.0, self._budget.total - deadline.remaining())
                )
            elif error.__class__.__name__ == "CancelledError":
                self._emit(owning_id, "cancelled")
            else:
                self._emit(owning_id, "failed")

        def settle_success() -> None:
            nonlocal settled
            if not settled:
                try:
                    deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
                except BaseException as error:
                    settle_failure(error)
                    raise
                settled = True
                success_before = self._breakers.inspect(key)
                success_after = self._breakers.record_success(key)
                self._emit_breaker_change(owning_id, success_before.open, success_after.open)
                self._emit(owning_id, "succeeded")

        async def guarded_chunks() -> AsyncGenerator[bytes, None]:
            nonlocal consumed
            try:
                async for chunk in response:
                    deadline.assert_dispatchable(str(owning_id), PLATFORM.value)
                    yield chunk
            except BaseException as error:
                settle_failure(error)
                raise
            else:
                consumed = True

        stream_chunks = guarded_chunks()

        async def close() -> None:
            try:
                await stream_chunks.aclose()
                await response.aclose()
            except BaseException as error:
                settle_failure(error)
                raise
            if not settled and not consumed:
                settle_failure(RuntimeError("The catalog stream was closed before completion."))

        return AsyncRuntimeStreamResponse(
            status_code=response.status_code,
            headers=response.headers,
            chunks=stream_chunks,
            close_callback=close,
            retry_after=response.retry_after,
            failure_callback=settle_failure,
            completion_callback=settle_success,
        )

    async def aclose(self) -> None:
        """Close the client and its owned transport exactly once."""
        if not self._closed:
            self._closed = True
            if self._owns_transport:
                await cast(AsyncCatalogTransport, self._transport).aclose()

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
        transport = cast(CatalogTransport, override)
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


def _require_controlled_settings(settings: UDataClientSettings) -> None:
    if settings.base_url != _CONTROLLED_ORIGIN:
        raise _controlled_error("Controlled uData mutations require the approved loopback origin.")
    if settings.sync_transport is not None or settings.async_transport is not None:
        raise _controlled_error("Controlled uData mutations cannot use caller-provided transports.")


def _create_controlled_sync_client(settings: UDataClientSettings) -> SyncUDataClient:
    """Construct a stock synchronous client with a live-verified mutation authority."""
    require_extra("udata")
    _require_controlled_settings(settings)
    transport = _ControlledSyncTransport(tls_policy=settings.tls_policy, budget=settings.budget)
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
        owns_transport=True,
        probe_runner=settings.probe_runner,
    )


async def _create_controlled_async_client(settings: UDataClientSettings) -> AsyncUDataClient:
    """Construct a stock asynchronous client with a live-verified mutation authority."""
    require_extra("udata")
    _require_controlled_settings(settings)
    transport = _ControlledAsyncTransport(tls_policy=settings.tls_policy, budget=settings.budget)
    await transport.verify()
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
        owns_transport=True,
        async_probe_runner=settings.async_probe_runner,
    )
