"""Transport-backed strict-version sync and async uData clients."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
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
from typing import TYPE_CHECKING, Self, cast
from urllib.parse import urlsplit

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
from datasluice.connectors.catalog.udata.settings import UDataClientSettings, normalize_origin
from datasluice.contracts.catalog.native.udata import UDataResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.auth import EffectivePermissions, SecretValue, UDataCredential
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
from datasluice.domain.catalog.udata import SET_SITE_OPERATION
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
    TransportFailure,
)
from datasluice.runtime.transport.httpx_transport import AsyncHttpxCatalogTransport, HttpxCatalogTransport

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.services.datasets import AsyncDatasetsService, SyncDatasetsService
    from datasluice.connectors.catalog.udata.services.root_profile import (
        AsyncRootProfileService,
        SyncRootProfileService,
    )

_PROFILE_RESOURCE = "udata-17.6.json"
_PAGER_PARAMS = frozenset({"page", "page_size"})
_CONTROLLED_ORIGIN = "http://127.0.0.1:5640"
_CONTROLLED_SOURCE_COMMIT = "0546582058d84706812a1c37387576efc4e5ad1f"
_CONTROLLED_COMPOSE_SHA256 = "f7acbcd1ea2f88f7b9361cbfadbd46e62be82bd707538f22f0615796e8bf09a3"
_CONTROLLED_DOCKERFILE_SHA256 = "6c21f02c3a287f1c1a2b42db392e767a484792bb763827a65bce5fcdd0d97e3b"
_CONTROLLED_UDATA_IMAGE_REPOSITORY = "udata-evidence-udata"
_CONTROLLED_DEPENDENCY_IMAGE_SPECS = (
    ("mongo", "mongo@sha256:d3d7c7fbbbb18f61baac3f8d13f0834c28a0e000cae444691def321d568abe47"),
    ("redis", "redis@sha256:28bd5e15c3674c48a472a3dd475ba446d0a3cd876e7addb988b5840a286b2256"),
    (
        "search",
        "docker.elastic.co/elasticsearch/elasticsearch@sha256:5496dd095a610571a02c362cd5f60ddd29a2cac5225d52f953241a5189871356",
    ),
    ("storage", "minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"),
    ("mailpit", "axllent/mailpit@sha256:fa9d90f91a042f92cc28cf6dc4c75c6d57ac693b2737cdd30a6bfd9879838bbf"),
)
_CONTROLLED_IMAGE_DIGESTS = tuple(image for _, image in _CONTROLLED_DEPENDENCY_IMAGE_SPECS)
_CONTROLLED_SERVICE_NAMES = ("udata", "mongo", "redis", "search", "storage", "mailpit")
_CONTROLLED_EVIDENCE_ROOT = Path(__file__).resolve().parents[5] / "dev" / "udata-evidence"
_CONTROLLED_COMPOSE_FILE = _CONTROLLED_EVIDENCE_ROOT / "compose.yaml"
_CONTROLLED_ENV_FILE = _CONTROLLED_EVIDENCE_ROOT / ".env"
_CONTROLLED_AUTHORITY_TTL_SECONDS = 60.0
_CONTROLLED_COMMAND_TIMEOUT_SECONDS = 15.0
_CONTROLLED_REAP_TIMEOUT_SECONDS = 1.0
_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES = 65536
_CONTROLLED_DOCKER_EXECUTABLES = (
    Path("/usr/local/bin/docker"),
    Path("/opt/homebrew/bin/docker"),
    Path("/usr/bin/docker"),
)
_CONTROLLED_PATCH_PROGRAM = """
import json
import sys
import urllib.error
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

def emit(response):
    body = response.read(8193)
    print(json.dumps({
        "status": getattr(response, "status", getattr(response, "code", 0)),
        "content_type": response.headers.get("Content-Type", ""),
        "location": response.headers.get("Location", ""),
        "body": body.decode("utf-8", "replace"),
    }, separators=(",", ":")))

request_data = json.loads(sys.stdin.read())
request = urllib.request.Request(
    "http://127.0.0.1:7000/api/1/site/",
    data=json.dumps(request_data["body"], separators=(",", ":")).encode(),
    headers={"Content-Type": "application/json", "X-API-KEY": request_data["token"]},
    method="PATCH",
)
try:
    response = urllib.request.build_opener(NoRedirect()).open(request, timeout=10)
except urllib.error.HTTPError as error:
    emit(error)
else:
    try:
        emit(response)
    finally:
        response.close()
""".strip()
_CONTROLLED_SITE_PROBE_PROGRAM = """
import json
import urllib.error
import urllib.request

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None

request = urllib.request.Request("http://127.0.0.1:7000/api/1/site/")
try:
    response = urllib.request.build_opener(NoRedirect()).open(request, timeout=10)
except urllib.error.HTTPError as error:
    response = error
try:
    body = response.read(8193)
    print(json.dumps({
        "status": getattr(response, "status", getattr(response, "code", 0)),
        "content_type": response.headers.get("Content-Type", ""),
        "location": response.headers.get("Location", ""),
        "body": body.decode("utf-8", "replace"),
    }, separators=(",", ":")))
finally:
    response.close()
""".strip()

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


def _controlled_command_spec(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
) -> tuple[tuple[str, ...], dict[str, str]]:
    if not _CONTROLLED_COMPOSE_FILE.is_file() or not _CONTROLLED_ENV_FILE.is_file():
        raise _controlled_error("The controlled uData evidence configuration is unavailable.")
    if input_data is not None and len(input_data) > _CONTROLLED_COMMAND_MAX_OUTPUT_BYTES:
        raise _controlled_error("The controlled uData command input is too large.")
    if docker_endpoint is not None:
        parsed_endpoint = urlsplit(docker_endpoint)
        if (
            parsed_endpoint.scheme != "unix"
            or bool(parsed_endpoint.netloc)
            or not parsed_endpoint.path.startswith("/")
            or bool(parsed_endpoint.query)
            or bool(parsed_endpoint.fragment)
        ):
            raise _controlled_error("Controlled uData evidence requires a local Unix Docker context.")
    executable = next(
        (path for path in _CONTROLLED_DOCKER_EXECUTABLES if path.is_file() and os.access(path, os.X_OK)),
        None,
    )
    if executable is None:
        raise _controlled_error("The trusted Docker executable is unavailable.")
    if os.environ.get("DOCKER_HOST") or os.environ.get("DOCKER_CONTEXT"):
        raise _controlled_error("Controlled uData evidence cannot use Docker environment overrides.")
    executable_prefix = (str(executable),) if docker_endpoint is None else (str(executable), "--host", docker_endpoint)
    command = (
        (*executable_prefix, *args)
        if direct or args[:2] == ("context", "inspect")
        else (
            *executable_prefix,
            "compose",
            "--env-file",
            str(_CONTROLLED_ENV_FILE),
            "-f",
            str(_CONTROLLED_COMPOSE_FILE),
            *args,
        )
    )
    return command, {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}


def _make_bound_controlled_command_spec() -> Callable[..., tuple[tuple[str, ...], dict[str, str]]]:
    compose_file = _CONTROLLED_COMPOSE_FILE
    env_file = _CONTROLLED_ENV_FILE
    compose_file_exists = compose_file.is_file
    env_file_exists = env_file.is_file
    executable_flag = os.X_OK
    executable_candidates = tuple((str(path), path.is_file, os.access) for path in _CONTROLLED_DOCKER_EXECUTABLES)
    environment_get = os.environ.get
    url_split = urlsplit
    error = _controlled_error
    input_limit = _CONTROLLED_COMMAND_MAX_OUTPUT_BYTES
    path_value = environment_get("PATH", "/usr/bin:/bin")

    def command_spec(
        args: tuple[str, ...],
        *,
        input_data: bytes | None = None,
        docker_endpoint: str | None = None,
        direct: bool = False,
    ) -> tuple[tuple[str, ...], dict[str, str]]:
        if not compose_file_exists() or not env_file_exists():
            raise error("The controlled uData evidence configuration is unavailable.")
        if input_data is not None and len(input_data) > input_limit:
            raise error("The controlled uData command input is too large.")
        if docker_endpoint is not None:
            parsed_endpoint = url_split(docker_endpoint)
            if (
                parsed_endpoint.scheme != "unix"
                or bool(parsed_endpoint.netloc)
                or not parsed_endpoint.path.startswith("/")
                or bool(parsed_endpoint.query)
                or bool(parsed_endpoint.fragment)
            ):
                raise error("Controlled uData evidence requires a local Unix Docker context.")
        executable = next(
            (path for path, is_file, access in executable_candidates if is_file() and access(path, executable_flag)),
            None,
        )
        if executable is None:
            raise error("The trusted Docker executable is unavailable.")
        if environment_get("DOCKER_HOST") or environment_get("DOCKER_CONTEXT"):
            raise error("Controlled uData evidence cannot use Docker environment overrides.")
        executable_prefix = (executable,) if docker_endpoint is None else (executable, "--host", docker_endpoint)
        command = (
            (*executable_prefix, *args)
            if direct or args[:2] == ("context", "inspect")
            else (
                *executable_prefix,
                "compose",
                "--env-file",
                str(env_file),
                "-f",
                str(compose_file),
                *args,
            )
        )
        return command, {"PATH": path_value}

    return command_spec


@dataclass(frozen=True, slots=True)
class _ControlledSyncRuntime:
    command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]]
    error: Callable[[str], CatalogValidationError]
    pipe: Callable[[], tuple[int, int]]
    posix_spawnp: Callable[..., int]
    close: Callable[[int], None]
    set_blocking: Callable[[int, bool], None]
    read: Callable[[int, int], bytes]
    write: Callable[[int, bytes], int]
    kill: Callable[[int, int], None]
    waitpid: Callable[..., tuple[int, int]]
    waitstatus_to_exitcode: Callable[[int], int]
    select: Callable[..., tuple[list[int], list[int], list[int]]]
    monotonic: Callable[[], float]
    posix_spawn_dup2: int
    posix_spawn_close: int
    wnohang: int
    sigkill: int
    timeout_seconds: float
    reap_timeout_seconds: float
    max_output_bytes: int


def _current_controlled_sync_runtime(
    *, command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]] | None = None
) -> _ControlledSyncRuntime:
    return _ControlledSyncRuntime(
        command_spec=_controlled_command_spec if command_spec is None else command_spec,
        error=_controlled_error,
        pipe=os.pipe,
        posix_spawnp=os.posix_spawnp,
        close=os.close,
        set_blocking=os.set_blocking,
        read=os.read,
        write=os.write,
        kill=os.kill,
        waitpid=os.waitpid,
        waitstatus_to_exitcode=os.waitstatus_to_exitcode,
        select=select.select,
        monotonic=monotonic,
        posix_spawn_dup2=os.POSIX_SPAWN_DUP2,
        posix_spawn_close=os.POSIX_SPAWN_CLOSE,
        wnohang=os.WNOHANG,
        sigkill=signal.SIGKILL,
        timeout_seconds=_CONTROLLED_COMMAND_TIMEOUT_SECONDS,
        reap_timeout_seconds=_CONTROLLED_REAP_TIMEOUT_SECONDS,
        max_output_bytes=_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES,
    )


def _terminate_controlled_process_sync(process_id: int, runtime: _ControlledSyncRuntime) -> None:
    try:
        runtime.kill(process_id, runtime.sigkill)
    except OSError:
        pass
    reap_deadline = runtime.monotonic() + runtime.reap_timeout_seconds
    while True:
        try:
            completed_pid, _ = runtime.waitpid(process_id, runtime.wnohang)
        except OSError:
            return
        if completed_pid == process_id:
            return
        remaining = reap_deadline - runtime.monotonic()
        if remaining <= 0:
            return
        runtime.select([], [], [], min(remaining, 0.05))


def _run_controlled_command(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
    runtime: _ControlledSyncRuntime,
) -> str:
    def close_descriptor(fd: int) -> BaseException | None:
        if fd < 0:
            return None
        try:
            runtime.close(fd)
        except BaseException as error:
            return error
        return None

    command, environment = runtime.command_spec(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
    )
    read_fd = write_fd = input_read_fd = input_write_fd = -1
    process_id: int | None = None
    wait_status: int | None = None
    chunks: list[bytes] = []
    primary_error: BaseException | None = None
    cleanup_errors: list[BaseException] = []
    try:
        read_fd, write_fd = runtime.pipe()
        if input_data is not None:
            input_read_fd, input_write_fd = runtime.pipe()
        deadline = runtime.monotonic() + runtime.timeout_seconds
        file_actions = [
            (runtime.posix_spawn_dup2, write_fd, 1),
            (runtime.posix_spawn_dup2, write_fd, 2),
            (runtime.posix_spawn_close, read_fd),
            (runtime.posix_spawn_close, write_fd),
        ]
        if input_data is not None:
            file_actions.extend(
                (
                    (runtime.posix_spawn_dup2, input_read_fd, 0),
                    (runtime.posix_spawn_close, input_read_fd),
                    (runtime.posix_spawn_close, input_write_fd),
                )
            )
        process_id = runtime.posix_spawnp(command[0], command, environment, file_actions=tuple(file_actions))
        close_error = close_descriptor(write_fd)
        write_fd = -1
        if close_error is not None:
            raise close_error
        if input_read_fd >= 0:
            close_error = close_descriptor(input_read_fd)
            input_read_fd = -1
            if close_error is not None:
                raise close_error
        if input_write_fd >= 0:
            runtime.set_blocking(input_write_fd, False)
            offset = 0
            input_bytes = input_data or b""
            while offset < len(input_bytes):
                remaining = deadline - runtime.monotonic()
                if remaining <= 0 or not runtime.select([], [input_write_fd], [], remaining)[1]:
                    _terminate_controlled_process_sync(process_id, runtime)
                    raise runtime.error(timeout_message)
                try:
                    offset += runtime.write(input_write_fd, input_bytes[offset:])
                except BlockingIOError:
                    continue
            close_error = close_descriptor(input_write_fd)
            input_write_fd = -1
            if close_error is not None:
                raise close_error
        output_size = 0
        while True:
            remaining = deadline - runtime.monotonic()
            if remaining <= 0 or not runtime.select([read_fd], [], [], remaining)[0]:
                _terminate_controlled_process_sync(process_id, runtime)
                raise runtime.error(timeout_message)
            chunk = runtime.read(read_fd, 65536)
            if not chunk:
                break
            output_size += len(chunk)
            if output_size > runtime.max_output_bytes:
                _terminate_controlled_process_sync(process_id, runtime)
                raise runtime.error(output_message)
            chunks.append(chunk)
        while True:
            completed_pid, wait_status = runtime.waitpid(process_id, runtime.wnohang)
            if completed_pid == process_id:
                break
            remaining = deadline - runtime.monotonic()
            if remaining <= 0:
                _terminate_controlled_process_sync(process_id, runtime)
                raise runtime.error(timeout_message)
            runtime.select([], [], [], min(remaining, 0.05))
    except (AttributeError, OSError) as error:
        if process_id is not None:
            _terminate_controlled_process_sync(process_id, runtime)
        try:
            raise runtime.error(failure_message) from error
        except CatalogValidationError as sanitized:
            primary_error = sanitized
    except BaseException as error:
        primary_error = error
    finally:
        for fd in (read_fd, write_fd, input_read_fd, input_write_fd):
            close_error = close_descriptor(fd)
            if close_error is not None:
                cleanup_errors.append(close_error)
    if primary_error is not None:
        if cleanup_errors:
            raise primary_error from cleanup_errors[0]
        raise primary_error
    if cleanup_errors:
        raise runtime.error(failure_message) from cleanup_errors[0]
    if wait_status is None:
        raise runtime.error(failure_message)
    if runtime.waitstatus_to_exitcode(wait_status) != 0:
        raise runtime.error(failure_message)
    invalid_output = False
    try:
        output = b"".join(chunks).decode().strip()
    except UnicodeDecodeError:
        invalid_output = True
    if invalid_output:
        raise runtime.error(output_message)
    return output


def _controlled_command(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
) -> str:
    return _run_controlled_command(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message=timeout_message,
        output_message=output_message,
        failure_message=failure_message,
        runtime=_current_controlled_sync_runtime(),
    )


def _make_bound_controlled_command(runtime: _ControlledSyncRuntime) -> Callable[..., str]:
    run_command = _run_controlled_command

    def command(
        args: tuple[str, ...],
        *,
        input_data: bytes | None = None,
        docker_endpoint: str | None = None,
        direct: bool = False,
        timeout_message: str,
        output_message: str,
        failure_message: str,
    ) -> str:
        return run_command(
            args,
            input_data=input_data,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message=timeout_message,
            output_message=output_message,
            failure_message=failure_message,
            runtime=runtime,
        )

    return command


def _compose_read(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
    return _controlled_command(
        args,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message="The controlled uData stack identity check timed out.",
        output_message="The controlled uData stack identity check returned too much output.",
        failure_message="The controlled uData stack identity check failed.",
    )


async def _terminate_controlled_process(
    process: asyncio.subprocess.Process | None,
    *,
    wait_for: Callable[..., Awaitable[object]] = asyncio.wait_for,
    clock: Callable[[], float] = monotonic,
    reap_timeout: float = _CONTROLLED_REAP_TIMEOUT_SECONDS,
) -> None:
    if process is None or process.returncode is not None:
        return
    try:
        process.kill()
    except OSError:
        pass
    reap_deadline = clock() + reap_timeout
    remaining = reap_deadline - clock()
    if remaining <= 0:
        return
    try:
        await wait_for(process.wait(), timeout=remaining)
    except (OSError, TimeoutError):
        return


@dataclass(frozen=True, slots=True)
class _ControlledAsyncRuntime:
    command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]]
    error: Callable[[str], CatalogValidationError]
    create_subprocess_exec: Callable[..., Awaitable[asyncio.subprocess.Process]]
    wait_for: Callable[..., Awaitable[object]]
    terminate: Callable[[asyncio.subprocess.Process | None], Awaitable[None]]
    pipe: int
    devnull: int
    stdout: int
    stderr: int
    monotonic: Callable[[], float]
    timeout_seconds: float
    max_output_bytes: int


def _current_controlled_async_runtime(
    *, command_spec: Callable[..., tuple[tuple[str, ...], dict[str, str]]] | None = None
) -> _ControlledAsyncRuntime:
    return _ControlledAsyncRuntime(
        command_spec=_controlled_command_spec if command_spec is None else command_spec,
        error=_controlled_error,
        create_subprocess_exec=asyncio.create_subprocess_exec,
        wait_for=asyncio.wait_for,
        terminate=_terminate_controlled_process,
        pipe=asyncio.subprocess.PIPE,
        devnull=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        monotonic=monotonic,
        timeout_seconds=_CONTROLLED_COMMAND_TIMEOUT_SECONDS,
        max_output_bytes=_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES,
    )


async def _run_controlled_command_async(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
    runtime: _ControlledAsyncRuntime,
) -> str:
    command, environment = runtime.command_spec(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
    )
    process: asyncio.subprocess.Process | None = None
    chunks: list[bytes] = []
    deadline = runtime.monotonic() + runtime.timeout_seconds
    try:
        remaining = deadline - runtime.monotonic()
        if remaining <= 0:
            raise runtime.error(timeout_message)
        try:
            process = cast(
                asyncio.subprocess.Process,
                await runtime.wait_for(
                    runtime.create_subprocess_exec(
                        *command,
                        stdin=runtime.pipe if input_data is not None else runtime.devnull,
                        stdout=runtime.stdout,
                        stderr=runtime.stderr,
                        env=environment,
                    ),
                    timeout=remaining,
                ),
            )
        except TimeoutError:
            raise runtime.error(timeout_message) from None
        if process.stdout is None:
            raise OSError("controlled command stdout was unavailable")
        if input_data is not None:
            if process.stdin is None:
                raise OSError("controlled command stdin was unavailable")
            remaining = deadline - runtime.monotonic()
            if remaining <= 0:
                await runtime.terminate(process)
                raise runtime.error(timeout_message)
            process.stdin.write(input_data)
            try:
                await runtime.wait_for(process.stdin.drain(), timeout=remaining)
            except TimeoutError:
                await runtime.terminate(process)
                raise runtime.error(timeout_message) from None
            process.stdin.close()
        output_size = 0
        while True:
            remaining = deadline - runtime.monotonic()
            if remaining <= 0:
                await runtime.terminate(process)
                raise runtime.error(timeout_message)
            try:
                chunk = cast(bytes, await runtime.wait_for(process.stdout.read(65536), timeout=remaining))
            except TimeoutError:
                await runtime.terminate(process)
                raise runtime.error(timeout_message) from None
            if not chunk:
                break
            output_size += len(chunk)
            if output_size > runtime.max_output_bytes:
                await runtime.terminate(process)
                raise runtime.error(output_message)
            chunks.append(chunk)
        remaining = deadline - runtime.monotonic()
        if remaining <= 0:
            await runtime.terminate(process)
            raise runtime.error(timeout_message)
        try:
            await runtime.wait_for(process.wait(), timeout=remaining)
        except TimeoutError:
            await runtime.terminate(process)
            raise runtime.error(timeout_message) from None
    except (OSError, ChildProcessError) as error:
        await runtime.terminate(process)
        raise runtime.error(failure_message) from error
    finally:
        await runtime.terminate(process)
    if process is None or process.returncode != 0:
        raise runtime.error(failure_message)
    try:
        return b"".join(chunks).decode().strip()
    except UnicodeDecodeError:
        raise runtime.error(output_message) from None


async def _controlled_command_async(
    args: tuple[str, ...],
    *,
    input_data: bytes | None = None,
    docker_endpoint: str | None = None,
    direct: bool = False,
    timeout_message: str,
    output_message: str,
    failure_message: str,
) -> str:
    return await _run_controlled_command_async(
        args,
        input_data=input_data,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message=timeout_message,
        output_message=output_message,
        failure_message=failure_message,
        runtime=_current_controlled_async_runtime(),
    )


def _make_bound_controlled_command_async(runtime: _ControlledAsyncRuntime) -> Callable[..., Awaitable[str]]:
    run_command = _run_controlled_command_async

    async def command(
        args: tuple[str, ...],
        *,
        input_data: bytes | None = None,
        docker_endpoint: str | None = None,
        direct: bool = False,
        timeout_message: str,
        output_message: str,
        failure_message: str,
    ) -> str:
        return await run_command(
            args,
            input_data=input_data,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message=timeout_message,
            output_message=output_message,
            failure_message=failure_message,
            runtime=runtime,
        )

    return command


async def _compose_read_async(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
    return await _controlled_command_async(
        args,
        docker_endpoint=docker_endpoint,
        direct=direct,
        timeout_message="The controlled uData stack identity check timed out.",
        output_message="The controlled uData stack identity check returned too much output.",
        failure_message="The controlled uData stack identity check failed.",
    )


def _controlled_patch_response(
    credential: UDataCredential,
    body: Mapping[str, object],
    *,
    command: Callable[..., str] | None = None,
    source_verifier: Callable[..., _ControlledSourceIdentity] | None = None,
    response_parser: Callable[[str], RuntimeResponse] | None = None,
    patch_program: str | None = None,
    source_identity: _ControlledSourceIdentity | None = None,
) -> RuntimeResponse:
    runner = _controlled_command if command is None else command
    verify_source = _verify_controlled_source_and_nonce if source_verifier is None else source_verifier
    parse_response = _parse_controlled_patch_output if response_parser is None else response_parser
    program = _CONTROLLED_PATCH_PROGRAM if patch_program is None else patch_program
    identity = source_identity if source_identity is not None else verify_source()
    token = credential.api_key.reveal() if isinstance(credential.api_key, SecretValue) else credential.api_key
    input_data = json.dumps({"token": token, "body": dict(body)}, separators=(",", ":"), allow_nan=False).encode()
    output = runner(
        ("exec", "-i", identity.udata_container_id, "python", "-c", program),
        input_data=input_data,
        docker_endpoint=identity.docker_endpoint,
        direct=True,
        timeout_message="The controlled uData site PATCH timed out.",
        output_message="The controlled uData site PATCH returned too much output.",
        failure_message="The controlled uData site PATCH process failed.",
    )
    return parse_response(output)


def _parse_controlled_patch_output(
    output: str, error_factory: Callable[[str], CatalogValidationError] = _controlled_error
) -> RuntimeResponse:
    try:
        result = json.loads(output)
    except (TypeError, ValueError):
        raise error_factory("The controlled uData site PATCH returned invalid output.") from None
    if not isinstance(result, Mapping):
        raise error_factory("The controlled uData site PATCH returned invalid output.")
    status = result.get("status")
    content_type = result.get("content_type")
    location = result.get("location")
    response_body = result.get("body")
    if (
        type(status) is not int
        or not 100 <= status <= 599
        or not isinstance(content_type, str)
        or not isinstance(location, str)
        or not isinstance(response_body, str)
    ):
        raise error_factory("The controlled uData site PATCH returned invalid output.")
    headers: dict[str, str] = {"Content-Type": content_type}
    if location:
        headers["Location"] = location
    encoded_body = response_body.encode()
    if len(encoded_body) > 8192:
        raise error_factory("The controlled uData site PATCH returned too much output.")
    return RuntimeResponse(status, headers, encoded_body)


async def _controlled_patch_response_async(
    credential: UDataCredential,
    body: Mapping[str, object],
    *,
    command: Callable[..., Awaitable[str]] | None = None,
    source_verifier: Callable[..., Awaitable[_ControlledSourceIdentity]] | None = None,
    response_parser: Callable[[str], RuntimeResponse] | None = None,
    patch_program: str | None = None,
    source_identity: _ControlledSourceIdentity | None = None,
) -> RuntimeResponse:
    runner = _controlled_command_async if command is None else command
    verify_source = _verify_controlled_source_and_nonce_async if source_verifier is None else source_verifier
    parse_response = _parse_controlled_patch_output if response_parser is None else response_parser
    program = _CONTROLLED_PATCH_PROGRAM if patch_program is None else patch_program
    identity = source_identity if source_identity is not None else await verify_source()
    token = credential.api_key.reveal() if isinstance(credential.api_key, SecretValue) else credential.api_key
    input_data = json.dumps({"token": token, "body": dict(body)}, separators=(",", ":"), allow_nan=False).encode()
    output = await runner(
        ("exec", "-i", identity.udata_container_id, "python", "-c", program),
        input_data=input_data,
        docker_endpoint=identity.docker_endpoint,
        direct=True,
        timeout_message="The controlled uData site PATCH timed out.",
        output_message="The controlled uData site PATCH returned too much output.",
        failure_message="The controlled uData site PATCH process failed.",
    )
    return parse_response(output)


def _controlled_source_nonce(
    *,
    nonce_getter: Callable[..., str | None] = os.environ.get,
    compose_file: Path = _CONTROLLED_COMPOSE_FILE,
    evidence_root: Path = _CONTROLLED_EVIDENCE_ROOT,
    compose_sha256: str = _CONTROLLED_COMPOSE_SHA256,
    dockerfile_sha256: str = _CONTROLLED_DOCKERFILE_SHA256,
    error_factory: Callable[[str], CatalogValidationError] = _controlled_error,
) -> str:
    nonce = nonce_getter("UDATA_EVIDENCE_STACK_NONCE")
    if not nonce:
        raise error_factory("The controlled uData stack nonce is unavailable.")
    try:
        compose_text = compose_file.read_text(encoding="utf-8")
        dockerfile_text = evidence_root.joinpath("Dockerfile").read_text(encoding="utf-8")
    except OSError as error:
        raise error_factory("The controlled uData source identity is unavailable.") from error
    if hashlib.sha256(compose_text.encode()).hexdigest() != compose_sha256:
        raise error_factory("The controlled uData compose identity does not match the approved source.")
    if hashlib.sha256(dockerfile_text.encode()).hexdigest() != dockerfile_sha256:
        raise error_factory("The controlled uData image identity does not match the approved source.")
    return nonce


def _validate_controlled_context_endpoint(
    context_endpoint: str, error_factory: Callable[[str], CatalogValidationError] = _controlled_error
) -> str:
    try:
        endpoint = json.loads(context_endpoint)
    except (TypeError, ValueError):
        raise error_factory("The active Docker context identity is invalid.") from None
    parsed_endpoint = urlsplit(endpoint) if isinstance(endpoint, str) else None
    if (
        parsed_endpoint is None
        or parsed_endpoint.scheme != "unix"
        or bool(parsed_endpoint.netloc)
        or not parsed_endpoint.path.startswith("/")
        or bool(parsed_endpoint.query)
        or bool(parsed_endpoint.fragment)
    ):
        raise error_factory("Controlled uData evidence requires a local Unix Docker context.")
    return endpoint


def _parse_controlled_json_fields(
    output: str, field_count: int, error_factory: Callable[[str], CatalogValidationError]
) -> tuple[object, ...]:
    fields = output.split(" ", field_count - 1)
    if len(fields) != field_count:
        raise error_factory("The controlled uData image identity output is invalid.")
    try:
        return tuple(json.loads(field) for field in fields)
    except (TypeError, ValueError):
        raise error_factory("The controlled uData image identity output is invalid.") from None


def _controlled_service_image_identity(
    read: Callable[..., str],
    service: str,
    docker_endpoint: str,
    expected_image: str | None,
    udata_image_repository: str,
    error_factory: Callable[[str], CatalogValidationError],
) -> tuple[str, str]:
    container_output = read("ps", "-q", service, docker_endpoint=docker_endpoint)
    container_ids = [value for value in container_output.splitlines() if value]
    if len(container_ids) != 1 or re.fullmatch(r"[0-9a-f]{64}", container_ids[0]) is None:
        raise error_factory("The controlled uData service container identity is invalid.")
    container_id = container_ids[0]
    container_fields = _parse_controlled_json_fields(
        read(
            "inspect",
            "--format",
            "{{json .Id}} {{json .Image}} {{json .Config.Image}}",
            container_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        3,
        error_factory,
    )
    inspected_container_id, image_id, config_image = container_fields
    if (
        inspected_container_id != container_id
        or not isinstance(image_id, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id)
        or not isinstance(config_image, str)
    ):
        raise error_factory("The controlled uData container image identity is invalid.")
    if service == "udata":
        if config_image != udata_image_repository:
            raise error_factory("The controlled uData service image is not the approved build.")
    elif config_image != expected_image:
        raise error_factory("A controlled uData dependency image is not approved.")
    image_fields = _parse_controlled_json_fields(
        read(
            "image",
            "inspect",
            "--format",
            "{{json .Id}} {{json .RepoDigests}}",
            image_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        2,
        error_factory,
    )
    inspected_image_id, repository_digests = image_fields
    if not isinstance(repository_digests, list) or not all(isinstance(value, str) for value in repository_digests):
        raise error_factory("The controlled uData image digest output is invalid.")
    if inspected_image_id != image_id:
        raise error_factory("The controlled uData image ID does not match its container.")
    if service == "udata":
        expected_repository_digest = f"{udata_image_repository}@{image_id}"
    else:
        if not isinstance(expected_image, str) or "@" not in expected_image:
            raise error_factory("The controlled uData dependency image allowlist is invalid.")
        expected_repository_digest = expected_image
        expected_digest = expected_image.rsplit("@sha256:", 1)[1]
        expected_image_id = f"sha256:{expected_digest}"
        if image_id != expected_image_id:
            raise error_factory("The controlled uData dependency image ID is not approved.")
    if expected_repository_digest not in repository_digests:
        raise error_factory("The controlled uData image repository digest is not approved.")
    return container_id, f"{service}|{config_image}|{image_id}|{expected_repository_digest}"


async def _controlled_service_image_identity_async(
    read: Callable[..., Awaitable[str]],
    service: str,
    docker_endpoint: str,
    expected_image: str | None,
    udata_image_repository: str,
    error_factory: Callable[[str], CatalogValidationError],
) -> tuple[str, str]:
    container_output = await read("ps", "-q", service, docker_endpoint=docker_endpoint)
    container_ids = [value for value in container_output.splitlines() if value]
    if len(container_ids) != 1 or re.fullmatch(r"[0-9a-f]{64}", container_ids[0]) is None:
        raise error_factory("The controlled uData service container identity is invalid.")
    container_id = container_ids[0]
    container_fields = _parse_controlled_json_fields(
        await read(
            "inspect",
            "--format",
            "{{json .Id}} {{json .Image}} {{json .Config.Image}}",
            container_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        3,
        error_factory,
    )
    inspected_container_id, image_id, config_image = container_fields
    if (
        inspected_container_id != container_id
        or not isinstance(image_id, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id)
        or not isinstance(config_image, str)
    ):
        raise error_factory("The controlled uData container image identity is invalid.")
    if service == "udata":
        if config_image != udata_image_repository:
            raise error_factory("The controlled uData service image is not the approved build.")
    elif config_image != expected_image:
        raise error_factory("A controlled uData dependency image is not approved.")
    image_fields = _parse_controlled_json_fields(
        await read(
            "image",
            "inspect",
            "--format",
            "{{json .Id}} {{json .RepoDigests}}",
            image_id,
            docker_endpoint=docker_endpoint,
            direct=True,
        ),
        2,
        error_factory,
    )
    inspected_image_id, repository_digests = image_fields
    if not isinstance(repository_digests, list) or not all(isinstance(value, str) for value in repository_digests):
        raise error_factory("The controlled uData image digest output is invalid.")
    if inspected_image_id != image_id:
        raise error_factory("The controlled uData image ID does not match its container.")
    if service == "udata":
        expected_repository_digest = f"{udata_image_repository}@{image_id}"
    else:
        if not isinstance(expected_image, str) or "@" not in expected_image:
            raise error_factory("The controlled uData dependency image allowlist is invalid.")
        expected_repository_digest = expected_image
        expected_digest = expected_image.rsplit("@sha256:", 1)[1]
        expected_image_id = f"sha256:{expected_digest}"
        if image_id != expected_image_id:
            raise error_factory("The controlled uData dependency image ID is not approved.")
    if expected_repository_digest not in repository_digests:
        raise error_factory("The controlled uData image repository digest is not approved.")
    return container_id, f"{service}|{config_image}|{image_id}|{expected_repository_digest}"


def _verify_controlled_source_and_nonce(
    *,
    compose_read: Callable[..., str] | None = None,
    source_nonce: Callable[[], str] | None = None,
    context_validator: Callable[[str], str] | None = None,
    source_commit: str = _CONTROLLED_SOURCE_COMMIT,
    service_names: tuple[str, ...] = _CONTROLLED_SERVICE_NAMES,
    expected_services: frozenset[str] = frozenset(_CONTROLLED_SERVICE_NAMES),
    dependency_image_specs: tuple[tuple[str, str], ...] = _CONTROLLED_DEPENDENCY_IMAGE_SPECS,
    udata_image_repository: str = _CONTROLLED_UDATA_IMAGE_REPOSITORY,
    expected_port: str = "127.0.0.1:5640",
    error_factory: Callable[[str], CatalogValidationError] = _controlled_error,
    docker_endpoint: str | None = None,
) -> _ControlledSourceIdentity:
    read = _compose_read if compose_read is None else compose_read
    nonce_reader = _controlled_source_nonce if source_nonce is None else source_nonce
    validate_context = _validate_controlled_context_endpoint if context_validator is None else context_validator
    nonce = nonce_reader()
    if docker_endpoint is None:
        context_endpoint = read("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
        docker_endpoint = validate_context(context_endpoint)
    else:
        validate_context(json.dumps(docker_endpoint))
    services = read("ps", "--status", "running", "--services", docker_endpoint=docker_endpoint).splitlines()
    if len(services) != len(expected_services) or set(services) != expected_services:
        raise error_factory("The running services do not match the controlled uData stack.")
    if read("port", "udata", "7000", docker_endpoint=docker_endpoint) != expected_port:
        raise error_factory("The controlled uData service is not bound to the approved loopback port.")
    dependency_images = dict(dependency_image_specs)
    image_identities: list[str] = []
    udata_container_id = ""
    for service in service_names:
        container_id, image_identity = _controlled_service_image_identity(
            read,
            service,
            docker_endpoint,
            dependency_images.get(service),
            udata_image_repository,
            error_factory,
        )
        image_identities.append(image_identity)
        if service == "udata":
            udata_container_id = container_id
    if (
        read(
            "exec",
            udata_container_id,
            "git",
            "-C",
            "/opt/udata",
            "rev-parse",
            "HEAD",
            docker_endpoint=docker_endpoint,
            direct=True,
        )
        != source_commit
    ):
        raise error_factory("The controlled uData service source does not match the approved commit.")
    if (
        read(
            "exec",
            udata_container_id,
            "printenv",
            "UDATA_EVIDENCE_STACK_NONCE",
            docker_endpoint=docker_endpoint,
            direct=True,
        )
        != nonce
    ):
        raise error_factory("The controlled uData service nonce does not match the local evidence nonce.")
    return _ControlledSourceIdentity(
        nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
        docker_endpoint=docker_endpoint,
        udata_container_id=udata_container_id,
        image_identities=tuple(image_identities),
    )


async def _verify_controlled_source_and_nonce_async(
    *,
    compose_read: Callable[..., Awaitable[str]] | None = None,
    source_nonce: Callable[[], str] | None = None,
    context_validator: Callable[[str], str] | None = None,
    source_commit: str = _CONTROLLED_SOURCE_COMMIT,
    service_names: tuple[str, ...] = _CONTROLLED_SERVICE_NAMES,
    expected_services: frozenset[str] = frozenset(_CONTROLLED_SERVICE_NAMES),
    dependency_image_specs: tuple[tuple[str, str], ...] = _CONTROLLED_DEPENDENCY_IMAGE_SPECS,
    udata_image_repository: str = _CONTROLLED_UDATA_IMAGE_REPOSITORY,
    expected_port: str = "127.0.0.1:5640",
    error_factory: Callable[[str], CatalogValidationError] = _controlled_error,
    docker_endpoint: str | None = None,
) -> _ControlledSourceIdentity:
    read = _compose_read_async if compose_read is None else compose_read
    nonce_reader = _controlled_source_nonce if source_nonce is None else source_nonce
    validate_context = _validate_controlled_context_endpoint if context_validator is None else context_validator
    nonce = nonce_reader()
    if docker_endpoint is None:
        context_endpoint = await read("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}")
        docker_endpoint = validate_context(context_endpoint)
    else:
        validate_context(json.dumps(docker_endpoint))
    services = (await read("ps", "--status", "running", "--services", docker_endpoint=docker_endpoint)).splitlines()
    if len(services) != len(expected_services) or set(services) != expected_services:
        raise error_factory("The running services do not match the controlled uData stack.")
    if await read("port", "udata", "7000", docker_endpoint=docker_endpoint) != expected_port:
        raise error_factory("The controlled uData service is not bound to the approved loopback port.")
    dependency_images = dict(dependency_image_specs)
    image_identities: list[str] = []
    udata_container_id = ""
    for service in service_names:
        container_id, image_identity = await _controlled_service_image_identity_async(
            read,
            service,
            docker_endpoint,
            dependency_images.get(service),
            udata_image_repository,
            error_factory,
        )
        image_identities.append(image_identity)
        if service == "udata":
            udata_container_id = container_id
    if (
        await read(
            "exec",
            udata_container_id,
            "git",
            "-C",
            "/opt/udata",
            "rev-parse",
            "HEAD",
            docker_endpoint=docker_endpoint,
            direct=True,
        )
        != source_commit
    ):
        raise error_factory("The controlled uData service source does not match the approved commit.")
    if (
        await read(
            "exec",
            udata_container_id,
            "printenv",
            "UDATA_EVIDENCE_STACK_NONCE",
            docker_endpoint=docker_endpoint,
            direct=True,
        )
        != nonce
    ):
        raise error_factory("The controlled uData service nonce does not match the local evidence nonce.")
    return _ControlledSourceIdentity(
        nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
        docker_endpoint=docker_endpoint,
        udata_container_id=udata_container_id,
        image_identities=tuple(image_identities),
    )


def _controlled_peer_evidence(
    response: RuntimeResponse, error_factory: Callable[[str], CatalogValidationError] = _controlled_error
) -> str:
    content_type = _response_header(response, "content-type")
    if (
        not 200 <= response.status_code < 300
        or content_type is None
        or content_type.split(";", 1)[0] != "application/json"
    ):
        raise error_factory("The controlled uData peer did not return its expected site identity.")
    invalid_payload = False
    try:
        payload = json.loads(response.body)
    except (TypeError, ValueError):
        invalid_payload = True
        payload = None
    if invalid_payload:
        raise error_factory("The controlled uData peer returned an invalid site identity.")
    if (
        not isinstance(payload, Mapping)
        or not isinstance(payload.get("id"), str)
        or not payload["id"]
        or payload.get("version") != "17.6.0"
    ):
        raise error_factory("The controlled uData peer did not match the seeded site identity.")
    return payload["id"]


def _verify_controlled_sync_stack(
    transport: CatalogTransport,
    *,
    source_verifier: Callable[..., _ControlledSourceIdentity] | None = None,
    peer_evidence: Callable[[RuntimeResponse], str] | None = None,
    site_probe: Callable[[str, str], RuntimeResponse] | None = None,
    controlled_origin: str = _CONTROLLED_ORIGIN,
    docker_endpoint: str | None = None,
) -> _ControlledVerification:
    verify_source = _verify_controlled_source_and_nonce if source_verifier is None else source_verifier
    decode_peer = _controlled_peer_evidence if peer_evidence is None else peer_evidence
    identity = verify_source(docker_endpoint=docker_endpoint)
    response = (
        transport.send(
            RuntimeRequest(
                method="GET",
                url=f"{controlled_origin}/api/1/site/",
                headers={},
                redirect_policy=RedirectPolicy.NO_FOLLOW,
                max_response_bytes=8192,
            )
        )
        if site_probe is None
        else site_probe(identity.docker_endpoint, identity.udata_container_id)
    )
    evidence = _ControlledStackEvidence(
        nonce_sha256=identity.nonce_sha256,
        site_id=decode_peer(response),
        docker_endpoint_sha256=hashlib.sha256(identity.docker_endpoint.encode()).hexdigest(),
        image_digests=identity.image_identities,
    )
    return _ControlledVerification(identity=identity, evidence=evidence)


async def _verify_controlled_async_stack(
    transport: AsyncCatalogTransport,
    *,
    source_verifier: Callable[..., Awaitable[_ControlledSourceIdentity]] | None = None,
    peer_evidence: Callable[[RuntimeResponse], str] | None = None,
    site_probe: Callable[[str, str], Awaitable[RuntimeResponse]] | None = None,
    controlled_origin: str = _CONTROLLED_ORIGIN,
    docker_endpoint: str | None = None,
) -> _ControlledVerification:
    verify_source = _verify_controlled_source_and_nonce_async if source_verifier is None else source_verifier
    decode_peer = _controlled_peer_evidence if peer_evidence is None else peer_evidence
    identity = await verify_source(docker_endpoint=docker_endpoint)
    response = (
        await transport.send(
            RuntimeRequest(
                method="GET",
                url=f"{controlled_origin}/api/1/site/",
                headers={},
                redirect_policy=RedirectPolicy.NO_FOLLOW,
                max_response_bytes=8192,
            )
        )
        if site_probe is None
        else await site_probe(identity.docker_endpoint, identity.udata_container_id)
    )
    evidence = _ControlledStackEvidence(
        nonce_sha256=identity.nonce_sha256,
        site_id=decode_peer(response),
        docker_endpoint_sha256=hashlib.sha256(identity.docker_endpoint.encode()).hexdigest(),
        image_digests=identity.image_identities,
    )
    return _ControlledVerification(identity=identity, evidence=evidence)


@dataclass(frozen=True, slots=True)
class _ControlledSyncOperations:
    verify: Callable[..., _ControlledVerification]
    dispatch: Callable[..., RuntimeResponse]


@dataclass(frozen=True, slots=True)
class _ControlledAsyncOperations:
    verify: Callable[..., Awaitable[_ControlledVerification]]
    dispatch: Callable[..., Awaitable[RuntimeResponse]]


def _make_controlled_compose_read(command: Callable[..., str]) -> Callable[..., str]:
    def compose_read(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
        return command(
            args,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message="The controlled uData stack identity check timed out.",
            output_message="The controlled uData stack identity check returned too much output.",
            failure_message="The controlled uData stack identity check failed.",
        )

    return compose_read


def _make_controlled_compose_read_async(command: Callable[..., Awaitable[str]]) -> Callable[..., Awaitable[str]]:
    async def compose_read(*args: str, docker_endpoint: str | None = None, direct: bool = False) -> str:
        return await command(
            args,
            docker_endpoint=docker_endpoint,
            direct=direct,
            timeout_message="The controlled uData stack identity check timed out.",
            output_message="The controlled uData stack identity check returned too much output.",
            failure_message="The controlled uData stack identity check failed.",
        )

    return compose_read


def _make_controlled_site_probe(
    command: Callable[..., str], response_parser: Callable[[str], RuntimeResponse], program: str
) -> Callable[[str, str], RuntimeResponse]:
    def probe(docker_endpoint: str, container_id: str) -> RuntimeResponse:
        output = command(
            ("exec", "-i", container_id, "python", "-c", program),
            docker_endpoint=docker_endpoint,
            direct=True,
            timeout_message="The controlled uData stack identity check timed out.",
            output_message="The controlled uData stack identity check returned too much output.",
            failure_message="The controlled uData stack identity check failed.",
        )
        return response_parser(output)

    return probe


def _make_controlled_site_probe_async(
    command: Callable[..., Awaitable[str]], response_parser: Callable[[str], RuntimeResponse], program: str
) -> Callable[[str, str], Awaitable[RuntimeResponse]]:
    async def probe(docker_endpoint: str, container_id: str) -> RuntimeResponse:
        output = await command(
            ("exec", "-i", container_id, "python", "-c", program),
            docker_endpoint=docker_endpoint,
            direct=True,
            timeout_message="The controlled uData stack identity check timed out.",
            output_message="The controlled uData stack identity check returned too much output.",
            failure_message="The controlled uData stack identity check failed.",
        )
        return response_parser(output)

    return probe


def _make_controlled_sync_operations(command: Callable[..., str]) -> _ControlledSyncOperations:
    compose_read = _make_controlled_compose_read(command)
    source_checker = _verify_controlled_source_and_nonce
    source_nonce = _controlled_source_nonce
    context_validator = _validate_controlled_context_endpoint
    stack_verifier = _verify_controlled_sync_stack
    peer_evidence = _controlled_peer_evidence
    patch_response = _controlled_patch_response
    response_parser = _parse_controlled_patch_output
    site_probe = _make_controlled_site_probe(command, response_parser, _CONTROLLED_SITE_PROBE_PROGRAM)
    patch_program = _CONTROLLED_PATCH_PROGRAM

    def verify_source(*, docker_endpoint: str | None = None) -> _ControlledSourceIdentity:
        return source_checker(
            compose_read=compose_read,
            source_nonce=source_nonce,
            context_validator=context_validator,
            docker_endpoint=docker_endpoint,
        )

    def verify(transport: CatalogTransport, *, docker_endpoint: str | None = None) -> _ControlledVerification:
        return stack_verifier(
            transport,
            source_verifier=verify_source,
            peer_evidence=peer_evidence,
            site_probe=site_probe,
            docker_endpoint=docker_endpoint,
        )

    def dispatch(
        transport: CatalogTransport,
        credential: UDataCredential,
        body: Mapping[str, object],
        *,
        docker_endpoint: str,
    ) -> RuntimeResponse:
        verification = verify(transport, docker_endpoint=docker_endpoint)
        return patch_response(
            credential,
            body,
            command=command,
            response_parser=response_parser,
            patch_program=patch_program,
            source_identity=verification.identity,
        )

    return _ControlledSyncOperations(verify=verify, dispatch=dispatch)


def _make_controlled_async_operations(command: Callable[..., Awaitable[str]]) -> _ControlledAsyncOperations:
    compose_read = _make_controlled_compose_read_async(command)
    source_checker = _verify_controlled_source_and_nonce_async
    source_nonce = _controlled_source_nonce
    context_validator = _validate_controlled_context_endpoint
    stack_verifier = _verify_controlled_async_stack
    peer_evidence = _controlled_peer_evidence
    patch_response = _controlled_patch_response_async
    response_parser = _parse_controlled_patch_output
    site_probe = _make_controlled_site_probe_async(command, response_parser, _CONTROLLED_SITE_PROBE_PROGRAM)
    patch_program = _CONTROLLED_PATCH_PROGRAM

    async def verify_source(*, docker_endpoint: str | None = None) -> _ControlledSourceIdentity:
        return await source_checker(
            compose_read=compose_read,
            source_nonce=source_nonce,
            context_validator=context_validator,
            docker_endpoint=docker_endpoint,
        )

    async def verify(
        transport: AsyncCatalogTransport, *, docker_endpoint: str | None = None
    ) -> _ControlledVerification:
        return await stack_verifier(
            transport,
            source_verifier=verify_source,
            peer_evidence=peer_evidence,
            site_probe=site_probe,
            docker_endpoint=docker_endpoint,
        )

    async def dispatch(
        transport: AsyncCatalogTransport,
        credential: UDataCredential,
        body: Mapping[str, object],
        *,
        docker_endpoint: str,
    ) -> RuntimeResponse:
        verification = await verify(transport, docker_endpoint=docker_endpoint)
        return await patch_response(
            credential,
            body,
            command=command,
            response_parser=response_parser,
            patch_program=patch_program,
            source_identity=verification.identity,
        )

    return _ControlledAsyncOperations(verify=verify, dispatch=dispatch)


class _ImmutableDispatchGateType(type):
    def __new__(
        mcls, name: str, bases: tuple[type, ...], namespace: dict[str, object], **kwargs: object
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
            raise AttributeError("Controlled dispatch authorization is immutable.")
        return super().__new__(mcls, name, bases, namespace, **kwargs)

    def __setattr__(cls, name: str, value: object) -> None:
        if name in {
            "authorize_sync",
            "authorize_async",
            "dispatch_sync",
            "dispatch_async",
            "bind_sync_client",
            "bind_async_client",
        }:
            raise AttributeError("Controlled dispatch authorization is immutable.")
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
            raise AttributeError("Controlled dispatch authorization is immutable.")
        super().__delattr__(name)


class _ImmutableClientType(type):
    def __new__(
        mcls, name: str, bases: tuple[type, ...], namespace: dict[str, object], **kwargs: object
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
            raise AttributeError("Controlled dispatch authorization is factory-owned.")
        return super().__new__(mcls, name, bases, namespace, **kwargs)

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
            raise AttributeError("Controlled dispatch authorization is factory-owned.")
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
            raise AttributeError("Controlled dispatch authorization is factory-owned.")
        super().__delattr__(name)


class _ImmutableTransportType(type):
    def __new__(
        mcls, name: str, bases: tuple[type, ...], namespace: dict[str, object], **kwargs: object
    ) -> _ImmutableTransportType:
        reserved = {
            "_factory_bindings",
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
        inherited = any("_factory_bindings" in ancestor.__dict__ for base in bases for ancestor in base.__mro__)
        if inherited and reserved.intersection(namespace):
            raise AttributeError("Controlled transport bindings are factory-owned.")
        return super().__new__(mcls, name, bases, namespace, **kwargs)

    def __setattr__(cls, name: str, value: object) -> None:
        if name in {
            "_factory_bindings",
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
            raise AttributeError("Controlled transport bindings are factory-owned.")
        super().__setattr__(name, value)

    def __delattr__(cls, name: str) -> None:
        if name in {
            "_factory_bindings",
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
            raise AttributeError("Controlled transport bindings are factory-owned.")
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


def _build_controlled_transport_types():
    type SyncState = tuple[CatalogTransport, _ControlledStackEvidence, float, _ControlledSyncOperations, str]
    type AsyncState = tuple[
        AsyncCatalogTransport, _ControlledStackEvidence | None, float, _ControlledAsyncOperations, str | None
    ]
    controlled_origin = _CONTROLLED_ORIGIN
    authority_ttl = _CONTROLLED_AUTHORITY_TTL_SECONDS
    clock = monotonic
    error_factory = _controlled_error
    trusted_command_spec = _make_bound_controlled_command_spec()
    trusted_sync_operations = _make_controlled_sync_operations(
        _make_bound_controlled_command(_current_controlled_sync_runtime(command_spec=trusted_command_spec))
    )
    trusted_async_operations = _make_controlled_async_operations(
        _make_bound_controlled_command_async(_current_controlled_async_runtime(command_spec=trusted_command_spec))
    )
    sync_registry: _IdentityRegistry[SyncState] = _IdentityRegistry()
    async_registry: _IdentityRegistry[AsyncState] = _IdentityRegistry()
    sync_client_registry: _IdentityRegistry[object] = _IdentityRegistry()
    async_client_registry: _IdentityRegistry[object] = _IdentityRegistry()

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

    class _ControlledSyncTransport(metaclass=_ImmutableTransportType):
        """Own a stock transport after live controlled-stack verification."""

        __slots__ = ("__weakref__",)
        _factory_bindings = (
            HttpxCatalogTransport,
            HttpxCatalogTransport.__init__,
            HttpxCatalogTransport.send,
            HttpxCatalogTransport.send_stream,
            HttpxCatalogTransport.close,
            trusted_sync_operations,
        )

        def __init__(self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None) -> None:
            transport_type, transport_initializer, _, _, close, operations = type(self)._factory_bindings
            transport = object.__new__(transport_type)
            try:
                cast(Callable[..., None], transport_initializer)(transport, tls_policy=tls_policy, budget=budget)
                verification = operations.verify(transport)
            except BaseException:
                cast(Callable[[HttpxCatalogTransport], None], close)(transport)
                raise
            sync_registry.set(
                self,
                (
                    transport,
                    verification.evidence,
                    clock(),
                    operations,
                    verification.identity.docker_endpoint,
                ),
            )

        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            state = sync_state(self)
            if state is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            send = cast(
                Callable[[HttpxCatalogTransport, RuntimeRequest], RuntimeResponse],
                type(self)._factory_bindings[2],
            )
            return send(cast(HttpxCatalogTransport, state[0]), request)

        def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
            state = sync_state(self)
            if state is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            send_stream = cast(
                Callable[[HttpxCatalogTransport, RuntimeRequest], RuntimeStreamResponse],
                type(self)._factory_bindings[3],
            )
            return send_stream(cast(HttpxCatalogTransport, state[0]), request)

        def close(self) -> None:
            state = sync_state(self)
            if state is not None:
                close = cast(Callable[[HttpxCatalogTransport], None], type(self)._factory_bindings[4])
                close(cast(HttpxCatalogTransport, state[0]))

    class _ControlledAsyncTransport(metaclass=_ImmutableTransportType):
        """Own a stock asynchronous transport after live controlled-stack verification."""

        __slots__ = ("__weakref__",)
        _factory_bindings = (
            AsyncHttpxCatalogTransport,
            AsyncHttpxCatalogTransport.__init__,
            AsyncHttpxCatalogTransport.send,
            AsyncHttpxCatalogTransport.send_stream,
            AsyncHttpxCatalogTransport.aclose,
            trusted_async_operations,
        )

        def __init__(self, *, tls_policy: TLSPolicy | None = None, budget: TimeBudget | None = None) -> None:
            transport_type, transport_initializer, _, _, _, operations = type(self)._factory_bindings
            transport = object.__new__(transport_type)
            cast(Callable[..., None], transport_initializer)(transport, tls_policy=tls_policy, budget=budget)
            async_registry.set(
                self,
                (
                    transport,
                    None,
                    0.0,
                    operations,
                    None,
                ),
            )

        async def verify(self) -> None:
            """Complete live verification before the transport is used for mutations."""
            state = async_state(self)
            if state is None:
                return
            try:
                verification = await state[3].verify(state[0])
            except BaseException:
                _, _, _, _, aclose, _ = type(self)._factory_bindings
                await aclose(cast(AsyncHttpxCatalogTransport, state[0]))
                async_registry.discard(self)
                raise
            async_registry.set(
                self,
                (
                    state[0],
                    verification.evidence,
                    clock(),
                    state[3],
                    verification.identity.docker_endpoint,
                ),
            )

        async def send(self, request: RuntimeRequest) -> RuntimeResponse:
            state = async_state(self)
            if state is None or state[1] is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            send = cast(
                Callable[[AsyncHttpxCatalogTransport, RuntimeRequest], Awaitable[RuntimeResponse]],
                type(self)._factory_bindings[2],
            )
            return await send(cast(AsyncHttpxCatalogTransport, state[0]), request)

        async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
            state = async_state(self)
            if state is None or state[1] is None:
                raise _controlled_error("The controlled uData transport is not factory-bound.")
            send_stream = cast(
                Callable[[AsyncHttpxCatalogTransport, RuntimeRequest], Awaitable[AsyncRuntimeStreamResponse]],
                type(self)._factory_bindings[3],
            )
            return await send_stream(cast(AsyncHttpxCatalogTransport, state[0]), request)

        async def aclose(self) -> None:
            state = async_state(self)
            if state is not None:
                aclose = cast(Callable[[AsyncHttpxCatalogTransport], Awaitable[None]], type(self)._factory_bindings[4])
                await aclose(cast(AsyncHttpxCatalogTransport, state[0]))

    def sync_site_id(value: object) -> str | None:
        state = sync_state(value)
        return state[1].site_id if state is not None else None

    def sync_evidence_digest(value: object) -> str | None:
        state = sync_state(value)
        return state[1].digest if state is not None else None

    def sync_revalidate(value: object, *, origin: str, site_id: str) -> bool:
        state = sync_state(value)
        if state is None:
            return False
        evidence = state[3].verify(state[0], docker_endpoint=state[4]).evidence
        return (
            origin == controlled_origin
            and clock() - state[2] <= authority_ttl
            and evidence == state[1]
            and evidence.site_id == site_id
        )

    def async_site_id(value: object) -> str | None:
        state = async_state(value)
        return state[1].site_id if state is not None and state[1] is not None else None

    def async_evidence_digest(value: object) -> str | None:
        state = async_state(value)
        return state[1].digest if state is not None and state[1] is not None else None

    async def async_revalidate(value: object, *, origin: str, site_id: str) -> bool:
        state = async_state(value)
        if state is None or state[1] is None:
            return False
        evidence = (await state[3].verify(state[0], docker_endpoint=state[4])).evidence
        return (
            origin == controlled_origin
            and clock() - state[2] <= authority_ttl
            and evidence == state[1]
            and evidence.site_id == site_id
        )

    class _ControlledDispatchGate(metaclass=_ImmutableDispatchGateType):
        __slots__ = ()

        def __get__(self, instance: object | None, owner: type | None = None) -> _ControlledDispatchGate:
            return self

        def __set__(self, instance: object, value: object) -> None:
            raise AttributeError("Controlled dispatch authorization is factory-owned.")

        def bind_sync_client(self, client: object, transport: object) -> None:
            if type(transport) is _ControlledSyncTransport and sync_state(transport) is not None:
                sync_client_registry.set(client, transport)

        def bind_async_client(self, client: object, transport: object) -> None:
            if type(transport) is _ControlledAsyncTransport and async_state(transport) is not None:
                async_client_registry.set(client, transport)

        def authorize_sync(
            self,
            client: object,
            transport: object,
            request: RuntimeRequest,
            *,
            origin: str,
        ) -> bool:
            if sync_client_registry.get(client) is not transport:
                return False
            if origin != controlled_origin or request.method != "PATCH":
                return False
            if request.url != f"{controlled_origin}/api/1/site/":
                return False
            state = sync_state(transport)
            if state is None:
                return False
            try:
                evidence = state[3].verify(state[0], docker_endpoint=state[4]).evidence
            except Exception:
                return False
            return clock() - state[2] <= authority_ttl and evidence == state[1]

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
            if sync_client_registry.get(client) is not transport:
                raise error_factory("The uData site PATCH is not bound to the verified controlled client.")
            if (
                origin != controlled_origin
                or request.method != "PATCH"
                or request.url != f"{controlled_origin}/api/1/site/"
            ):
                raise error_factory("The uData site PATCH is not bound to the verified controlled client.")
            state = sync_state(transport)
            if state is None or not isinstance(credential, UDataCredential) or not isinstance(body, Mapping):
                raise error_factory("The controlled uData site PATCH requires a resolved credential and body.")
            return state[3].dispatch(state[0], credential, body, docker_endpoint=state[4])

        async def authorize_async(
            self,
            client: object,
            transport: object,
            request: RuntimeRequest,
            *,
            origin: str,
        ) -> bool:
            if async_client_registry.get(client) is not transport:
                return False
            if origin != controlled_origin or request.method != "PATCH":
                return False
            if request.url != f"{controlled_origin}/api/1/site/":
                return False
            state = async_state(transport)
            if state is None or state[1] is None:
                return False
            try:
                evidence = (await state[3].verify(state[0], docker_endpoint=state[4])).evidence
            except Exception:
                return False
            return clock() - state[2] <= authority_ttl and evidence == state[1]

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
            if async_client_registry.get(client) is not transport:
                raise error_factory("The uData site PATCH is not bound to the verified controlled client.")
            if (
                origin != controlled_origin
                or request.method != "PATCH"
                or request.url != f"{controlled_origin}/api/1/site/"
            ):
                raise error_factory("The uData site PATCH is not bound to the verified controlled client.")
            state = async_state(transport)
            if state is None or not isinstance(credential, UDataCredential) or not isinstance(body, Mapping):
                raise error_factory("The controlled uData site PATCH requires a resolved credential and body.")
            if state[4] is None:
                raise error_factory("The controlled uData transport is not factory-bound.")
            return await state[3].dispatch(state[0], credential, body, docker_endpoint=state[4])

    return (
        _ControlledSyncTransport,
        _ControlledAsyncTransport,
        sync_site_id,
        sync_evidence_digest,
        sync_revalidate,
        async_site_id,
        async_evidence_digest,
        async_revalidate,
        _ControlledDispatchGate(),
    )


(
    _ControlledSyncTransport,
    _ControlledAsyncTransport,
    _controlled_sync_site_id,
    _controlled_sync_evidence_digest,
    _controlled_sync_revalidate,
    _controlled_async_site_id,
    _controlled_async_evidence_digest,
    _controlled_async_revalidate,
    _controlled_dispatch_gate,
) = _build_controlled_transport_types()


class _UDataClientCore(metaclass=_ImmutableClientType):
    """Shared strict-gate state for the sync and async uData clients."""

    _mutation_dispatch_gate = _controlled_dispatch_gate

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
        invalid_payload = False
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError):
            invalid_payload = True
            payload = None
        if invalid_payload:
            raise NativeCatalogError(
                "Catalog operation returned an invalid JSON result.",
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
                if owning_operation == SET_SITE_OPERATION:
                    response = self._mutation_dispatch_gate.dispatch_sync(
                        self,
                        self._transport,
                        request,
                        resolved_credential,
                        json_body,
                        origin=self._origin,
                    )
                else:
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
                invalid_payload = False
                try:
                    payload = json.loads(response.body)
                except (TypeError, ValueError):
                    invalid_payload = True
                    payload = None
                if invalid_payload:
                    raise NativeCatalogError(
                        "Catalog operation returned an invalid JSON result.",
                        operation=str(owning_id),
                        platform=PLATFORM.value,
                        status_code=response.status_code,
                        metadata={"ambiguous": json_body is not None and method != "GET"},
                    )
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
            except BaseException as error:
                self._emit(owning_id, "failed")
                try:
                    response.close()
                except BaseException as cleanup_error:
                    raise error from cleanup_error
                raise error
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
            primary_error: BaseException | None = None
            try:
                stream_chunks.close()
            except BaseException as error:
                primary_error = error
                settle_failure(error)
            cleanup_error: BaseException | None = None
            try:
                response.close()
            except BaseException as error:
                cleanup_error = error
            if primary_error is not None:
                if cleanup_error is not None:
                    raise primary_error from cleanup_error
                raise primary_error
            if cleanup_error is not None:
                settle_failure(cleanup_error)
                raise cleanup_error
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
                if owning_operation == SET_SITE_OPERATION:
                    response = await self._mutation_dispatch_gate.dispatch_async(
                        self,
                        self._transport,
                        request,
                        resolved_credential,
                        json_body,
                        origin=self._origin,
                    )
                else:
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
                invalid_payload = False
                try:
                    payload = json.loads(response.body)
                except (TypeError, ValueError):
                    invalid_payload = True
                    payload = None
                if invalid_payload:
                    raise NativeCatalogError(
                        "Catalog operation returned an invalid JSON result.",
                        operation=str(owning_id),
                        platform=PLATFORM.value,
                        status_code=response.status_code,
                        metadata={"ambiguous": json_body is not None and method != "GET"},
                    )
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
            except BaseException as error:
                self._emit(owning_id, "failed")
                try:
                    await response.aclose()
                except BaseException as cleanup_error:
                    raise error from cleanup_error
                raise error
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
            primary_error: BaseException | None = None
            try:
                await stream_chunks.aclose()
            except BaseException as error:
                primary_error = error
                settle_failure(error)
            cleanup_error: BaseException | None = None
            try:
                await response.aclose()
            except BaseException as error:
                cleanup_error = error
            if primary_error is not None:
                if cleanup_error is not None:
                    raise primary_error from cleanup_error
                raise primary_error
            if cleanup_error is not None:
                settle_failure(cleanup_error)
                raise cleanup_error
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


def _require_controlled_settings(settings: UDataClientSettings, controlled_origin: str = _CONTROLLED_ORIGIN) -> None:
    if settings.base_url != controlled_origin:
        raise _controlled_error("Controlled uData mutations require the approved loopback origin.")
    if settings.sync_transport is not None or settings.async_transport is not None:
        raise _controlled_error("Controlled uData mutations cannot use caller-provided transports.")


def _create_controlled_sync_client(settings: UDataClientSettings) -> SyncUDataClient:
    """Construct a stock synchronous client with a live-verified mutation authority."""
    require_extra("udata")
    _require_controlled_settings(settings)
    transport = _ControlledSyncTransport(tls_policy=settings.tls_policy, budget=settings.budget)
    client = SyncUDataClient(
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
    client._mutation_dispatch_gate.bind_sync_client(client, transport)
    return client


async def _create_controlled_async_client(settings: UDataClientSettings) -> AsyncUDataClient:
    """Construct a stock asynchronous client with a live-verified mutation authority."""
    require_extra("udata")
    _require_controlled_settings(settings)
    transport = _ControlledAsyncTransport(tls_policy=settings.tls_policy, budget=settings.budget)
    await transport.verify()
    client = AsyncUDataClient(
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
    client._mutation_dispatch_gate.bind_async_client(client, transport)
    return client


def _load_services():
    from datasluice.connectors.catalog.udata.services.datasets import AsyncDatasetsService, SyncDatasetsService
    from datasluice.connectors.catalog.udata.services.root_profile import (
        AsyncRootProfileService,
        SyncRootProfileService,
    )

    return AsyncDatasetsService, SyncDatasetsService, AsyncRootProfileService, SyncRootProfileService


AsyncDatasetsService, SyncDatasetsService, AsyncRootProfileService, SyncRootProfileService = _load_services()
