"""Exact-wire and safety evidence for the uData root-profile family."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncIterator, Callable, Generator, Mapping
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import httpx
import pytest

from datasluice.connectors.catalog.udata import clients as udata_clients
from datasluice.connectors.catalog.udata.clients import (
    AsyncUDataClient,
    SyncUDataClient,
    _create_controlled_sync_client,
    create_async_client,
    create_sync_client,
    declared_udata_profile,
)
from datasluice.connectors.catalog.udata.models.root_profile import (
    SiteCatalogQuery,
    SiteDataserviceCsvQuery,
    SiteDatasetCsvQuery,
    SiteDocument,
    SiteMutationResult,
    SiteOrganizationCsvQuery,
    SitePatchInput,
    SiteProfile,
    SiteReuseCsvQuery,
)
from datasluice.connectors.catalog.udata.probes import UDataVersionError
from datasluice.connectors.catalog.udata.settings import UDataClientSettings
from datasluice.connectors.catalog.udata.wire import root_profile as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
)
from datasluice.runtime.events import EventEmitter
from datasluice.runtime.transport.base import (
    AsyncRuntimeStreamResponse,
    RuntimeRequest,
    RuntimeResponse,
    RuntimeStreamResponse,
    TransportFailure,
)

_ORIGIN = "http://127.0.0.1:5640"
_SITE_URL = f"{_ORIGIN}/api/1/site/"
_CREDENTIAL = UDataCredential(api_key="site-key")
_PERMISSIONS = EffectivePermissions.for_credential(
    _CREDENTIAL, platform=CatalogPlatform.UDATA, roles=frozenset({"admin"})
)


def _controlled_evidence(site_id: str = "site", *, nonce: str = "unit-test-stack") -> Any:
    return udata_clients._ControlledStackEvidence(
        nonce_sha256=hashlib.sha256(nonce.encode()).hexdigest(),
        site_id=site_id,
        docker_endpoint_sha256=hashlib.sha256(b"unix:///Users/nitish/.docker/run/docker.sock").hexdigest(),
    )


def _controlled_compose_files(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    compose_file = tmp_path / "compose.yaml"
    env_file = tmp_path / ".env"
    compose_file.write_text("compose", encoding="utf-8")
    env_file.write_text("env", encoding="utf-8")
    monkeypatch.setattr(udata_clients, "_CONTROLLED_COMPOSE_FILE", compose_file)
    monkeypatch.setattr(udata_clients, "_CONTROLLED_ENV_FILE", env_file)


def test_controlled_command_reaps_a_process_after_pipe_eof(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    reads = iter((b"ok", b""))
    waits = iter(((0, 0), (7, 0)))

    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "read", lambda *_: next(reads))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: next(waits))
    monkeypatch.setattr(udata_clients.os, "waitstatus_to_exitcode", lambda _: 0)
    monkeypatch.setattr(udata_clients.select, "select", lambda readers, *_: ([10], [], []) if readers else ([], [], []))

    assert udata_clients._compose_read("ps") == "ok"


def test_controlled_command_decode_failure_is_redacted(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    reads = iter((b"\xff", b""))

    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "read", lambda *_: next(reads))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: (7, 0))
    monkeypatch.setattr(udata_clients.os, "waitstatus_to_exitcode", lambda _: 0)
    monkeypatch.setattr(udata_clients.select, "select", lambda readers, *_: ([10], [], []) if readers else ([], [], []))

    with pytest.raises(CatalogValidationError) as excinfo:
        udata_clients._compose_read("ps")

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_controlled_command_timeout_terminates_the_child(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    times = iter((0.0, 16.0, 16.0))
    killed: list[int] = []

    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "kill", lambda pid, _: killed.append(pid))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: (7, 0))
    monkeypatch.setattr(udata_clients, "monotonic", lambda: next(times))

    with pytest.raises(CatalogValidationError):
        udata_clients._compose_read("ps")

    assert killed == [7]


def test_controlled_sync_reap_uses_a_separate_bounded_cleanup_window() -> None:
    waits = iter(((0, 0), (7, 0)))
    killed: list[int] = []
    runtime = replace(
        udata_clients._current_controlled_sync_runtime(),
        kill=lambda pid, _: killed.append(pid),
        waitpid=lambda *_: next(waits),
        monotonic=lambda: 0.0,
        select=lambda *_: ([], [], []),
    )

    udata_clients._terminate_controlled_process_sync(7, runtime)

    assert killed == [7]


def test_controlled_async_reap_uses_a_bounded_cleanup_timeout() -> None:
    class PendingProcess:
        returncode: int | None = None

        def __init__(self) -> None:
            self.killed = False

        def kill(self) -> None:
            self.killed = True

        def wait(self) -> object:
            return object()

    process = PendingProcess()
    observed_timeouts: list[float] = []

    async def bounded_wait(_awaitable: object, timeout: float) -> object:
        observed_timeouts.append(timeout)
        raise TimeoutError

    async def run() -> None:
        await udata_clients._terminate_controlled_process(cast(Any, process), wait_for=bounded_wait, clock=lambda: 0.0)

    asyncio.run(run())

    assert process.killed is True
    assert observed_timeouts == [1.0]


def test_controlled_command_output_limit_terminates_the_child(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    killed: list[int] = []

    monkeypatch.setattr(udata_clients, "_CONTROLLED_COMMAND_MAX_OUTPUT_BYTES", 1)
    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "read", lambda *_: b"too much")
    monkeypatch.setattr(udata_clients.os, "kill", lambda pid, _: killed.append(pid))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: (7, 0))
    monkeypatch.setattr(udata_clients.select, "select", lambda readers, *_: ([10], [], []))

    with pytest.raises(CatalogValidationError):
        udata_clients._compose_read("ps")

    assert killed == [7]


def test_controlled_command_nonzero_exit_is_sanitized(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    reads = iter((b"ok", b""))

    monkeypatch.setattr(udata_clients.os, "pipe", lambda: (10, 11))
    monkeypatch.setattr(udata_clients.os, "posix_spawnp", lambda *args, **kwargs: 7)
    monkeypatch.setattr(udata_clients.os, "close", lambda _: None)
    monkeypatch.setattr(udata_clients.os, "read", lambda *_: next(reads))
    monkeypatch.setattr(udata_clients.os, "waitpid", lambda *_: (7, 0))
    monkeypatch.setattr(udata_clients.os, "waitstatus_to_exitcode", lambda _: 1)
    monkeypatch.setattr(udata_clients.select, "select", lambda readers, *_: ([10], [], []) if readers else ([], [], []))

    with pytest.raises(CatalogValidationError) as excinfo:
        udata_clients._compose_read("ps")

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_controlled_command_rejects_docker_environment_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _controlled_compose_files(monkeypatch, tmp_path)
    monkeypatch.setenv("DOCKER_CONTEXT", "remote")

    with pytest.raises(CatalogValidationError, match="environment overrides"):
        udata_clients._compose_read("ps")


def test_bound_controlled_command_spec_ignores_mutable_module_configuration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bound_spec = udata_clients._make_bound_controlled_command_spec()
    original = bound_spec(("ps",))

    monkeypatch.setattr(udata_clients, "_CONTROLLED_COMPOSE_FILE", tmp_path / "compose.yaml")
    monkeypatch.setattr(udata_clients, "_CONTROLLED_ENV_FILE", tmp_path / ".env")
    monkeypatch.setattr(udata_clients, "_CONTROLLED_DOCKER_EXECUTABLES", ())

    assert bound_spec(("ps",)) == original


def test_controlled_peer_decode_failure_is_redacted() -> None:
    response = RuntimeResponse(status_code=200, headers={"Content-Type": "application/json"}, body=b"not-json")

    with pytest.raises(CatalogValidationError) as excinfo:
        udata_clients._controlled_peer_evidence(response)

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_root_document_decode_failure_is_redacted() -> None:
    with pytest.raises(NativeCatalogError) as excinfo:
        wire.parse_document(
            b"\xffpayload",
            endpoint=_SITE_URL,
            expected_media_type="text/csv",
            response_media_type="text/csv",
        )

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_stream_document_decode_failure_is_redacted() -> None:
    response = RuntimeStreamResponse(
        status_code=200,
        headers={"Content-Type": "text/csv"},
        chunks=iter((b"\xff",)),
        close_callback=lambda: None,
    )

    with pytest.raises(NativeCatalogError) as excinfo:
        wire.digest_stream_document(
            response,
            endpoint=_SITE_URL,
            expected_media_type="text/csv",
            max_bytes=8,
        )

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None


def test_async_stream_document_decode_failure_is_redacted() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"\xff"

    response = AsyncRuntimeStreamResponse(
        status_code=200,
        headers={"Content-Type": "text/csv"},
        chunks=chunks(),
        close_callback=lambda: None,
    )

    async def run() -> None:
        with pytest.raises(NativeCatalogError) as excinfo:
            await wire.digest_stream_document_async(
                response,
                endpoint=_SITE_URL,
                expected_media_type="text/csv",
                max_bytes=8,
            )
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__context__ is None

    asyncio.run(run())


def test_stream_document_cleanup_failure_preserves_primary_error() -> None:
    def chunks() -> Generator[bytes, None, None]:
        yield b"valid\n"
        raise ValueError("primary")

    def close() -> None:
        raise RuntimeError("cleanup")

    response = RuntimeStreamResponse(
        status_code=200,
        headers={"Content-Type": "text/csv"},
        chunks=chunks(),
        close_callback=close,
    )

    with pytest.raises(ValueError, match="primary") as excinfo:
        wire.digest_stream_document(
            response,
            endpoint=_SITE_URL,
            expected_media_type="text/csv",
            max_bytes=8,
        )

    assert isinstance(excinfo.value.__cause__, RuntimeError)
    assert str(excinfo.value.__cause__) == "cleanup"


def test_async_stream_document_cleanup_failure_preserves_primary_error() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"valid\n"
        raise ValueError("primary")

    async def close() -> None:
        raise RuntimeError("cleanup")

    response = AsyncRuntimeStreamResponse(
        status_code=200,
        headers={"Content-Type": "text/csv"},
        chunks=chunks(),
        close_callback=close,
    )

    async def run() -> None:
        with pytest.raises(ValueError, match="primary") as excinfo:
            await wire.digest_stream_document_async(
                response,
                endpoint=_SITE_URL,
                expected_media_type="text/csv",
                max_bytes=8,
            )

        assert isinstance(excinfo.value.__cause__, RuntimeError)
        assert str(excinfo.value.__cause__) == "cleanup"

    asyncio.run(run())


def test_sync_root_json_decode_failure_does_not_retain_response_body() -> None:
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "application/json"})))
    )
    with client:
        client.site_version()
        transport.routes[("GET", _SITE_URL)] = RuntimeResponse(200, {"Content-Type": "application/json"}, b"not-json")
        with pytest.raises(NativeCatalogError) as excinfo:
            client.root_profile.get()

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
    assert len(transport.requests) == 2


def test_async_root_json_decode_failure_does_not_retain_response_body() -> None:
    transport, client = _async_client(
        _routes(("GET", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "application/json"})))
    )

    async def run() -> None:
        async with client:
            await client.site_version()
            transport.routes[("GET", _SITE_URL)] = RuntimeResponse(
                200, {"Content-Type": "application/json"}, b"not-json"
            )
            with pytest.raises(NativeCatalogError) as excinfo:
                await client.root_profile.get()
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__context__ is None

    asyncio.run(run())
    assert len(transport.requests) == 2


def test_sync_site_probe_decode_failure_does_not_retain_response_body() -> None:
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, RuntimeResponse(200, {"Content-Type": "application/json"}, b"not-json")))
    )
    with client, pytest.raises(UDataVersionError) as excinfo:
        client.site_version()

    assert excinfo.value.__cause__ is None
    assert excinfo.value.__context__ is None
    assert len(transport.requests) == 1


def test_async_site_probe_decode_failure_does_not_retain_response_body() -> None:
    transport, client = _async_client(
        _routes(("GET", _SITE_URL, RuntimeResponse(200, {"Content-Type": "application/json"}, b"not-json")))
    )

    async def run() -> None:
        async with client:
            with pytest.raises(UDataVersionError) as excinfo:
                await client.site_version()
        assert excinfo.value.__cause__ is None
        assert excinfo.value.__context__ is None

    asyncio.run(run())
    assert len(transport.requests) == 1


def _site_body(*, title: str = "uData") -> dict[str, object]:
    return {
        "id": "site",
        "title": title,
        "keywords": ["open", "data"],
        "feed_size": 20,
        "configs": {"default_language": "en"},
        "themes": {"name": "default"},
        "settings": {"home_datasets": [], "home_reuses": []},
        "datasets_blocs": [],
        "reuses_blocs": [],
        "dataservices_blocs": [],
        "metrics": {"datasets": 1},
        "version": "17.6.0",
        "portal_extension": {"enabled": True},
    }


def _json_response(status: int, payload: object, headers: Mapping[str, str] | None = None) -> RuntimeResponse:
    body = b"" if payload is None else payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    return RuntimeResponse(status_code=status, headers=dict(headers or {}), body=body)


class RouterTransport:
    """A deterministic transport that records every wire request."""

    def __init__(self, routes: Mapping[tuple[str, str], RuntimeResponse]) -> None:
        self.routes = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self._suppress_next = False

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        suppress = self._suppress_next
        self._suppress_next = False
        if not suppress:
            self.requests.append(request)
        try:
            return self.routes[(request.method, request.url)]
        except KeyError as error:
            raise AssertionError(f"unexpected request {(request.method, request.url)}") from error

    def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
        response = self.send(request)
        return RuntimeStreamResponse(
            response.status_code,
            response.headers,
            iter((response.body,)),
            lambda: None,
            response.retry_after,
        )

    def close(self) -> None:
        self.close_count += 1


class AsyncRouterTransport:
    """An asynchronous deterministic transport with the same request map."""

    def __init__(self, routes: Mapping[tuple[str, str], RuntimeResponse]) -> None:
        self.routes = dict(routes)
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0
        self._suppress_next = False

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        suppress = self._suppress_next
        self._suppress_next = False
        if not suppress:
            self.requests.append(request)
        try:
            return self.routes[(request.method, request.url)]
        except KeyError as error:
            raise AssertionError(f"unexpected request {(request.method, request.url)}") from error

    async def send_stream(self, request: RuntimeRequest) -> AsyncRuntimeStreamResponse:
        response = await self.send(request)

        async def chunks() -> AsyncIterator[bytes]:
            yield response.body

        return AsyncRuntimeStreamResponse(
            response.status_code,
            response.headers,
            chunks(),
            lambda: None,
            response.retry_after,
        )

    async def aclose(self) -> None:
        self.close_count += 1


def _controlled_process_setup(stack: ExitStack, transport: RouterTransport | AsyncRouterTransport) -> None:
    def sync_command(args: tuple[str, ...], *, input_data: bytes | None = None, **_: object) -> str:
        if args == ("context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"):
            return '"unix:///Users/nitish/.docker/run/docker.sock"'
        if args[-4:] == ("ps", "--status", "running", "--services"):
            return "udata\nmongo\nredis\nsearch\nstorage\nmailpit"
        if args[-3:] == ("port", "udata", "7000"):
            return "127.0.0.1:5640"
        if args[-8:] == ("exec", "-T", "udata", "git", "-C", "/opt/udata", "rev-parse", "HEAD"):
            return "0546582058d84706812a1c37387576efc4e5ad1f"
        if args[-5:] == ("exec", "-T", "udata", "printenv", "UDATA_EVIDENCE_STACK_NONCE"):
            return "unit-test-stack"
        if args[-3] == "python":
            response = transport.routes.get(
                ("PATCH", _SITE_URL),
                _json_response(200, _site_body(), {"Content-Type": "application/json"}),
            )
            if 'method="PATCH"' not in args[-1]:
                response = _json_response(200, _site_body(), {"Content-Type": "application/json"})
            return json.dumps(
                {
                    "status": response.status_code,
                    "content_type": response.headers.get("Content-Type", ""),
                    "location": response.headers.get("Location", ""),
                    "body": response.body.decode("utf-8", "replace"),
                },
                separators=(",", ":"),
            )
        raise AssertionError(f"unexpected controlled command {args}")

    async def async_command(args: tuple[str, ...], *, input_data: bytes | None = None, **_: object) -> str:
        return sync_command(args, input_data=input_data)

    sync_type = udata_clients._ControlledSyncTransport
    async_type = udata_clients._ControlledAsyncTransport
    sync_bindings = sync_type._factory_bindings
    async_bindings = async_type._factory_bindings
    type.__setattr__(
        sync_type,
        "_factory_bindings",
        (*sync_bindings[:5], udata_clients._make_controlled_sync_operations(sync_command)),
    )
    type.__setattr__(
        async_type,
        "_factory_bindings",
        (*async_bindings[:5], udata_clients._make_controlled_async_operations(async_command)),
    )

    def restore_bindings() -> None:
        type.__setattr__(sync_type, "_factory_bindings", sync_bindings)
        type.__setattr__(async_type, "_factory_bindings", async_bindings)

    stack.callback(restore_bindings)


def _sync_client(
    routes: Mapping[tuple[str, str], RuntimeResponse],
    *,
    origin: str = _ORIGIN,
    credential: UDataCredential | None = None,
    revalidate: Callable[..., bool] | None = None,
    emitter: EventEmitter | None = None,
    root_export_max_bytes: int = 8 * 1024 * 1024,
    test_transport: RouterTransport | None = None,
) -> tuple[RouterTransport, SyncUDataClient]:
    transport = test_transport or RouterTransport(routes)
    stack = ExitStack()

    original_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {
            {
                "accept": "Accept",
                "content-type": "Content-Type",
                "x-api-key": "X-API-KEY",
            }.get(key.lower(), key): value
            for key, value in request.headers.items()
        }
        response = transport.send(
            RuntimeRequest(
                method=request.method,
                url=str(request.url),
                headers=headers,
                body=request.content or None,
            )
        )
        return httpx.Response(
            response.status_code,
            headers=dict(response.headers),
            content=response.body,
            request=request,
        )

    def client_factory(**_: object) -> httpx.Client:
        return original_client(transport=httpx.MockTransport(handler), follow_redirects=False)

    stack.enter_context(patch.object(httpx, "Client", client_factory))

    stack.enter_context(patch.dict(os.environ, {"UDATA_EVIDENCE_STACK_NONCE": "unit-test-stack"}))
    _controlled_process_setup(stack, transport)
    try:
        controlled_transport = udata_clients._ControlledSyncTransport()
    except BaseException:
        stack.close()
        raise
    finally:
        transport.requests.clear()
    if revalidate is not None and not revalidate(site_id="site"):
        changed_site = _site_body()
        changed_site["id"] = "changed"
        transport.routes[("GET", _SITE_URL)] = _json_response(200, changed_site, {"Content-Type": "application/json"})
    settings = UDataClientSettings(
        base_url=origin,
        credential=credential,
        sync_transport=lambda: controlled_transport,
        root_export_max_bytes=root_export_max_bytes,
    )
    client = create_sync_client(settings)
    client._mutation_dispatch_gate.bind_sync_client(client, controlled_transport)
    original_close = client.close
    closed = False

    def close() -> None:
        nonlocal closed
        try:
            original_close()
        finally:
            if not closed:
                stack.close()
                closed = True

    cast(Any, client).close = close
    if emitter is not None:
        client._emitter = emitter
    return transport, client


def _async_client(
    routes: Mapping[tuple[str, str], RuntimeResponse],
    *,
    origin: str = _ORIGIN,
    credential: UDataCredential | None = None,
    emitter: EventEmitter | None = None,
    root_export_max_bytes: int = 8 * 1024 * 1024,
) -> tuple[AsyncRouterTransport, AsyncUDataClient]:
    transport = AsyncRouterTransport(routes)
    stack = ExitStack()

    original_client = httpx.AsyncClient

    async def handler(request: httpx.Request) -> httpx.Response:
        response = await transport.send(
            RuntimeRequest(
                method=request.method,
                url=str(request.url),
                headers=dict(request.headers),
                body=request.content or None,
            )
        )
        return httpx.Response(
            response.status_code,
            headers=dict(response.headers),
            content=response.body,
            request=request,
        )

    def client_factory(**_: object) -> httpx.AsyncClient:
        return original_client(transport=httpx.MockTransport(handler), follow_redirects=False)

    stack.enter_context(patch.object(httpx, "AsyncClient", client_factory))

    stack.enter_context(patch.dict(os.environ, {"UDATA_EVIDENCE_STACK_NONCE": "unit-test-stack"}))
    _controlled_process_setup(stack, transport)
    try:
        controlled_transport = udata_clients._ControlledAsyncTransport()
        asyncio.run(controlled_transport.verify())
    except BaseException:
        stack.close()
        raise
    finally:
        transport.requests.clear()
    settings = UDataClientSettings(
        base_url=origin,
        credential=credential,
        async_transport=lambda: controlled_transport,
        root_export_max_bytes=root_export_max_bytes,
    )
    client = create_async_client(settings)
    client._mutation_dispatch_gate.bind_async_client(client, controlled_transport)
    original_aclose = client.aclose
    closed = False

    async def aclose() -> None:
        nonlocal closed
        try:
            await original_aclose()
        finally:
            if not closed:
                stack.close()
                closed = True

    cast(Any, client).aclose = aclose
    if emitter is not None:
        client._emitter = emitter
    return transport, client


def _routes(*responses: tuple[str, str, RuntimeResponse]) -> dict[tuple[str, str], RuntimeResponse]:
    result = {(method, url): response for method, url, response in responses}
    result.setdefault(("GET", _SITE_URL), _json_response(200, _site_body(), {"Content-Type": "application/json"}))
    return result


def _site_policy(*, target: str = "site") -> MutationPolicy:
    return MutationPolicy(
        confirmation=ConfirmationPolicy(
            confirmed=True,
            operation=wire.ROOT_OPERATIONS["set_site"],
            target=target,
        ),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = vars(error).get("mutation_receipt")
    assert isinstance(receipt, MutationReceipt)
    return receipt


def test_row183_get_site_decodes_a_lossless_typed_profile() -> None:
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "application/json"})))
    )
    with client:
        profile = client.root_profile.get()

    assert isinstance(profile, SiteProfile)
    assert profile.id == "site"
    assert profile.title == "uData"
    assert profile.version == "17.6.0"
    assert profile.payload["portal_extension"] == {"enabled": True}
    profile_payload = cast(dict[str, object], profile.to_dict()["payload"])
    assert profile_payload["metrics"] == {"datasets": 1}
    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


def test_row184_set_site_uses_patch_presence_and_exact_confirmation() -> None:
    patch_url = _SITE_URL
    transport, client = _sync_client(
        _routes(
            (
                "PATCH",
                patch_url,
                _json_response(200, _site_body(title="Changed"), {"Content-Type": "application/json"}),
            ),
        ),
        credential=_CREDENTIAL,
    )
    with client:
        result = client.root_profile.set_site(
            SitePatchInput(title="Changed", configs=None),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert isinstance(result, SiteMutationResult)
    assert result.profile is not None and result.profile.title == "Changed"
    assert result.receipt.outcome == "succeeded"
    assert result.receipt.target.value == "site"
    assert result.receipt.audit_metadata["controlled_evidence_digest"] == _controlled_evidence().digest
    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_controlled_sync_dispatch_keeps_factory_bound_operations_after_helper_override() -> None:
    transport, client = _sync_client(
        _routes(
            (
                "PATCH",
                _SITE_URL,
                _json_response(200, _site_body(title="Changed"), {"Content-Type": "application/json"}),
            )
        ),
        credential=_CREDENTIAL,
    )
    called = False

    def replacement(*_: object, **__: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("mutable controlled helper was invoked")

    with client:
        with (
            patch.object(udata_clients, "_controlled_command", replacement),
            patch.object(udata_clients, "_controlled_patch_response", replacement),
        ):
            result = client.root_profile.set_site(
                SitePatchInput(title="Changed"),
                permissions=_PERMISSIONS,
                mutation_policy=_site_policy(),
            )

    assert result.receipt.outcome == "succeeded"
    assert called is False


def test_controlled_async_dispatch_keeps_factory_bound_operations_after_helper_override() -> None:
    transport, client = _async_client(
        _routes(
            (
                "PATCH",
                _SITE_URL,
                _json_response(200, _site_body(title="Changed"), {"Content-Type": "application/json"}),
            )
        ),
        credential=_CREDENTIAL,
    )
    called = False

    async def replacement(*_: object, **__: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("mutable controlled helper was invoked")

    async def run() -> SiteMutationResult:
        async with client:
            with (
                patch.object(udata_clients, "_controlled_command_async", replacement),
                patch.object(udata_clients, "_controlled_patch_response_async", replacement),
            ):
                return await client.root_profile.set_site(
                    SitePatchInput(title="Changed"),
                    permissions=_PERMISSIONS,
                    mutation_policy=_site_policy(),
                )

    result = asyncio.run(run())

    assert result.receipt.outcome == "succeeded"
    assert called is False


def test_controlled_sync_construction_ignores_preconstruction_helper_override() -> None:
    called = False

    def replacement(*_: object, **__: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("mutable controlled helper was invoked")

    with patch.object(udata_clients, "_controlled_command", replacement):
        _, client = _sync_client(
            _routes(("PATCH", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "application/json"}))),
            credential=_CREDENTIAL,
        )

    with client:
        result = client.root_profile.set_site(
            SitePatchInput(title="Changed"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert result.receipt.outcome == "succeeded"
    assert called is False


def test_controlled_async_construction_ignores_preconstruction_helper_override() -> None:
    called = False

    async def replacement(*_: object, **__: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("mutable controlled helper was invoked")

    with patch.object(udata_clients, "_controlled_command_async", replacement):
        _, client = _async_client(
            _routes(("PATCH", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "application/json"}))),
            credential=_CREDENTIAL,
        )

    async def run() -> SiteMutationResult:
        async with client:
            return await client.root_profile.set_site(
                SitePatchInput(title="Changed"),
                permissions=_PERMISSIONS,
                mutation_policy=_site_policy(),
            )

    result = asyncio.run(run())

    assert result.receipt.outcome == "succeeded"
    assert called is False


def test_row184_set_site_rejects_redirect_without_following() -> None:
    location = "https://other.example/api/1/site/"
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(307, None, {"Location": location}))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(NativeCatalogError, match="redirect"):
        client.root_profile.set_site(
            SitePatchInput(title="unchanged"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_site_patch_omits_unset_fields_but_retains_explicit_null() -> None:
    body = SitePatchInput(configs=None, keywords=("one", "two")).payload()

    assert body == {"keywords": ["one", "two"], "configs": None}
    assert "title" not in body
    assert SitePatchInput().payload() == {}


def test_site_data_portal_redirect_is_typed_and_same_origin() -> None:
    location = f"{_ORIGIN}/api/1/site/catalog.json"
    transport, client = _sync_client(
        _routes(
            (
                "GET",
                f"{_ORIGIN}/api/1/site/data.json",
                _json_response(302, None, {"Location": location}),
            )
        )
    )
    with client:
        redirect = client.root_profile.data_portal("json")

    assert isinstance(redirect, SiteDocument)
    assert redirect.location == "/api/1/site/catalog.json"
    assert redirect.status_code == 302
    assert redirect.payload["location"] == "/api/1/site/catalog.json"
    assert [request.url for request in transport.requests] == [_SITE_URL, f"{_ORIGIN}/api/1/site/data.json"]


def test_site_rdf_catalog_preserves_accept_and_catalog_pagination_query() -> None:
    catalog_url = f"{_ORIGIN}/api/1/site/catalog?page=2&page_size=5&q=air+quality"
    location = f"{_ORIGIN}/api/1/site/catalog.json?page=2&page_size=5&q=air+quality"
    transport, client = _sync_client(_routes(("GET", catalog_url, _json_response(302, None, {"Location": location}))))
    with client:
        redirect = client.root_profile.rdf_catalog(
            SiteCatalogQuery(page=2, page_size=5, q="air quality"), accept="application/ld+json"
        )

    request = transport.requests[-1]
    assert request.headers["Accept"] == "application/ld+json"
    assert request.url == catalog_url
    assert redirect.location == "/api/1/site/catalog.json?page=2&page_size=5&q=air+quality"


def test_site_rdf_catalog_accepts_upstream_default_query_order() -> None:
    catalog_url = f"{_ORIGIN}/api/1/site/catalog"
    location = f"{_ORIGIN}/api/1/site/catalog.json?page_size=100&page=1"
    transport, client = _sync_client(_routes(("GET", catalog_url, _json_response(302, None, {"Location": location}))))
    with client:
        redirect = client.root_profile.rdf_catalog()

    assert redirect.location == "/api/1/site/catalog.json?page_size=100&page=1"
    assert transport.requests[-1].url == catalog_url


def test_site_rdf_catalog_format_returns_bounded_document_metadata() -> None:
    url = f"{_ORIGIN}/api/1/site/catalog.json?page=1&page_size=100"
    body = b'{"@context": {}}'
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "application/ld+json"})))
    )
    with client:
        document = client.root_profile.rdf_catalog_format("json", SiteCatalogQuery())

    assert document.media_type == "application/ld+json"
    assert document.size_bytes == len(body)
    assert document.sha256
    assert "body" not in document.to_dict()


def test_root_export_forwards_chunks_to_the_caller_sink_and_enforces_the_limit() -> None:
    url = f"{_ORIGIN}/api/1/site/datasets.csv"
    body = b"id\n123\n"
    received: list[bytes] = []
    _, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))),
        root_export_max_bytes=len(body),
    )
    with client:
        document = client.root_profile.datasets_csv(sink=received.append)

    assert b"".join(received) == body
    assert document.size_bytes == len(body)

    _, limited_client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))),
        root_export_max_bytes=len(body) - 1,
    )
    with limited_client, pytest.raises(NativeCatalogError, match="byte limit"):
        limited_client.root_profile.datasets_csv()


def _assert_csv_export(method_name: str, path: str) -> None:
    url = f"{_ORIGIN}{path}"
    body = b'"id";"title"\n"one";"A"\n'
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv; charset=utf-8"})))
    )
    with client:
        document = getattr(client.root_profile, method_name)()

    assert isinstance(document, SiteDocument)
    assert document.media_type == "text/csv"
    assert document.payload["size_bytes"] == len(body)
    assert transport.requests[-1].url == url


def test_root_export_emits_failure_only_after_stream_consumption_fails() -> None:
    class FailingStreamTransport(RouterTransport):
        def send_stream(self, request: RuntimeRequest) -> RuntimeStreamResponse:
            self.requests.append(request)

            def chunks() -> Generator[bytes, None, None]:
                yield b"id\n"
                raise TransportFailure("stream interrupted")

            return RuntimeStreamResponse(
                200,
                {"Content-Type": "text/csv"},
                chunks(),
                lambda: None,
            )

    events = []
    transport = FailingStreamTransport(_routes())
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        emitter=EventEmitter(sinks=(events.append,)),
        owns_transport=False,
    )
    with client, pytest.raises(TransportFailure):
        client.root_profile.datasets_csv()

    assert events[-1].outcome == "failed"
    assert not any(event.outcome == "succeeded" for event in events if event.operation_id == wire.ROOT_OPERATION)


def test_async_site_patch_matches_sync_target_and_receipt_contract() -> None:
    transport, client = _async_client(
        _routes(
            (
                "PATCH",
                _SITE_URL,
                _json_response(200, _site_body(title="Changed"), {"Content-Type": "application/json"}),
            )
        ),
        credential=_CREDENTIAL,
    )

    async def run() -> SiteMutationResult:
        async with client:
            return await client.root_profile.set_site(
                SitePatchInput(title="Changed"),
                permissions=_PERMISSIONS,
                mutation_policy=_site_policy(),
            )

    result = asyncio.run(run())
    assert result.profile is not None and result.profile.site_id == "site"
    assert result.receipt.target.value == "site"
    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_root_profile_rejects_oversized_buffered_json() -> None:
    payload = _site_body()
    transport = RouterTransport(
        _routes(("GET", _SITE_URL, _json_response(200, payload, {"Content-Type": "application/json"})))
    )
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        root_export_max_bytes=4,
        owns_transport=False,
    )
    with client, pytest.raises(NativeCatalogError, match="byte limit"):
        client.root_profile.get()

    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


@pytest.mark.parametrize(
    "headers",
    ({}, {"Content-Type": ""}, {"Content-Type": "application/json,text/plain"}, {"Content-Type": "text/plain"}),
)
def test_site_profile_requires_one_exact_response_media_type(headers: Mapping[str, str]) -> None:
    transport, client = _sync_client(_routes(("GET", _SITE_URL, _json_response(200, _site_body(), headers))))
    with client, pytest.raises(NativeCatalogError):
        client.root_profile.get()

    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


def test_row188_site_datasets_csv() -> None:
    _assert_csv_export("datasets_csv", "/api/1/site/datasets.csv")


def test_row189_site_resources_csv() -> None:
    _assert_csv_export("resources_csv", "/api/1/site/resources.csv")


def test_row190_site_organizations_csv() -> None:
    _assert_csv_export("organizations_csv", "/api/1/site/organizations.csv")


def test_row191_site_reuses_csv() -> None:
    _assert_csv_export("reuses_csv", "/api/1/site/reuses.csv")


def test_row192_site_dataservices_csv() -> None:
    _assert_csv_export("dataservices_csv", "/api/1/site/dataservices.csv")


def test_row193_site_harvests_csv() -> None:
    _assert_csv_export("harvests_csv", "/api/1/site/harvests.csv")


def test_row194_site_tags_csv() -> None:
    _assert_csv_export("tags_csv", "/api/1/site/tags.csv")


def test_row195_jsonld_context_decodes_json_without_retaining_raw_bytes() -> None:
    url = f"{_ORIGIN}/api/1/site/context.jsonld"
    payload = {"@vocab": "http://www.w3.org/ns/dcat#", "title": "dct:title"}
    body = json.dumps(payload).encode()
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(200, body, {"Content-Type": "application/ld+json"})))
    )
    with client:
        document = client.root_profile.jsonld_context()

    assert document.payload["@vocab"] == payload["@vocab"]
    assert document.media_type == "application/ld+json"
    assert "body" not in document.to_dict()
    assert transport.requests[-1].url == url


@pytest.mark.parametrize("fmt", ("rdf", "owl"))
def test_rdf_xml_aliases_are_supported(fmt: str) -> None:
    assert wire.media_type_for_format(fmt) == "application/rdf+xml"


def test_route_specific_csv_query_models_do_not_share_dataset_filters() -> None:
    with pytest.raises(TypeError):
        cast(Any, SiteOrganizationCsvQuery)(name="org", page_size=2)
    with pytest.raises(ValueError):
        SiteDatasetCsvQuery(filters={"last_update_range": "not-a-range"})
    assert wire.reuses_csv_request(SiteReuseCsvQuery(filters={"tag": ("one", "two")}))[1].endswith("tag=one&tag=two")
    assert wire.dataservices_csv_request(SiteDataserviceCsvQuery(filters={"tag": ("one", "two")}))[1].endswith(
        "tag=one&tag=two"
    )


def test_root_invalid_format_is_rejected_before_site_probe() -> None:
    transport, client = _sync_client(_routes())
    with client, pytest.raises(CatalogValidationError):
        client.root_profile.data_portal("yaml")

    assert transport.requests == []


def test_root_malformed_profile_maps_to_typed_error() -> None:
    payload = _site_body()
    payload["title"] = 42
    transport, client = _sync_client(
        _routes(("GET", _SITE_URL, _json_response(200, payload, {"Content-Type": "application/json"})))
    )
    with client, pytest.raises(CatalogValidationError) as excinfo:
        client.root_profile.get()

    assert excinfo.value.operation == wire.ROOT_OPERATION
    assert [request.url for request in transport.requests] == [_SITE_URL, _SITE_URL]


def test_root_external_redirect_is_rejected_without_following() -> None:
    url = f"{_ORIGIN}/api/1/site/data.json"
    transport, client = _sync_client(
        _routes(("GET", url, _json_response(302, None, {"Location": "https://other.example/site/catalog.json"})))
    )
    with client, pytest.raises(NativeCatalogError):
        client.root_profile.data_portal("json")

    assert [request.url for request in transport.requests] == [_SITE_URL, url]


def test_set_site_rejects_public_origins_before_any_dispatch_and_keeps_receipt() -> None:
    transport = RouterTransport({("PATCH", "https://public.example/api/1/site/"): _json_response(200, _site_body())})
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin="https://public.example",
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client, pytest.raises(CatalogValidationError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="unsafe"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    receipt = _receipt_from(excinfo.value)
    assert receipt.outcome == "rejected"
    assert transport.requests == []


def test_set_site_rejects_wrong_confirmation_without_dispatch() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(200, _site_body()))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(ForbiddenError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="unsafe"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(target="other-site"),
        )

    assert _receipt_from(excinfo.value).outcome == "rejected"
    assert transport.requests == []


def test_set_site_maps_423_to_non_retryable_deployment_disabled_with_receipt() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(423, None))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(CatalogUnavailableError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="read-only"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    error = excinfo.value
    assert error.capability_state == "deployment-disabled"
    assert error.metadata["status_code"] == 423
    assert _receipt_from(error).outcome == "failed"
    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_site_patch_post_dispatch_media_failure_is_ambiguous() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(200, _site_body(), {"Content-Type": "text/plain"}))),
        credential=_CREDENTIAL,
    )
    with client, pytest.raises(NativeCatalogError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="possibly-written"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    receipt = _receipt_from(excinfo.value)
    assert receipt.outcome == "ambiguous"
    assert receipt.audit_metadata["status_code"] == 200


def test_set_site_denial_does_not_poison_the_root_read_capability() -> None:
    transport, client = _sync_client(
        _routes(("PATCH", _SITE_URL, _json_response(423, None))),
        credential=_CREDENTIAL,
    )
    with client:
        with pytest.raises(CatalogUnavailableError):
            client.root_profile.set_site(
                SitePatchInput(title="read-only"),
                permissions=_PERMISSIONS,
                mutation_policy=_site_policy(),
            )
        assert client.root_profile.get().id == "site"

    assert [request.method for request in transport.requests] == ["GET", "GET", "GET"]


def test_set_site_requires_controlled_factory_before_any_dispatch() -> None:
    transport = RouterTransport(_routes())
    client = SyncUDataClient(
        transport,
        declared_udata_profile(),
        origin=_ORIGIN,
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client, pytest.raises(CatalogValidationError) as excinfo:
        client.root_profile.set_site(
            SitePatchInput(title="unattested"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert _receipt_from(excinfo.value).outcome == "rejected"
    assert transport.requests == []


def test_fabricated_controlled_evidence_cannot_authorize_an_injected_transport() -> None:
    transport = RouterTransport(_routes())
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, udata_clients._ControlledSyncTransport)(transport=transport)
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        cast(Any, UDataClientSettings)(
            base_url=_ORIGIN,
            credential=_CREDENTIAL,
            sync_transport=transport,
            controlled_stack_attestation=object(),
        )

    fabricated = object.__new__(udata_clients._ControlledSyncTransport)
    with pytest.raises(AttributeError):
        object.__setattr__(fabricated, "_transport", transport)
    client = SyncUDataClient(
        fabricated,
        declared_udata_profile(),
        origin=_ORIGIN,
        credentials=_CREDENTIAL,
        owns_transport=False,
    )
    with client:
        with pytest.raises((CatalogValidationError, ForbiddenError)):
            client.root_profile.set_site(
                SitePatchInput(title="unattested"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
            )
    assert transport.requests == []


def test_service_helper_override_cannot_bypass_transport_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    from datasluice.connectors.catalog.udata.services import root_profile as root_service

    transport = RouterTransport(_routes())
    client = create_sync_client(UDataClientSettings(base_url=_ORIGIN, credential=_CREDENTIAL, sync_transport=transport))
    monkeypatch.setattr(root_service, "_controlled_sync_site_id", lambda _: "site")
    monkeypatch.setattr(root_service, "_controlled_sync_revalidate", lambda *args, **kwargs: True)
    monkeypatch.setattr(root_service, "_controlled_sync_evidence_digest", lambda _: _controlled_evidence().digest)

    client_type = cast(Any, type(client))
    with pytest.raises(AttributeError, match="factory-owned"):
        client_type._mutation_dispatch_gate = object()

    with client, pytest.raises(CatalogValidationError):
        client.root_profile.set_site(
            SitePatchInput(title="unattested"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_registered_transport_cannot_be_rebound_to_another_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    from datasluice.connectors.catalog.udata.services import root_profile as root_service

    origin = "https://other.example"
    site_url = f"{origin}/api/1/site/"
    transport, client = _sync_client(
        _routes(
            ("GET", site_url, _json_response(200, _site_body(), {"Content-Type": "application/json"})),
            ("PATCH", site_url, _json_response(200, _site_body(), {"Content-Type": "application/json"})),
        ),
        origin=origin,
        credential=_CREDENTIAL,
    )
    monkeypatch.setattr(root_service, "_controlled_sync_site_id", lambda _: "site")
    monkeypatch.setattr(root_service, "_controlled_sync_revalidate", lambda *args, **kwargs: True)
    monkeypatch.setattr(root_service, "_controlled_sync_evidence_digest", lambda _: _controlled_evidence().digest)

    with client, pytest.raises(CatalogValidationError):
        client.root_profile.set_site(
            SitePatchInput(title="unattested"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_injected_transport_remains_available_for_read_only_behavior() -> None:
    transport = RouterTransport(_routes())
    client = create_sync_client(UDataClientSettings(base_url=_ORIGIN, sync_transport=transport))

    with client:
        assert client.root_profile.get().id == "site"

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_controlled_factory_rejects_injected_transport_before_any_dispatch() -> None:
    transport = RouterTransport(_routes())
    with pytest.raises(CatalogValidationError):
        _create_controlled_sync_client(
            UDataClientSettings(
                base_url=_ORIGIN,
                credential=_CREDENTIAL,
                sync_transport=transport,
            )
        )

    assert transport.requests == []


def test_root_mutation_does_not_dispatch_through_injected_transport() -> None:
    class FailingTransport(RouterTransport):
        def send(self, request: RuntimeRequest) -> RuntimeResponse:
            suppress = self._suppress_next
            self._suppress_next = False
            if not suppress:
                self.requests.append(request)
            return _json_response(200, _site_body(), {"Content-Type": "application/json"})

    transport, client = _sync_client(
        {},
        credential=_CREDENTIAL,
        test_transport=FailingTransport({}),
    )
    with client:
        result = client.root_profile.set_site(
            SitePatchInput(title="once"),
            permissions=_PERMISSIONS,
            mutation_policy=_site_policy(),
        )

    assert result.receipt.outcome == "succeeded"
    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_unrelated_local_listener_loses_authority_before_patch_dispatch() -> None:
    transport, client = _sync_client(
        _routes(),
        credential=_CREDENTIAL,
        revalidate=lambda *, site_id: False,
    )

    with client, pytest.raises(CatalogValidationError):
        client.root_profile.set_site(
            SitePatchInput(title="unchanged"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_forwarding_listener_loses_authority_before_patch_dispatch() -> None:
    transport, client = _sync_client(
        _routes(),
        credential=_CREDENTIAL,
        revalidate=lambda *, site_id: False,
    )

    with client, pytest.raises(CatalogValidationError):
        client.root_profile.set_site(
            SitePatchInput(title="unchanged"), permissions=_PERMISSIONS, mutation_policy=_site_policy()
        )

    assert [request.method for request in transport.requests] == ["GET", "GET"]


def test_async_root_service_matches_sync_wire_and_result_shapes() -> None:
    url = f"{_ORIGIN}/api/1/site/datasets.csv"
    body = b'"id";"title"\n'
    routes = _routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"})))
    sync_transport, sync_client = _sync_client(routes)
    with sync_client:
        sync_document = sync_client.root_profile.datasets_csv()

    transport, client = _async_client(_routes(("GET", url, _json_response(200, body, {"Content-Type": "text/csv"}))))

    async def run() -> SiteDocument:
        async with client:
            return await client.root_profile.datasets_csv()

    document = asyncio.run(run())
    assert isinstance(document, SiteDocument)
    assert document.sha256
    assert [request.url for request in transport.requests] == [_SITE_URL, url]
    assert document == sync_document
    assert [request.url for request in sync_transport.requests] == [_SITE_URL, url]


def test_root_profile_wire_operations_use_the_existing_broad_capability_identity() -> None:
    assert wire.ROOT_OPERATIONS["set_site"] == "udata/api-v1.set_site"
    assert all(
        operation == wire.ROOT_OPERATION for name, operation in wire.ROOT_OPERATIONS.items() if name != "set_site"
    )
    assert next(
        operation
        for operation in declared_udata_profile().operations
        if operation.method == "root-and-effective-profile-probe"
    )


def test_root_profile_models_are_typed_and_immutable() -> None:
    profile = SiteProfile.from_payload(_site_body())
    with pytest.raises(TypeError):
        dict.__setitem__(cast(dict[str, object], profile.payload), "title", "changed")

    assert isinstance(profile.catalog_id.value, str)
    assert isinstance(profile.to_dict(), dict)
    assert isinstance(SitePatchInput(title="x"), SitePatchInput)
    assert NativeRecord is not SiteProfile
