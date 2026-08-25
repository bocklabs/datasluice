"""Asynchronous catalog runtime client tests."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping

import pytest

from datasluice.contracts.catalog.protocols import AsyncCatalogClient as AsyncCatalogClientProtocol
from datasluice.contracts.catalog.protocols import CatalogOperationGuard
from datasluice.domain.catalog.auth import (
    CatalogCredential,
    CredentialResolutionPolicy,
    CredentialResolver,
    CredentialSource,
    EffectivePermissions,
    SocrataCredential,
    UDataCredential,
)
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.profiles import (
    CredentialClassification,
    ProbeEvidence,
    ProbeResponseClass,
    RoleClassification,
)
from datasluice.errors.catalog import CatalogNotFoundError, ForbiddenError, UnsupportedCapabilityError
from datasluice.runtime.clients import AsyncCatalogClient
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse
from tests.unit.runtime._fixtures import _envelope, _guard, _profile, _request


class _AsyncTransport:
    def __init__(self) -> None:
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(200, {}, _envelope())

    async def aclose(self) -> None:
        self.close_count += 1


class _AsyncProbeRunner:
    def __init__(self, response_class: ProbeResponseClass) -> None:
        self.response_class = response_class
        self.calls = 0

    async def probe(self, operation_id: OperationId) -> ProbeEvidence:
        self.calls += 1
        return ProbeEvidence(
            operation_id=operation_id,
            deployment_url="https://catalog.example.test/api",
            credential_classification=CredentialClassification.ANONYMOUS,
            role_classification=RoleClassification.ANONYMOUS,
            observed_response_class=self.response_class,
        )


class _FailingResolver(CredentialResolver):
    async def resolve_async(self) -> object:
        raise AssertionError("resolver methods must not be invoked for explicit credentials")

    def resolve(
        self,
        discovered: Mapping[CredentialSource, CatalogCredential],
        *,
        policy: CredentialResolutionPolicy | None = None,
    ) -> CatalogCredential | None:
        del discovered, policy
        raise AssertionError("resolver methods must not be invoked for explicit credentials")


def test_async_client_dispatches_dataset_get_and_closes_once() -> None:
    async def exercise() -> _AsyncTransport:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile())

        assert isinstance(client, AsyncCatalogClientProtocol)
        assert (await client.datasets.get(_request(), _guard())).items[0].name == "Fixture dataset"
        await client.aclose()
        await client.aclose()
        return transport

    transport = asyncio.run(exercise())
    assert transport.close_count == 1


def test_async_client_dispatches_after_a_successful_probe() -> None:
    async def exercise() -> tuple[int, int]:
        transport = _AsyncTransport()
        runner = _AsyncProbeRunner(ProbeResponseClass.SUCCESS)
        client = AsyncCatalogClient(transport, _profile(), probe_runner=runner)

        assert (await client.datasets.get(_request(), _guard())).items[0].name == "Fixture dataset"
        await client.aclose()
        return runner.calls, len(transport.requests)

    assert asyncio.run(exercise()) == (1, 1)


def test_async_client_matches_sync_guard_and_error_semantics() -> None:
    async def exercise() -> None:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile())
        operation = OperationId("reference", "resources", "get")

        with pytest.raises(UnsupportedCapabilityError) as unsupported:
            await client.datasets.get(_request(operation), _guard(operation))
        assert unsupported.value.safe_action
        assert transport.requests == []

        class _NotFoundTransport(_AsyncTransport):
            async def send(self, request: RuntimeRequest) -> RuntimeResponse:
                return RuntimeResponse(404, {}, b"")

        not_found = AsyncCatalogClient(_NotFoundTransport(), _profile())
        try:
            with pytest.raises(CatalogNotFoundError):
                await not_found.datasets.get(_request(), _guard())
        finally:
            await not_found.aclose()

    asyncio.run(exercise())


@pytest.mark.parametrize("service_method", ["get", "list"])
def test_async_client_rejects_denied_caller_guard_before_probe_or_transport(service_method: str) -> None:
    async def exercise() -> None:
        operation = _request().operation_id
        permissions = EffectivePermissions(
            platform=CatalogPlatform("reference"),
            scopes=frozenset(),
            authenticated=True,
            operation_scopes={str(operation): frozenset({"datasets:read"})},
        )
        guard = CatalogOperationGuard(operation_id=operation, permissions=permissions)
        transport = _AsyncTransport()
        runner = _AsyncProbeRunner(ProbeResponseClass.SUCCESS)
        client = AsyncCatalogClient(transport, _profile(), probe_runner=runner)

        with pytest.raises(ForbiddenError):
            await getattr(client.datasets, service_method)(_request(), guard)

        assert runner.calls == 0
        assert transport.requests == []

    asyncio.run(exercise())


def test_async_client_rejects_guard_for_different_operation_before_probe_or_transport() -> None:
    async def exercise() -> None:
        operation = _request().operation_id
        guard = CatalogOperationGuard(operation_id=OperationId("reference", "datasets", "list"))
        transport = _AsyncTransport()
        runner = _AsyncProbeRunner(ProbeResponseClass.SUCCESS)
        client = AsyncCatalogClient(transport, _profile(), probe_runner=runner)

        with pytest.raises(ValueError, match="does not match request"):
            await client.datasets.get(_request(operation), guard)

        assert runner.calls == 0
        assert transport.requests == []

    asyncio.run(exercise())


def test_async_client_allowed_caller_guard_does_not_bypass_denied_effective_capability() -> None:
    async def exercise() -> None:
        operation = _request().operation_id
        transport = _AsyncTransport()
        runner = _AsyncProbeRunner(ProbeResponseClass.UNAVAILABLE)
        client = AsyncCatalogClient(transport, _profile(), probe_runner=runner)

        with pytest.raises(UnsupportedCapabilityError):
            await client.datasets.get(_request(operation), _guard(operation))

        assert runner.calls == 1
        assert transport.requests == []

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ("credentials", "header", "value"),
    [
        (SocrataCredential(app_token="async-app"), "X-App-Token", "async-app"),
        (_FailingResolver(explicit=UDataCredential(api_key="async-key")), "X-API-KEY", "async-key"),
    ],
)
def test_async_client_honors_static_and_resolver_credentials_without_resolve_async(
    credentials: object, header: str, value: str
) -> None:
    async def exercise() -> RuntimeRequest:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile(), credentials=credentials)

        await client.datasets.get(_request(), _guard())

        return transport.requests[0]

    assert asyncio.run(exercise()).headers[header] == value


def test_async_client_exposes_its_transport_seam() -> None:
    async def exercise() -> bool:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile())
        return client.transport is transport

    assert asyncio.run(exercise())


def test_async_client_aclose_marks_closed_without_closing_borrowed_transport() -> None:
    async def exercise() -> int:
        transport = _AsyncTransport()
        client = AsyncCatalogClient(transport, _profile(), owns_transport=False)

        await client.datasets.get(_request(), _guard())
        await client.aclose()
        await client.aclose()
        with pytest.raises(RuntimeError, match="closed"):
            await client.datasets.get(_request(), _guard())
        return transport.close_count

    assert asyncio.run(exercise()) == 0


def test_async_client_context_exit_keeps_borrowed_transport_open() -> None:
    async def exercise() -> tuple[int, int]:
        transport = _AsyncTransport()

        async with AsyncCatalogClient(transport, _profile(), owns_transport=False) as client:
            await client.datasets.get(_request(), _guard())

        return transport.close_count, len(transport.requests)

    close_count, dispatches = asyncio.run(exercise())
    assert close_count == 0
    assert dispatches == 1


def test_async_client_context_exit_closes_owned_transport_once() -> None:
    async def exercise() -> int:
        transport = _AsyncTransport()
        async with AsyncCatalogClient(transport, _profile()) as client:
            await client.datasets.get(_request(), _guard())
        return transport.close_count

    assert asyncio.run(exercise()) == 1
