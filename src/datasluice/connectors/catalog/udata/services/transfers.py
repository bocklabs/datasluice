"""Dual-mode uData transfer-request service over the shared guarded dispatch."""

from __future__ import annotations

import asyncio
from functools import partial
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.transfers import (
    TransferListQuery,
    TransferMutationResult,
    TransferRequestInput,
    TransferResponseInput,
)
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import transfers as wire
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.errors.catalog import NativeCatalogError, attach_catalog_metadata
from datasluice.runtime.transport.base import TransportError

from .datasets import _error_status, _mutation_outcome, _require_mutation_permission
from .taxonomies import (
    AsyncCatalogService,
    Permissions,
    Policy,
    Request,
    Response,
    SyncCatalogService,
    _dataset_mutate,
    _parsed,
    _run_mutation,
    _run_mutation_async,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient
    from datasluice.domain.catalog.models import MappingRecord

_MAX_TRANSFER_READ_BYTES = 1_048_576
_CANCELLATIONS = (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError)

_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    TransferMutationResult,
    ResourceKind.DATASET,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    TransferMutationResult,
    ResourceKind.DATASET,
)


def _settle_ambiguous(error: BaseException) -> None:
    """Record that one unsettled transfer write may already have changed its target.

    A transfer write is irreversible from the caller's seat: accepting a request
    moves the subject's owner and the deployment may fail between recording the
    accepted transfer and saving the subject. Any failure that leaves the
    deployment state unknown must therefore settle ``ambiguous``, because
    ``failed`` would assert the target is unchanged. A pre-dispatch refusal, an
    exhausted budget, an open breaker, and a definitive 4xx refusal all do assert
    an unchanged target and are left to the shared classification.
    """
    if isinstance(error, (*_CANCELLATIONS, TransportError)) or _mutation_outcome(error, None) == "rejected":
        return
    status = _error_status(error)
    if status == 0 or 400 <= status < 500:
        return
    attach_catalog_metadata(error, {"ambiguous": True})


def _guarded[R](dispatch: Callable[[], R]) -> R:
    """Dispatch one transfer write, recording an unsettled outcome as ambiguous."""
    try:
        return dispatch()
    except SETTLEMENT_ERRORS as error:
        _settle_ambiguous(error)
        raise


async def _guarded_async[R](dispatch: Callable[[], Awaitable[R]]) -> R:
    """Await one transfer write, recording an unsettled outcome as ambiguous."""
    try:
        return await dispatch()
    except ASYNC_SETTLEMENT_ERRORS as error:
        _settle_ambiguous(error)
        raise


class SyncTransfersService(SyncCatalogService):
    """Typed synchronous transfer-request operations."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_transfers(self, query: TransferListQuery, permissions: Permissions) -> tuple[MappingRecord, ...]:
        operation = wire.LIST_TRANSFERS_OPERATION
        return _parsed(
            self._read_transfers(wire.list_transfers_request(query), permissions, operation),
            operation,
            wire.parse_transfers,
        )

    def get_transfer(self, transfer_id: str, permissions: Permissions) -> MappingRecord:
        operation = wire.GET_TRANSFER_OPERATION
        return _parsed(
            self._read_transfers(wire.get_transfer_request(transfer_id), permissions, operation),
            operation,
            wire.parse_transfer,
        )

    def request_transfer(
        self,
        client_input: TransferRequestInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TransferMutationResult:
        return _mutation(
            client_input.mutation_target(),
            mutation_policy,
            "requested",
            wire.REQUEST_TRANSFER_OPERATION,
            lambda: wire.request_transfer_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.REQUEST_TRANSFER_OPERATION),
        )

    def respond_to_transfer(
        self,
        transfer_id: str,
        client_input: TransferResponseInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TransferMutationResult:
        return _mutation(
            transfer_id,
            mutation_policy,
            "accepted" if client_input.accepts else "refused",
            wire.RESPOND_TO_TRANSFER_OPERATION,
            lambda: wire.respond_to_transfer_request(transfer_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.RESPOND_TO_TRANSFER_OPERATION),
            destructive=client_input.accepts,
        )

    def _read_transfers(self, request: Request, permissions: Permissions, operation: str) -> Response:
        method, path, headers, body = request
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=_require_mutation_permission(self._client._resolved_credential(), operation, permissions),
            max_response_bytes=_MAX_TRANSFER_READ_BYTES,
        )

    def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        return _guarded(
            lambda: _dataset_mutate(
                self._client._dataset_call,
                self._client._resolved_credential(),
                permissions,
                policy,
                operation,
                request,
            )
        )


class AsyncTransfersService(AsyncCatalogService):
    """Typed asynchronous transfer-request operations mirroring the sync surface."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_transfers(self, query: TransferListQuery, permissions: Permissions) -> tuple[MappingRecord, ...]:
        operation = wire.LIST_TRANSFERS_OPERATION
        response = await self._read_transfers(wire.list_transfers_request(query), permissions, operation)
        return _parsed(response, operation, wire.parse_transfers)

    async def get_transfer(self, transfer_id: str, permissions: Permissions) -> MappingRecord:
        operation = wire.GET_TRANSFER_OPERATION
        response = await self._read_transfers(wire.get_transfer_request(transfer_id), permissions, operation)
        return _parsed(response, operation, wire.parse_transfer)

    async def request_transfer(
        self,
        client_input: TransferRequestInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TransferMutationResult:
        return await _mutation_async(
            client_input.mutation_target(),
            mutation_policy,
            "requested",
            wire.REQUEST_TRANSFER_OPERATION,
            lambda: wire.request_transfer_request(client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.REQUEST_TRANSFER_OPERATION),
        )

    async def respond_to_transfer(
        self,
        transfer_id: str,
        client_input: TransferResponseInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TransferMutationResult:
        return await _mutation_async(
            transfer_id,
            mutation_policy,
            "accepted" if client_input.accepts else "refused",
            wire.RESPOND_TO_TRANSFER_OPERATION,
            lambda: wire.respond_to_transfer_request(transfer_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.RESPOND_TO_TRANSFER_OPERATION),
            destructive=client_input.accepts,
        )

    async def _read_transfers(self, request: Request, permissions: Permissions, operation: str) -> Response:
        method, path, headers, body = request
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=_require_mutation_permission(
                await self._client._resolved_credential_async(), operation, permissions
            ),
            max_response_bytes=_MAX_TRANSFER_READ_BYTES,
        )

    async def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        credential = await self._client._resolved_credential_async()
        return await _guarded_async(
            lambda: _dataset_mutate(
                self._client._dataset_call_async, credential, permissions, policy, operation, request
            )
        )
