"""Dual-mode uData taxonomy, schema, format, and badge service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from functools import partial
from typing import TYPE_CHECKING, NoReturn, Protocol

from datasluice.connectors.catalog.udata.models.taxonomies import (
    BadgeCreateInput,
    SuggestQuery,
    TaxonomyMutationResult,
)
from datasluice.connectors.catalog.udata.services.resources import _attach, _receipt
from datasluice.connectors.catalog.udata.settlement import ASYNC_SETTLEMENT_ERRORS, SETTLEMENT_ERRORS
from datasluice.connectors.catalog.udata.wire import taxonomies as wire
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError
from datasluice.runtime.transport.base import RuntimeResponse, UploadPart

from .datasets import _enforce_mutation_policy, _error_status, _mutation_outcome, _require_mutation_permission

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None
type Response = tuple[int, object, RuntimeResponse]
type Request = tuple[str, str, dict[str, str], object]
type Parser[T] = Callable[[object, str], T]
type Dispatch = Callable[[Request], Response]
type AsyncDispatch = Callable[[Request], Awaitable[Response]]
type SettlementErrors = tuple[type[BaseException], ...]
type ResultBuilder[T] = Callable[[MutationReceipt, MappingRecord | None], T]


class SupportsUpload(Protocol):
    def part(self) -> UploadPart: ...


def _parsed[T](response: Response, operation: str, parser: Parser[T]) -> T:
    _, payload, _ = response
    return parser(payload, operation)


def _dataset_read[R](call: Callable[..., R], request: Request, operation: str) -> R:
    method, path, headers, body = request
    return call(method=method, path=path, owning_operation=operation, headers=headers, json_body=body)


def _dataset_mutate[R](
    call: Callable[..., R],
    credential: object,
    permissions: Permissions,
    policy: Policy,
    operation: str,
    request: Request,
    *,
    admin: bool = False,
) -> R:
    method, path, headers, body = request
    return call(
        method=method,
        path=path,
        owning_operation=operation,
        headers=headers,
        json_body=body,
        permissions=permissions,
        credential=_require_mutation_permission(credential, operation, permissions, admin=admin),
        idempotency_policy=policy.idempotency if policy else None,
    )


def _dataset_upload[R](
    call: Callable[..., R],
    credential: object,
    permissions: Permissions,
    policy: Policy,
    operation: str,
    route: tuple[str, str, dict[str, str]],
    upload: SupportsUpload,
    *,
    admin: bool = False,
) -> R:
    method, path, headers = route
    return call(
        method=method,
        path=path,
        owning_operation=operation,
        headers=headers,
        permissions=permissions,
        credential=_require_mutation_permission(credential, operation, permissions, admin=admin),
        idempotency_policy=policy.idempotency if policy else None,
        files=(upload.part(),),
    )


def _mutation_record(payload: object) -> MappingRecord | None:
    return MappingRecord(payload) if isinstance(payload, Mapping) and payload else None


def _kind_receipt(
    target: str,
    policy: Policy,
    outcome: str,
    status: int,
    mutation: str,
    operation: str,
    kind: ResourceKind,
) -> MutationReceipt:
    return _receipt(policy, target, outcome, status, mutation, resource_kind=kind, operation=operation)


def _reject(
    error: BaseException,
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    kind: ResourceKind,
) -> NoReturn:
    _attach(error, _kind_receipt(target, policy, "rejected", _error_status(error), mutation, operation, kind))
    raise error


def _failed(
    error: BaseException,
    target: str,
    policy: Policy,
    response: object | None,
    mutation: str,
    operation: str,
    kind: ResourceKind,
) -> NoReturn:
    outcome = (
        "cancelled"
        if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
        else _mutation_outcome(error, response)
    )
    _attach(error, _kind_receipt(target, policy, outcome, _error_status(error, response), mutation, operation, kind))
    raise error


def _succeeded[T](
    build: ResultBuilder[T],
    kind: ResourceKind,
    target: str,
    policy: Policy,
    status: int,
    mutation: str,
    operation: str,
    payload: object,
    success_target: Callable[[object], str] | None,
) -> T:
    settled = success_target(payload) if success_target is not None else target
    return build(
        _kind_receipt(settled, policy, "succeeded", status, mutation, operation, kind),
        _mutation_record(payload),
    )


def _prepared_request(
    request: Callable[[], Request],
    settlement_errors: SettlementErrors,
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    kind: ResourceKind,
) -> Request:
    try:
        return request()
    except settlement_errors as error:
        _reject(error, target, policy, mutation, operation, kind)


def _run_mutation[T](
    settlement_errors: SettlementErrors,
    build: ResultBuilder[T],
    kind: ResourceKind,
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    request: Callable[[], Request],
    dispatch: Dispatch,
    *,
    destructive: bool = False,
    success_target: Callable[[object], str] | None = None,
) -> T:
    prepared = _prepared_request(request, settlement_errors, target, policy, mutation, operation, kind)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = dispatch(prepared)
        return _succeeded(build, kind, target, policy, status, mutation, operation, payload, success_target)
    except settlement_errors as error:
        _failed(error, target, policy, response, mutation, operation, kind)


async def _run_mutation_async[T](
    settlement_errors: SettlementErrors,
    build: ResultBuilder[T],
    kind: ResourceKind,
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    request: Callable[[], Request],
    dispatch: AsyncDispatch,
    *,
    destructive: bool = False,
    success_target: Callable[[object], str] | None = None,
) -> T:
    prepared = _prepared_request(request, settlement_errors, target, policy, mutation, operation, kind)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = await dispatch(prepared)
        return _succeeded(build, kind, target, policy, status, mutation, operation, payload, success_target)
    except settlement_errors as error:
        _failed(error, target, policy, response, mutation, operation, kind)


_mutation = partial(
    _run_mutation,
    SETTLEMENT_ERRORS,
    TaxonomyMutationResult,
    ResourceKind.DATASET,
)
_mutation_async = partial(
    _run_mutation_async,
    ASYNC_SETTLEMENT_ERRORS,
    TaxonomyMutationResult,
    ResourceKind.DATASET,
)


def _target(dataset_id: str, kind: str | None = None) -> str:
    return dataset_id if kind is None else f"{dataset_id}:{kind}"


class SyncCatalogService:
    _client: SyncUDataClient

    def _read[T](self, request: Request, operation: str, parser: Parser[T]) -> T:
        return _parsed(_dataset_read(self._client._dataset_call, request, operation), operation, parser)

    def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        return _dataset_mutate(
            self._client._dataset_call, self._client._resolved_credential(), permissions, policy, operation, request
        )


class AsyncCatalogService:
    _client: AsyncUDataClient

    async def _read[T](self, request: Request, operation: str, parser: Parser[T]) -> T:
        return _parsed(await _dataset_read(self._client._dataset_call_async, request, operation), operation, parser)

    async def _mutate(self, request: Request, permissions: Permissions, policy: Policy, operation: str) -> Response:
        return await _dataset_mutate(
            self._client._dataset_call_async,
            await self._client._resolved_credential_async(),
            permissions,
            policy,
            operation,
            request,
        )


class SyncTaxonomiesService(SyncCatalogService):
    """Typed synchronous methods for the assigned taxonomy family."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def available_badges(self) -> MappingRecord:
        return self._read(wire.available_badges_request(), wire.AVAILABLE_BADGES_OPERATION, wire.parse_mapping)

    def add_badge(
        self,
        dataset_id: str,
        client_input: BadgeCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TaxonomyMutationResult:
        return _mutation(
            dataset_id,
            mutation_policy,
            "added",
            wire.ADD_BADGE_OPERATION,
            lambda: wire.add_badge_request(dataset_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.ADD_BADGE_OPERATION),
        )

    def delete_badge(
        self,
        dataset_id: str,
        kind: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TaxonomyMutationResult:
        return _mutation(
            _target(dataset_id, kind),
            mutation_policy,
            "deleted",
            wire.DELETE_BADGE_OPERATION,
            lambda: wire.delete_badge_request(dataset_id, kind),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_BADGE_OPERATION),
            destructive=True,
        )

    def suggest_formats(self, query: SuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read(wire.suggest_request("formats", query), wire.SUGGEST_FORMATS_OPERATION, wire.parse_objects)

    def suggest_mime(self, query: SuggestQuery) -> tuple[MappingRecord, ...]:
        return self._read(wire.suggest_request("mime", query), wire.SUGGEST_MIME_OPERATION, wire.parse_objects)

    def licenses(self) -> tuple[MappingRecord, ...]:
        return self._read(wire.licenses_request(), wire.LICENSES_OPERATION, wire.parse_objects)

    def frequencies(self) -> tuple[MappingRecord, ...]:
        return self._read(wire.frequencies_request(), wire.FREQUENCIES_OPERATION, wire.parse_objects)

    def extensions(self) -> tuple[str, ...]:
        return self._read(wire.extensions_request(), wire.EXTENSIONS_OPERATION, wire.parse_strings)

    def schemas(self) -> tuple[MappingRecord, ...]:
        return self._read(wire.schemas_request(), wire.SCHEMAS_OPERATION, wire.parse_objects)

    def dataset_schemas(self, dataset_id: str) -> tuple[MappingRecord, ...]:
        return self._read(wire.dataset_schemas_request(dataset_id), wire.DATASET_SCHEMAS_OPERATION, wire.parse_objects)


class AsyncTaxonomiesService(AsyncCatalogService):
    """Typed asynchronous methods for the assigned taxonomy family."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def available_badges(self) -> MappingRecord:
        return await self._read(wire.available_badges_request(), wire.AVAILABLE_BADGES_OPERATION, wire.parse_mapping)

    async def add_badge(
        self,
        dataset_id: str,
        client_input: BadgeCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TaxonomyMutationResult:
        return await _mutation_async(
            dataset_id,
            mutation_policy,
            "added",
            wire.ADD_BADGE_OPERATION,
            lambda: wire.add_badge_request(dataset_id, client_input),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.ADD_BADGE_OPERATION),
        )

    async def delete_badge(
        self,
        dataset_id: str,
        kind: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TaxonomyMutationResult:
        return await _mutation_async(
            _target(dataset_id, kind),
            mutation_policy,
            "deleted",
            wire.DELETE_BADGE_OPERATION,
            lambda: wire.delete_badge_request(dataset_id, kind),
            lambda request: self._mutate(request, permissions, mutation_policy, wire.DELETE_BADGE_OPERATION),
            destructive=True,
        )

    async def suggest_formats(self, query: SuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.suggest_request("formats", query), wire.SUGGEST_FORMATS_OPERATION, wire.parse_objects
        )

    async def suggest_mime(self, query: SuggestQuery) -> tuple[MappingRecord, ...]:
        return await self._read(wire.suggest_request("mime", query), wire.SUGGEST_MIME_OPERATION, wire.parse_objects)

    async def licenses(self) -> tuple[MappingRecord, ...]:
        return await self._read(wire.licenses_request(), wire.LICENSES_OPERATION, wire.parse_objects)

    async def frequencies(self) -> tuple[MappingRecord, ...]:
        return await self._read(wire.frequencies_request(), wire.FREQUENCIES_OPERATION, wire.parse_objects)

    async def extensions(self) -> tuple[str, ...]:
        return await self._read(wire.extensions_request(), wire.EXTENSIONS_OPERATION, wire.parse_strings)

    async def schemas(self) -> tuple[MappingRecord, ...]:
        return await self._read(wire.schemas_request(), wire.SCHEMAS_OPERATION, wire.parse_objects)

    async def dataset_schemas(self, dataset_id: str) -> tuple[MappingRecord, ...]:
        return await self._read(
            wire.dataset_schemas_request(dataset_id), wire.DATASET_SCHEMAS_OPERATION, wire.parse_objects
        )
