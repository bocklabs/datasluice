"""Dual-mode uData taxonomy, schema, format, and badge service."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models.taxonomies import (
    BadgeCreateInput,
    SuggestQuery,
    TaxonomyMutationResult,
)
from datasluice.connectors.catalog.udata.services.resources import _attach, _receipt
from datasluice.connectors.catalog.udata.wire import taxonomies as wire
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import NativeCatalogError

from .datasets import _enforce_mutation_policy, _error_status, _mutation_outcome, _require_mutation_permission

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None
type Response = tuple[int, object, object]


def _target(dataset_id: str, kind: str | None = None) -> str:
    return dataset_id if kind is None else f"{dataset_id}:{kind}"


def _tax_receipt(target: str, policy: Policy, outcome: str, status: int, mutation: str, operation: str):
    return _receipt(
        policy,
        target,
        outcome,
        status,
        mutation,
        resource_kind=ResourceKind.DATASET,
        operation=operation,
    )


def _reject(error: BaseException, target: str, policy: Policy, mutation: str, operation: str) -> None:
    _attach(error, _tax_receipt(target, policy, "rejected", _error_status(error), mutation, operation))
    raise error


def _mutation(
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    request: Callable[[], tuple[str, str, dict[str, str], object]],
    dispatch: Callable[[str, str, dict[str, str], object], Response],
    *,
    destructive: bool = False,
) -> TaxonomyMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, mutation, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = dispatch(method, path, headers, body)
        result = TaxonomyMutationResult(
            _tax_receipt(target, policy, "succeeded", status, mutation, operation),
            MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
        )
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        outcome = (
            "cancelled"
            if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
            else _mutation_outcome(error, response)
        )
        receipt = _tax_receipt(target, policy, outcome, _error_status(error, response), mutation, operation)
        _attach(error, receipt)
        raise
    return result


async def _mutation_async(
    target: str,
    policy: Policy,
    mutation: str,
    operation: str,
    request: Callable[[], tuple[str, str, dict[str, str], object]],
    dispatch: Callable[[str, str, dict[str, str], object], Awaitable[Response]],
    *,
    destructive: bool = False,
) -> TaxonomyMutationResult:
    try:
        method, path, headers, body = request()
    except (Exception, KeyboardInterrupt, GeneratorExit) as error:
        _reject(error, target, policy, mutation, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = await dispatch(method, path, headers, body)
        result = TaxonomyMutationResult(
            _tax_receipt(target, policy, "succeeded", status, mutation, operation),
            MappingRecord(payload) if isinstance(payload, Mapping) and payload else None,
        )
    except (Exception, asyncio.CancelledError, KeyboardInterrupt, GeneratorExit) as error:
        outcome = (
            "cancelled"
            if isinstance(error, (KeyboardInterrupt, GeneratorExit, asyncio.CancelledError))
            else _mutation_outcome(error, response)
        )
        receipt = _tax_receipt(target, policy, outcome, _error_status(error, response), mutation, operation)
        _attach(error, receipt)
        raise
    return result


class SyncTaxonomiesService:
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
            lambda method, path, headers, body: self._call(
                method, path, headers, body, permissions, mutation_policy, wire.ADD_BADGE_OPERATION
            ),
        )

    def delete_badge(
        self,
        dataset_id: str,
        kind: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TaxonomyMutationResult:
        target = _target(dataset_id, kind)
        return _mutation(
            target,
            mutation_policy,
            "deleted",
            wire.DELETE_BADGE_OPERATION,
            lambda: wire.delete_badge_request(dataset_id, kind),
            lambda method, path, headers, body: self._call(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_BADGE_OPERATION
            ),
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

    def _read[T](
        self, request: tuple[str, str, dict[str, str], object], operation: str, parser: Callable[[object, str], T]
    ) -> T:
        method, path, headers, body = request
        _, payload, _ = self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
        )
        return parser(payload, operation)

    def _call(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: object,
        permissions: Permissions,
        policy: Policy,
        operation: str,
    ) -> Response:
        resolved = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
        )


class AsyncTaxonomiesService:
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
            lambda method, path, headers, body: self._call(
                method, path, headers, body, permissions, mutation_policy, wire.ADD_BADGE_OPERATION
            ),
        )

    async def delete_badge(
        self,
        dataset_id: str,
        kind: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> TaxonomyMutationResult:
        target = _target(dataset_id, kind)
        return await _mutation_async(
            target,
            mutation_policy,
            "deleted",
            wire.DELETE_BADGE_OPERATION,
            lambda: wire.delete_badge_request(dataset_id, kind),
            lambda method, path, headers, body: self._call(
                method, path, headers, body, permissions, mutation_policy, wire.DELETE_BADGE_OPERATION
            ),
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

    async def _read[T](
        self,
        request: tuple[str, str, dict[str, str], object],
        operation: str,
        parser: Callable[[object, str], T],
    ) -> T:
        method, path, headers, body = request
        _, payload, _ = await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
        )
        return parser(payload, operation)

    async def _call(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: object,
        permissions: Permissions,
        policy: Policy,
        operation: str,
    ) -> Response:
        resolved = _require_mutation_permission(
            await self._client._resolved_credential_async(),
            operation,
            permissions,
        )
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=operation,
            headers=headers,
            json_body=body,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
        )
