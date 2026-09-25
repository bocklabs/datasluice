"""Dual-mode uData resource and bounded-upload service."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.mapping import UDataPageEnvelope
from datasluice.connectors.catalog.udata.models.resources import (
    MidStreamUploadError,
    ResourceCreateInput,
    ResourceMutationResult,
    ResourceUpdateInput,
    ResourceUploadInput,
)
from datasluice.connectors.catalog.udata.wire import datasets as dataset_wire
from datasluice.connectors.catalog.udata.wire import resources as wire
from datasluice.domain.catalog.auth import EffectivePermissions
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import NativeRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import attach_catalog_metadata
from datasluice.runtime.mutation import build_mutation_receipt

from .datasets import (
    _enforce_mutation_policy,
    _error_status,
    _mutation_outcome,
    _operation_id,
    _receipt_policy,
    _require_mutation_permission,
    _safe_target_value,
)

if TYPE_CHECKING:
    from datasluice.connectors.catalog.udata.clients import AsyncUDataClient, SyncUDataClient

type Permissions = EffectivePermissions
type Policy = MutationPolicy | None
type Result = ResourceMutationResult


def _receipt(
    policy: Policy,
    target: str,
    outcome: str,
    status: int,
    mutation: str,
    *,
    resource_kind: ResourceKind = ResourceKind.RESOURCE,
    operation: str = wire.RESOURCE_OPERATION,
) -> MutationReceipt:
    safe_target = _safe_target_value(target)
    return build_mutation_receipt(
        _operation_id(operation),
        CatalogId(platform=CatalogPlatform.UDATA, resource_kind=resource_kind, value=safe_target),
        _receipt_policy(policy),
        outcome,
        {"mutation": mutation, "status_code": status, "target_valid": bool(target)},
    )


def _attach(error: BaseException, receipt: MutationReceipt) -> None:
    attach_catalog_metadata(error, {"receipt": receipt.to_dict()})
    if isinstance(getattr(error, "__dict__", None), dict):
        error.__dict__["mutation_receipt"] = receipt


def _close_upload(
    client_input: ResourceUploadInput,
    result: Result | None,
    primary_error: BaseException | None,
) -> None:
    try:
        client_input.close()
    except BaseException as close_error:
        receipt = result.receipt if result is not None else getattr(primary_error, "mutation_receipt", None)
        if isinstance(receipt, MutationReceipt):
            _attach(close_error, receipt)
        if primary_error is not None:
            raise primary_error from close_error
        raise


def _reject_route(
    error: BaseException, target: str, policy: Policy, mutation: str, resource_kind: ResourceKind, operation: str
) -> None:
    receipt = _receipt(
        policy, target, "rejected", _error_status(error), mutation, resource_kind=resource_kind, operation=operation
    )
    _attach(error, receipt)
    raise error


def _mutation_result(receipt: MutationReceipt, payload: object, mutation: str) -> Result:
    if mutation == "reordered":
        if not isinstance(payload, list):
            raise ValueError("The uData resource reorder response must be a list.")
        return ResourceMutationResult(receipt=receipt, records=tuple(wire.parse_resource(item) for item in payload))
    if mutation == "extras_updated" or (mutation == "extras_deleted" and payload is not None):
        return ResourceMutationResult(
            receipt=receipt,
            extras={} if payload is None else dataset_wire.parse_extras(payload, operation=wire.RESOURCE_OPERATION),
        )
    if mutation == "deleted" or mutation == "extras_deleted":
        return ResourceMutationResult(receipt=receipt)
    return ResourceMutationResult(receipt=receipt, record=wire.parse_resource(payload))


def _route_operation(mutation: str, resource_kind: ResourceKind, operation: str) -> str:
    if operation != wire.RESOURCE_OPERATION:
        return operation
    if mutation == "reordered":
        return wire.REORDER_OPERATION
    if mutation == "created":
        return wire.CREATE_OPERATION if resource_kind is ResourceKind.DATASET else wire.COMMUNITY_CREATE_OPERATION
    if mutation == "uploaded":
        return wire.UPLOAD_REPLACE_OPERATION if resource_kind is ResourceKind.RESOURCE else wire.UPLOAD_NEW_OPERATION
    if mutation == "updated":
        return wire.RESOURCE_UPDATE_OPERATION
    if mutation == "deleted":
        return wire.RESOURCE_DELETE_OPERATION
    if mutation == "extras_updated":
        return wire.EXTRAS_UPDATE_OPERATION
    if mutation == "extras_deleted":
        return wire.EXTRAS_DELETE_OPERATION
    return operation


def _dispatch_operation(policy: Policy) -> str:
    if policy is not None and policy.confirmation is not None and isinstance(policy.confirmation.operation, str):
        return policy.confirmation.operation
    return (
        wire.RESOURCE_DELETE_CAPABILITY
        if policy is not None and policy.destructive
        else wire.RESOURCE_MUTATION_CAPABILITY
    )


def _resource_mutation(
    target: str,
    policy: Policy,
    mutation: str,
    destructive: bool,
    dispatch: Callable[[], tuple[int, object, object]],
    *,
    resource_kind: ResourceKind = ResourceKind.RESOURCE,
    operation: str = wire.RESOURCE_OPERATION,
) -> Result:
    operation = _route_operation(mutation, resource_kind, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = dispatch()
        return _mutation_result(
            _receipt(policy, target, "succeeded", status, mutation, resource_kind=resource_kind, operation=operation),
            payload,
            mutation,
        )
    except BaseException as error:
        outcome = (
            "cancelled" if isinstance(error, (KeyboardInterrupt, GeneratorExit)) else _mutation_outcome(error, response)
        )
        if mutation == "uploaded" and isinstance(error, (OSError, MidStreamUploadError)):
            outcome = "ambiguous"
        receipt = _receipt(
            policy,
            target,
            outcome,
            _error_status(error, response),
            mutation,
            resource_kind=resource_kind,
            operation=operation,
        )
        _attach(error, receipt)
        raise


async def _async_resource_mutation(
    target: str,
    policy: Policy,
    mutation: str,
    destructive: bool,
    dispatch: Callable[[], Awaitable[tuple[int, object, object]]],
    *,
    resource_kind: ResourceKind = ResourceKind.RESOURCE,
    operation: str = wire.RESOURCE_OPERATION,
) -> Result:
    operation = _route_operation(mutation, resource_kind, operation)
    response: object | None = None
    try:
        _enforce_mutation_policy(operation, target, policy, destructive=destructive)
        status, payload, response = await dispatch()
        return _mutation_result(
            _receipt(policy, target, "succeeded", status, mutation, resource_kind=resource_kind, operation=operation),
            payload,
            mutation,
        )
    except BaseException as error:
        outcome = (
            "cancelled" if isinstance(error, (KeyboardInterrupt, GeneratorExit)) else _mutation_outcome(error, response)
        )
        if mutation == "uploaded" and isinstance(error, (OSError, MidStreamUploadError)):
            outcome = "ambiguous"
        receipt = _receipt(
            policy,
            target,
            outcome,
            _error_status(error, response),
            mutation,
            resource_kind=resource_kind,
            operation=operation,
        )
        _attach(error, receipt)
        raise


class SyncResourcesService:
    """Typed synchronous methods for every assigned resource row."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    def redirect(self, resource_id: str) -> str:
        method, path, _, _ = wire.redirect_resource_request(resource_id)
        _, headers, _ = self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=wire.REDIRECT_OPERATION,
            raw_text=True,
            redirect_mode=True,
        )
        return wire.redirect_location(headers)

    def create(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.create_resource_request(dataset_id, client_input)
        except BaseException as error:
            _reject_route(error, dataset_id, mutation_policy, "created", ResourceKind.DATASET, wire.CREATE_OPERATION)
        return _resource_mutation(
            dataset_id,
            mutation_policy,
            "created",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            resource_kind=ResourceKind.DATASET,
            operation=wire.CREATE_OPERATION,
        )

    def reorder(
        self,
        dataset_id: str,
        values: tuple[ResourceUpdateInput, ...],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.update_resources_request(dataset_id, values)
        except BaseException as error:
            _reject_route(error, dataset_id, mutation_policy, "reordered", ResourceKind.DATASET, wire.REORDER_OPERATION)
        return _resource_mutation(
            dataset_id,
            mutation_policy,
            "reordered",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            resource_kind=ResourceKind.DATASET,
            operation=wire.REORDER_OPERATION,
        )

    def upload(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
        resource_id: str | None = None,
        community: bool = False,
    ) -> Result:
        target = resource_id if resource_id is not None else dataset_id
        resource_kind = ResourceKind.RESOURCE if resource_id is not None else ResourceKind.DATASET
        operation = (
            wire.UPLOAD_COMMUNITY_REPLACE_OPERATION
            if community and resource_id is not None
            else wire.UPLOAD_REPLACE_OPERATION
            if resource_id is not None
            else wire.UPLOAD_COMMUNITY_NEW_OPERATION
            if community
            else wire.UPLOAD_NEW_OPERATION
        )
        result: Result | None = None
        primary_error: BaseException | None = None
        try:
            method, path, headers = wire.upload_resource_request(dataset_id, resource_id, community=community)
            result = _resource_mutation(
                target,
                mutation_policy,
                "uploaded",
                resource_id is not None,
                lambda: self._upload(method, path, headers, client_input, permissions, mutation_policy),
                resource_kind=resource_kind,
                operation=operation,
            )
            return result
        except BaseException as error:
            primary_error = error
            if not isinstance(getattr(error, "mutation_receipt", None), MutationReceipt):
                _attach(
                    error,
                    _receipt(
                        mutation_policy,
                        target,
                        "rejected",
                        _error_status(error),
                        "uploaded",
                        resource_kind=resource_kind,
                        operation=operation,
                    ),
                )
            raise
        finally:
            _close_upload(client_input, result, primary_error)

    def upload_community(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        return self.upload(dataset_id, client_input, permissions, mutation_policy, community=True)

    def reupload_community(
        self,
        resource_id: str,
        client_input: ResourceUploadInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        result: Result | None = None
        primary_error: BaseException | None = None
        try:
            method, path, headers = wire.reupload_community_request(resource_id)
            result = _resource_mutation(
                resource_id,
                mutation_policy,
                "uploaded",
                True,
                lambda: self._upload(method, path, headers, client_input, permissions, mutation_policy),
                operation=wire.UPLOAD_COMMUNITY_REPLACE_OPERATION,
            )
            return result
        except BaseException as error:
            primary_error = error
            if not isinstance(getattr(error, "mutation_receipt", None), MutationReceipt):
                _attach(
                    error,
                    _receipt(
                        mutation_policy,
                        resource_id,
                        "rejected",
                        _error_status(error),
                        "uploaded",
                        operation=wire.UPLOAD_COMMUNITY_REPLACE_OPERATION,
                    ),
                )
            raise
        finally:
            _close_upload(client_input, result, primary_error)

    def get(self, dataset_id: str, resource_id: str) -> NativeRecord:
        method, path, _, _ = wire.resource_request("GET", dataset_id, resource_id)
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.RESOURCE_GET_OPERATION
        )
        return wire.parse_resource(payload)

    def update(
        self,
        dataset_id: str,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.resource_request("PUT", dataset_id, resource_id, body=client_input.payload())
        except BaseException as error:
            _reject_route(
                error, resource_id, mutation_policy, "updated", ResourceKind.RESOURCE, wire.RESOURCE_UPDATE_OPERATION
            )
        return _resource_mutation(
            resource_id,
            mutation_policy,
            "updated",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
        )

    def delete(
        self,
        dataset_id: str,
        resource_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, _ = wire.resource_request("DELETE", dataset_id, resource_id)
        except BaseException as error:
            _reject_route(
                error, resource_id, mutation_policy, "deleted", ResourceKind.RESOURCE, wire.RESOURCE_DELETE_OPERATION
            )
        return _resource_mutation(
            resource_id,
            mutation_policy,
            "deleted",
            True,
            lambda: self._call(method, path, None, permissions, mutation_policy),
        )

    def list_community(self, params: Mapping[str, str | int] | None = None) -> UDataPageEnvelope:
        method, path, _, _ = wire.list_community_resources_request(params)
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.COMMUNITY_LIST_OPERATION
        )
        return wire.parse_resource_page(payload)

    def create_community(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.community_collection_request(
                "POST", client_input.payload() | {"dataset": dataset_id}
            )
        except BaseException as error:
            _reject_route(
                error, dataset_id, mutation_policy, "created", ResourceKind.DATASET, wire.COMMUNITY_CREATE_OPERATION
            )
        return _resource_mutation(
            dataset_id,
            mutation_policy,
            "created",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            resource_kind=ResourceKind.DATASET,
            operation=wire.COMMUNITY_CREATE_OPERATION,
        )

    def get_community(self, resource_id: str) -> NativeRecord:
        method, path, _, _ = wire.resource_request("GET", "", resource_id, community=True)
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.COMMUNITY_GET_OPERATION
        )
        return wire.parse_resource(payload)

    def update_community(
        self,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        return self._community_mutation(
            "PUT", resource_id, client_input.payload(), permissions, mutation_policy, False, "updated"
        )

    def delete_community(self, resource_id: str, permissions: Permissions, mutation_policy: Policy = None) -> Result:
        return self._community_mutation("DELETE", resource_id, None, permissions, mutation_policy, True, "deleted")

    def resource_types(self) -> tuple[Mapping[str, str], ...]:
        method, path, _, _ = wire.resource_types_request()
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.RESOURCE_TYPES_OPERATION
        )
        return wire.parse_resource_types(payload)

    def list_v2(self, dataset_id: str) -> UDataPageEnvelope:
        method, path, _, _ = wire.v2_resource_request(dataset_id)
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.V2_RESOURCE_LIST_OPERATION
        )
        return wire.parse_resource_page(payload)

    def get_v2(self, resource_id: str) -> NativeRecord:
        method, path, _, _ = wire.v2_resource_request("", resource_id)
        _, payload, _ = self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=wire.V2_RESOURCE_GET_OPERATION,
        )
        return wire.parse_resource(payload.get("resource") if isinstance(payload, Mapping) else payload)

    def get_dataset_v2(self, dataset_id: str) -> NativeRecord:
        method, path, _, _ = wire.v2_dataset_request(dataset_id)
        _, payload, _ = self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=wire.V2_DATASET_GET_OPERATION,
        )
        return wire.parse_v2_dataset(payload)

    def get_extras_v2(self, dataset_id: str, resource_id: str) -> Mapping[str, object]:
        method, path, _, _ = wire.v2_extras_request("GET", dataset_id, resource_id)
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, owning_operation=wire.V2_EXTRAS_GET_OPERATION
        )
        return dataset_wire.parse_extras(payload, operation=wire.RESOURCE_OPERATION)

    def update_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        values: Mapping[str, object],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.v2_extras_request("PUT", dataset_id, resource_id, dict(values))
        except BaseException as error:
            _reject_route(
                error,
                resource_id,
                mutation_policy,
                "extras_updated",
                ResourceKind.RESOURCE,
                wire.EXTRAS_UPDATE_OPERATION,
            )
        return _resource_mutation(
            resource_id,
            mutation_policy,
            "extras_updated",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
        )

    def delete_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        keys: tuple[str, ...],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.v2_extras_request("DELETE", dataset_id, resource_id, list(keys))
        except BaseException as error:
            _reject_route(
                error,
                resource_id,
                mutation_policy,
                "extras_deleted",
                ResourceKind.RESOURCE,
                wire.EXTRAS_DELETE_OPERATION,
            )
        return _resource_mutation(
            resource_id,
            mutation_policy,
            "extras_deleted",
            True,
            lambda: self._call(method, path, body, permissions, mutation_policy),
        )

    def _call(
        self, method: str, path: str, body: object, permissions: Permissions, policy: Policy
    ) -> tuple[int, object, object]:
        capability_operation = _dispatch_operation(policy)
        resolved = _require_mutation_permission(self._client._resolved_credential(), capability_operation, permissions)
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=capability_operation,
            json_body=body,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
        )

    def _upload(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        client_input: ResourceUploadInput,
        permissions: Permissions,
        policy: Policy,
    ) -> tuple[int, object, object]:
        capability_operation = _dispatch_operation(policy)
        resolved = _require_mutation_permission(self._client._resolved_credential(), capability_operation, permissions)
        return self._client._dataset_call(
            method=method,
            path=path,
            owning_operation=capability_operation,
            headers=headers,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
            files=(client_input.part(),),
        )

    def _community_mutation(
        self,
        method: str,
        target: str,
        body: object,
        permissions: Permissions,
        policy: Policy,
        destructive: bool,
        mutation: str,
    ) -> Result:
        try:
            _, path, _, _ = wire.resource_request(method, "", target, community=True, body=body)
        except BaseException as error:
            _reject_route(
                error,
                target,
                policy,
                mutation,
                ResourceKind.RESOURCE,
                wire.COMMUNITY_CREATE_OPERATION
                if mutation == "created"
                else wire.COMMUNITY_DELETE_OPERATION
                if destructive
                else wire.COMMUNITY_UPDATE_OPERATION,
            )
        return _resource_mutation(
            target,
            policy,
            mutation,
            destructive,
            lambda: self._call(method, path, body, permissions, policy),
            operation=(
                wire.COMMUNITY_CREATE_OPERATION
                if mutation == "created"
                else wire.COMMUNITY_DELETE_OPERATION
                if destructive
                else wire.COMMUNITY_UPDATE_OPERATION
            ),
        )


class AsyncResourcesService:
    """Typed asynchronous resource methods mirroring the sync surface."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    async def redirect(self, resource_id: str) -> str:
        method, path, _, _ = wire.redirect_resource_request(resource_id)
        _, headers, _ = await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=wire.REDIRECT_OPERATION,
            raw_text=True,
            redirect_mode=True,
        )
        return wire.redirect_location(headers)

    async def create(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.create_resource_request(dataset_id, client_input)
        except BaseException as error:
            _reject_route(error, dataset_id, mutation_policy, "created", ResourceKind.DATASET, wire.CREATE_OPERATION)
        return await _async_resource_mutation(
            dataset_id,
            mutation_policy,
            "created",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            resource_kind=ResourceKind.DATASET,
            operation=wire.CREATE_OPERATION,
        )

    async def reorder(
        self,
        dataset_id: str,
        values: tuple[ResourceUpdateInput, ...],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.update_resources_request(dataset_id, values)
        except BaseException as error:
            _reject_route(error, dataset_id, mutation_policy, "reordered", ResourceKind.DATASET, wire.REORDER_OPERATION)
        return await _async_resource_mutation(
            dataset_id,
            mutation_policy,
            "reordered",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            resource_kind=ResourceKind.DATASET,
            operation=wire.REORDER_OPERATION,
        )

    async def upload(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
        resource_id: str | None = None,
        community: bool = False,
    ) -> Result:
        target = resource_id if resource_id is not None else dataset_id
        resource_kind = ResourceKind.RESOURCE if resource_id is not None else ResourceKind.DATASET
        operation = (
            wire.UPLOAD_COMMUNITY_REPLACE_OPERATION
            if community and resource_id is not None
            else wire.UPLOAD_REPLACE_OPERATION
            if resource_id is not None
            else wire.UPLOAD_COMMUNITY_NEW_OPERATION
            if community
            else wire.UPLOAD_NEW_OPERATION
        )
        result: Result | None = None
        primary_error: BaseException | None = None
        try:
            method, path, headers = wire.upload_resource_request(dataset_id, resource_id, community=community)
            result = await _async_resource_mutation(
                target,
                mutation_policy,
                "uploaded",
                resource_id is not None,
                lambda: self._upload(method, path, headers, client_input, permissions, mutation_policy),
                resource_kind=resource_kind,
                operation=operation,
            )
            return result
        except BaseException as error:
            primary_error = error
            if not isinstance(getattr(error, "mutation_receipt", None), MutationReceipt):
                _attach(
                    error,
                    _receipt(
                        mutation_policy,
                        target,
                        "rejected",
                        _error_status(error),
                        "uploaded",
                        resource_kind=resource_kind,
                        operation=operation,
                    ),
                )
            raise
        finally:
            _close_upload(client_input, result, primary_error)

    async def get(self, dataset_id: str, resource_id: str) -> NativeRecord:
        method, path, _, _ = wire.resource_request("GET", dataset_id, resource_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.RESOURCE_GET_OPERATION
        )
        return wire.parse_resource(payload)

    async def delete(
        self,
        dataset_id: str,
        resource_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, _ = wire.resource_request("DELETE", dataset_id, resource_id)
        except BaseException as error:
            _reject_route(
                error, resource_id, mutation_policy, "deleted", ResourceKind.RESOURCE, wire.RESOURCE_DELETE_OPERATION
            )
        return await _async_resource_mutation(
            resource_id,
            mutation_policy,
            "deleted",
            True,
            lambda: self._call(method, path, None, permissions, mutation_policy),
        )

    async def update(
        self,
        dataset_id: str,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.resource_request("PUT", dataset_id, resource_id, body=client_input.payload())
        except BaseException as error:
            _reject_route(
                error, resource_id, mutation_policy, "updated", ResourceKind.RESOURCE, wire.RESOURCE_UPDATE_OPERATION
            )
        return await _async_resource_mutation(
            resource_id,
            mutation_policy,
            "updated",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
        )

    async def list_community(self, params: Mapping[str, str | int] | None = None) -> UDataPageEnvelope:
        method, path, _, _ = wire.list_community_resources_request(params)
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.COMMUNITY_LIST_OPERATION
        )
        return wire.parse_resource_page(payload)

    async def get_community(self, resource_id: str) -> NativeRecord:
        method, path, _, _ = wire.resource_request("GET", "", resource_id, community=True)
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.COMMUNITY_GET_OPERATION
        )
        return wire.parse_resource(payload)

    async def resource_types(self) -> tuple[Mapping[str, str], ...]:
        method, path, _, _ = wire.resource_types_request()
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.RESOURCE_TYPES_OPERATION
        )
        return wire.parse_resource_types(payload)

    async def list_v2(self, dataset_id: str) -> UDataPageEnvelope:
        method, path, _, _ = wire.v2_resource_request(dataset_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.V2_RESOURCE_LIST_OPERATION
        )
        return wire.parse_resource_page(payload)

    async def get_v2(self, resource_id: str) -> NativeRecord:
        method, path, _, _ = wire.v2_resource_request("", resource_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=wire.V2_RESOURCE_GET_OPERATION,
        )
        return wire.parse_resource(payload.get("resource") if isinstance(payload, Mapping) else payload)

    async def upload_community(
        self,
        dataset_id: str,
        client_input: ResourceUploadInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        return await self.upload(dataset_id, client_input, permissions, mutation_policy, community=True)

    async def reupload_community(
        self,
        resource_id: str,
        client_input: ResourceUploadInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        result: Result | None = None
        primary_error: BaseException | None = None
        try:
            method, path, headers = wire.reupload_community_request(resource_id)
            result = await _async_resource_mutation(
                resource_id,
                mutation_policy,
                "uploaded",
                True,
                lambda: self._upload(method, path, headers, client_input, permissions, mutation_policy),
                operation=wire.UPLOAD_COMMUNITY_REPLACE_OPERATION,
            )
            return result
        except BaseException as error:
            primary_error = error
            if not isinstance(getattr(error, "mutation_receipt", None), MutationReceipt):
                _attach(
                    error,
                    _receipt(
                        mutation_policy,
                        resource_id,
                        "rejected",
                        _error_status(error),
                        "uploaded",
                        operation=wire.UPLOAD_COMMUNITY_REPLACE_OPERATION,
                    ),
                )
            raise
        finally:
            _close_upload(client_input, result, primary_error)

    async def create_community(
        self,
        dataset_id: str,
        client_input: ResourceCreateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.community_collection_request(
                "POST", client_input.payload() | {"dataset": dataset_id}
            )
        except BaseException as error:
            _reject_route(
                error, dataset_id, mutation_policy, "created", ResourceKind.DATASET, wire.COMMUNITY_CREATE_OPERATION
            )
        return await _async_resource_mutation(
            dataset_id,
            mutation_policy,
            "created",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            resource_kind=ResourceKind.DATASET,
            operation=wire.COMMUNITY_CREATE_OPERATION,
        )

    async def update_community(
        self,
        resource_id: str,
        client_input: ResourceUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.resource_request(
                "PUT", "", resource_id, community=True, body=client_input.payload()
            )
        except BaseException as error:
            _reject_route(
                error, resource_id, mutation_policy, "updated", ResourceKind.RESOURCE, wire.COMMUNITY_UPDATE_OPERATION
            )
        return await _async_resource_mutation(
            resource_id,
            mutation_policy,
            "updated",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            operation=wire.COMMUNITY_UPDATE_OPERATION,
        )

    async def delete_community(
        self,
        resource_id: str,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, _ = wire.resource_request("DELETE", "", resource_id, community=True)
        except BaseException as error:
            _reject_route(
                error, resource_id, mutation_policy, "deleted", ResourceKind.RESOURCE, wire.COMMUNITY_DELETE_OPERATION
            )
        return await _async_resource_mutation(
            resource_id,
            mutation_policy,
            "deleted",
            True,
            lambda: self._call(method, path, None, permissions, mutation_policy),
            operation=wire.COMMUNITY_DELETE_OPERATION,
        )

    async def get_dataset_v2(self, dataset_id: str) -> NativeRecord:
        method, path, _, _ = wire.v2_dataset_request(dataset_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=wire.V2_DATASET_GET_OPERATION,
        )
        return wire.parse_v2_dataset(payload)

    async def get_extras_v2(self, dataset_id: str, resource_id: str) -> Mapping[str, object]:
        method, path, _, _ = wire.v2_extras_request("GET", dataset_id, resource_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, owning_operation=wire.V2_EXTRAS_GET_OPERATION
        )
        return dataset_wire.parse_extras(payload, operation=wire.RESOURCE_OPERATION)

    async def update_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        values: Mapping[str, object],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.v2_extras_request("PUT", dataset_id, resource_id, dict(values))
        except BaseException as error:
            _reject_route(
                error,
                resource_id,
                mutation_policy,
                "extras_updated",
                ResourceKind.RESOURCE,
                wire.EXTRAS_UPDATE_OPERATION,
            )
        return await _async_resource_mutation(
            resource_id,
            mutation_policy,
            "extras_updated",
            False,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            operation=wire.EXTRAS_UPDATE_OPERATION,
        )

    async def delete_extras_v2(
        self,
        dataset_id: str,
        resource_id: str,
        keys: tuple[str, ...],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> Result:
        try:
            method, path, _, body = wire.v2_extras_request("DELETE", dataset_id, resource_id, list(keys))
        except BaseException as error:
            _reject_route(
                error,
                resource_id,
                mutation_policy,
                "extras_deleted",
                ResourceKind.RESOURCE,
                wire.EXTRAS_DELETE_OPERATION,
            )
        return await _async_resource_mutation(
            resource_id,
            mutation_policy,
            "extras_deleted",
            True,
            lambda: self._call(method, path, body, permissions, mutation_policy),
            operation=wire.EXTRAS_DELETE_OPERATION,
        )

    async def _call(
        self, method: str, path: str, body: object, permissions: Permissions, policy: Policy
    ) -> tuple[int, object, object]:
        capability_operation = _dispatch_operation(policy)
        resolved = _require_mutation_permission(
            await self._client._resolved_credential_async(), capability_operation, permissions
        )
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=capability_operation,
            json_body=body,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
        )

    async def _upload(
        self,
        method: str,
        path: str,
        headers: Mapping[str, str],
        client_input: ResourceUploadInput,
        permissions: Permissions,
        policy: Policy,
    ) -> tuple[int, object, object]:
        capability_operation = _dispatch_operation(policy)
        resolved = _require_mutation_permission(
            await self._client._resolved_credential_async(), capability_operation, permissions
        )
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            owning_operation=capability_operation,
            headers=headers,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
            files=(client_input.part(),),
        )
