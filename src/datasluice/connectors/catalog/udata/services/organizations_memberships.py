"""Dual-mode typed uData organization and membership services."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Never, cast

from datasluice.connectors.catalog.udata.models.organizations import (
    MembershipRequestInput,
    MembershipRequestQuery,
    OrganizationCreateInput,
    OrganizationDatasetQuery,
    OrganizationInvitationInput,
    OrganizationListQuery,
    OrganizationLogoInput,
    OrganizationMemberInput,
    OrganizationMutationResult,
    OrganizationRefusalInput,
    OrganizationSuggestQuery,
    OrganizationUpdateInput,
)
from datasluice.connectors.catalog.udata.wire import organizations as wire
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.domain.catalog.udata import SiteDocument
from datasluice.errors.catalog import NativeCatalogError, attach_catalog_metadata
from datasluice.runtime.mutation import build_mutation_receipt
from datasluice.runtime.transport.base import RuntimeResponse

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
type Dispatch = Callable[[], tuple[int, object, RuntimeResponse]]
type AsyncDispatch = Callable[[], Awaitable[tuple[int, object, RuntimeResponse]]]


def _request_with_body(
    request: tuple[str, str, Mapping[str, str], object], body: object
) -> tuple[str, str, Mapping[str, str], object]:
    return request[0], request[1], request[2], body


def _receipt(
    operation: str,
    target: object,
    policy: Policy,
    outcome: str,
    status: int,
    mutation: str,
    *,
    kind: ResourceKind = ResourceKind.ORGANIZATION,
) -> MutationReceipt:
    safe_target = _safe_target_value(target)
    return build_mutation_receipt(
        _operation_id(operation),
        CatalogId(platform=CatalogPlatform.UDATA, resource_kind=kind, value=safe_target),
        _receipt_policy(policy),
        outcome,
        {"mutation": mutation, "status_code": status, "target_valid": bool(target)},
    )


def _attach(error: BaseException, receipt: MutationReceipt) -> None:
    attach_catalog_metadata(error, {"receipt": receipt.to_dict()})
    if isinstance(getattr(error, "__dict__", None), dict):
        error.__dict__["mutation_receipt"] = receipt


def _close_logo(
    client_input: OrganizationLogoInput,
    result: OrganizationMutationResult | None,
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


def _raise_with_receipt(error: BaseException, receipt: MutationReceipt) -> Never:
    _attach(error, receipt)
    raise error


def _mutation_result(receipt: MutationReceipt, payload: object) -> OrganizationMutationResult:
    if isinstance(payload, Mapping) and isinstance(payload.get("id"), str) and isinstance(payload.get("name"), str):
        return OrganizationMutationResult(
            receipt=receipt, record=wire.parse_organization(payload, operation=receipt.operation)
        )
    if isinstance(payload, Mapping):
        return OrganizationMutationResult(receipt=receipt, value=payload)
    if isinstance(payload, list):
        return OrganizationMutationResult(
            receipt=receipt,
            records=tuple(wire.parse_records(payload, operation=receipt.operation)),
        )
    return OrganizationMutationResult(receipt=receipt)


def _mutation(
    operation: str,
    target: object,
    policy: Policy,
    mutation: str,
    dispatch: Dispatch,
    *,
    destructive: bool = False,
    kind: ResourceKind = ResourceKind.ORGANIZATION,
) -> OrganizationMutationResult:
    response: RuntimeResponse | None = None
    try:
        policy_target = target if isinstance(target, str) else _safe_target_value(target)
        _enforce_mutation_policy(operation, policy_target, policy, destructive=destructive)
        status, payload, response = dispatch()
        receipt = _receipt(operation, target, policy, "succeeded", status, mutation, kind=kind)
        return _mutation_result(receipt, payload)
    except BaseException as error:
        outcome = _mutation_outcome(error, response)
        _raise_with_receipt(
            error,
            _receipt(operation, target, policy, outcome, _error_status(error, response), mutation, kind=kind),
        )


async def _async_mutation(
    operation: str,
    target: object,
    policy: Policy,
    mutation: str,
    dispatch: AsyncDispatch,
    *,
    destructive: bool = False,
    kind: ResourceKind = ResourceKind.ORGANIZATION,
) -> OrganizationMutationResult:
    response: RuntimeResponse | None = None
    try:
        policy_target = target if isinstance(target, str) else _safe_target_value(target)
        _enforce_mutation_policy(operation, policy_target, policy, destructive=destructive)
        status, payload, response = await dispatch()
        receipt = _receipt(operation, target, policy, "succeeded", status, mutation, kind=kind)
        return _mutation_result(receipt, payload)
    except (Exception, asyncio.CancelledError) as error:
        outcome = _mutation_outcome(error, response)
        _raise_with_receipt(
            error,
            _receipt(operation, target, policy, outcome, _error_status(error, response), mutation, kind=kind),
        )


def _json_dispatch(
    client: SyncUDataClient,
    request: tuple[str, str, Mapping[str, str], object],
    operation: str,
    permissions: Permissions,
    policy: Policy,
    *,
    admin: bool = False,
) -> tuple[int, object, RuntimeResponse]:
    resolved = _require_mutation_permission(client._resolved_credential(), operation, permissions, admin=admin)
    method, path, headers, body = request
    return client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        headers=headers,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=policy.idempotency if policy else None,
    )


async def _json_dispatch_async(
    client: AsyncUDataClient,
    request: tuple[str, str, Mapping[str, str], object],
    operation: str,
    permissions: Permissions,
    policy: Policy,
    *,
    admin: bool = False,
) -> tuple[int, object, RuntimeResponse]:
    resolved = await client._resolved_credential_async()
    _require_mutation_permission(resolved, operation, permissions, admin=admin)
    method, path, headers, body = request
    return await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        headers=headers,
        json_body=body,
        permissions=permissions,
        credential=resolved,
        idempotency_policy=policy.idempotency if policy else None,
    )


def _secure_read(client: SyncUDataClient, operation: str, permissions: Permissions | None) -> UDataCredential:
    return _require_mutation_permission(client._resolved_credential(), operation, permissions)


async def _secure_read_async(
    client: AsyncUDataClient, operation: str, permissions: Permissions | None
) -> UDataCredential:
    resolved = await client._resolved_credential_async()
    return _require_mutation_permission(resolved, operation, permissions)


def _document(
    client: SyncUDataClient, request: tuple[str, str, Mapping[str, str], object], operation: str
) -> SiteDocument:
    method, path, headers, _ = request
    status, value, response = client._dataset_call(
        method=method,
        path=path,
        owning_operation=operation,
        headers=headers,
        raw_text=True,
        redirect_mode=True,
        max_response_bytes=client._root_export_max_bytes,
    )
    media_type = _header(response, "content-type") or "application/octet-stream"
    if status in {301, 302, 303, 307, 308}:
        location = _header(response, "location")
        if not location:
            raise NativeCatalogError(
                "The uData organization redirect omits its Location header.",
                operation=operation,
                platform=CatalogPlatform.UDATA.value,
                status_code=status,
            )
        return SiteDocument(
            endpoint=path,
            media_type=media_type,
            status_code=status,
            size_bytes=0,
            sha256=hashlib.sha256(b"").hexdigest(),
            location=location,
        )
    body = cast(bytes, value)
    return SiteDocument(
        endpoint=path,
        media_type=media_type,
        status_code=status,
        size_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
    )


async def _document_async(
    client: AsyncUDataClient, request: tuple[str, str, Mapping[str, str], object], operation: str
) -> SiteDocument:
    method, path, headers, _ = request
    status, value, response = await client._dataset_call_async(
        method=method,
        path=path,
        owning_operation=operation,
        headers=headers,
        raw_text=True,
        redirect_mode=True,
        max_response_bytes=client._root_export_max_bytes,
    )
    media_type = _header(response, "content-type") or "application/octet-stream"
    if status in {301, 302, 303, 307, 308}:
        location = _header(response, "location")
        if not location:
            raise NativeCatalogError(
                "The uData organization redirect omits its Location header.",
                operation=operation,
                platform=CatalogPlatform.UDATA.value,
                status_code=status,
            )
        return SiteDocument(
            endpoint=path,
            media_type=media_type,
            status_code=status,
            size_bytes=0,
            sha256=hashlib.sha256(b"").hexdigest(),
            location=location,
        )
    body = cast(bytes, value)
    return SiteDocument(
        endpoint=path,
        media_type=media_type,
        status_code=status,
        size_bytes=len(body),
        sha256=hashlib.sha256(body).hexdigest(),
    )


def _header(response: RuntimeResponse, name: str) -> str | None:
    wanted = name.lower()
    return next((value for key, value in response.headers.items() if key.lower() == wanted), None)


def _page_items(payload: object, *, operation: str) -> tuple[MappingRecord, ...]:
    return wire.parse_records(payload, operation=operation)


class SyncOrganizationsMembershipsService:
    """Typed synchronous methods for all organization and membership rows."""

    def __init__(self, client: SyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    def list_organizations(self, query: OrganizationListQuery | None = None):
        method, path, headers, _ = wire.list_organizations_request(query)
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, headers=headers, owning_operation=wire.LIST_ORGANIZATIONS_OPERATION
        )
        return wire.parse_organization_page(payload, operation=wire.LIST_ORGANIZATIONS_OPERATION)

    def create_organization(
        self, client_input: OrganizationCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.CREATE_ORGANIZATION_OPERATION,
            client_input.name,
            mutation_policy,
            "created",
            lambda: _json_dispatch(
                self._client,
                wire.create_organization_request(client_input),
                wire.CREATE_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def get_organization(self, organization_id: str) -> NativeRecord:
        request = wire.get_organization_request(organization_id)
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.GET_ORGANIZATION_OPERATION
        )
        return wire.parse_organization(payload, operation=wire.GET_ORGANIZATION_OPERATION)

    def update_organization(
        self,
        organization_id: str,
        client_input: OrganizationUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.UPDATE_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "updated",
            lambda: _json_dispatch(
                self._client,
                wire.update_organization_request(organization_id, client_input),
                wire.UPDATE_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def delete_organization(
        self, organization_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.DELETE_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "deleted",
            lambda: _json_dispatch(
                self._client,
                wire.delete_organization_request(organization_id),
                wire.DELETE_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
        )

    def organization_datasets_csv(self, organization_id: str) -> SiteDocument:
        return _document(
            self._client,
            wire.organization_export_request(organization_id, "datasets"),
            wire.ORGANIZATION_DATASETS_CSV_OPERATION,
        )

    def organization_dataservices_csv(self, organization_id: str) -> SiteDocument:
        return _document(
            self._client,
            wire.organization_export_request(organization_id, "dataservices"),
            wire.ORGANIZATION_DATASERVICES_CSV_OPERATION,
        )

    def organization_discussions_csv(self, organization_id: str) -> SiteDocument:
        return _document(
            self._client,
            wire.organization_export_request(organization_id, "discussions"),
            wire.ORGANIZATION_DISCUSSIONS_CSV_OPERATION,
        )

    def organization_datasets_resources_csv(self, organization_id: str) -> SiteDocument:
        return _document(
            self._client,
            wire.organization_export_request(organization_id, "datasets-resources"),
            wire.ORGANIZATION_DATASETS_RESOURCES_CSV_OPERATION,
        )

    def rdf_organization(self, organization_id: str) -> SiteDocument:
        return _document(self._client, wire.rdf_organization_request(organization_id), wire.RDF_ORGANIZATION_OPERATION)

    def rdf_organization_format(self, organization_id: str, fmt: str) -> SiteDocument:
        return _document(
            self._client,
            wire.rdf_organization_format_request(organization_id, fmt),
            wire.RDF_ORGANIZATION_FORMAT_OPERATION,
        )

    def available_organization_badges(self) -> tuple[MappingRecord, ...]:
        method, path, headers, _ = wire.available_organization_badges_request()
        _, payload, _ = self._client._dataset_call(
            method=method, path=path, headers=headers, owning_operation=wire.AVAILABLE_ORGANIZATION_BADGES_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.AVAILABLE_ORGANIZATION_BADGES_OPERATION, kind=ResourceKind("badge")
        )

    def add_organization_badge(
        self, organization_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.ADD_ORGANIZATION_BADGE_OPERATION,
            f"{organization_id}/{badge_kind}",
            mutation_policy,
            "badge_added",
            lambda: _json_dispatch(
                self._client,
                wire.organization_badge_request(organization_id, badge_kind),
                wire.ADD_ORGANIZATION_BADGE_OPERATION,
                permissions,
                mutation_policy,
                admin=True,
            ),
        )

    def delete_organization_badge(
        self, organization_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.DELETE_ORGANIZATION_BADGE_OPERATION,
            f"{organization_id}/{badge_kind}",
            mutation_policy,
            "badge_deleted",
            lambda: _json_dispatch(
                self._client,
                wire.organization_badge_request(organization_id, badge_kind, delete=True),
                wire.DELETE_ORGANIZATION_BADGE_OPERATION,
                permissions,
                mutation_policy,
                admin=True,
            ),
            destructive=True,
        )

    def get_organization_contact_point(
        self, organization_id: str, query: OrganizationListQuery | None = None
    ) -> tuple[MappingRecord, ...]:
        request = wire.organization_contacts_request(organization_id, query)
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.GET_ORGANIZATION_CONTACT_POINT_OPERATION
        )
        return _page_items(payload, operation=wire.GET_ORGANIZATION_CONTACT_POINT_OPERATION)

    def suggest_org_contact_points(
        self, organization_id: str, query: OrganizationSuggestQuery
    ) -> tuple[MappingRecord, ...]:
        request = wire.organization_contacts_suggest_request(organization_id, query)
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.SUGGEST_ORG_CONTACT_POINTS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.SUGGEST_ORG_CONTACT_POINTS_OPERATION, kind=ResourceKind("contact-point")
        )

    def list_membership_requests(
        self, organization_id: str, permissions: Permissions, query: MembershipRequestQuery | None = None
    ) -> tuple[MappingRecord, ...]:
        credential = _secure_read(self._client, wire.LIST_MEMBERSHIP_REQUESTS_OPERATION, permissions)
        request = wire.membership_requests_request(organization_id, query)
        _, payload, _ = self._client._dataset_call(
            method=request[0],
            path=request[1],
            owning_operation=wire.LIST_MEMBERSHIP_REQUESTS_OPERATION,
            credential=credential,
            permissions=permissions,
        )
        return _page_items(payload, operation=wire.LIST_MEMBERSHIP_REQUESTS_OPERATION)

    def membership_request(
        self,
        organization_id: str,
        client_input: MembershipRequestInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.MEMBERSHIP_REQUEST_OPERATION,
            organization_id,
            mutation_policy,
            "membership_requested",
            lambda: _json_dispatch(
                self._client,
                wire.membership_request_request(organization_id, client_input),
                wire.MEMBERSHIP_REQUEST_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def accept_membership(
        self, organization_id: str, request_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.ACCEPT_MEMBERSHIP_OPERATION,
            f"{organization_id}/{request_id}",
            mutation_policy,
            "membership_accepted",
            lambda: _json_dispatch(
                self._client,
                wire.membership_action_request(organization_id, request_id, "accept"),
                wire.ACCEPT_MEMBERSHIP_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def refuse_membership(
        self,
        organization_id: str,
        request_id: str,
        client_input: OrganizationRefusalInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.REFUSE_MEMBERSHIP_OPERATION,
            f"{organization_id}/{request_id}",
            mutation_policy,
            "membership_refused",
            lambda: _json_dispatch(
                self._client,
                _request_with_body(
                    wire.membership_action_request(organization_id, request_id, "refuse"), client_input.payload()
                ),
                wire.REFUSE_MEMBERSHIP_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def cancel_membership(
        self, organization_id: str, request_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.CANCEL_MEMBERSHIP_OPERATION,
            f"{organization_id}/{request_id}",
            mutation_policy,
            "membership_canceled",
            lambda: _json_dispatch(
                self._client,
                wire.membership_action_request(organization_id, request_id, "cancel"),
                wire.CANCEL_MEMBERSHIP_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def invite_organization_member(
        self,
        organization_id: str,
        client_input: OrganizationInvitationInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.INVITE_ORGANIZATION_MEMBER_OPERATION,
            organization_id,
            mutation_policy,
            "member_invited",
            lambda: _json_dispatch(
                self._client,
                wire.invite_member_request(organization_id, client_input),
                wire.INVITE_ORGANIZATION_MEMBER_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def update_organization_member(
        self,
        organization_id: str,
        user_id: str,
        client_input: OrganizationMemberInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.UPDATE_ORGANIZATION_MEMBER_OPERATION,
            f"{organization_id}/{user_id}",
            mutation_policy,
            "member_updated",
            lambda: _json_dispatch(
                self._client,
                wire.member_request(organization_id, user_id, method="PUT", body=client_input.payload()),
                wire.UPDATE_ORGANIZATION_MEMBER_OPERATION,
                permissions,
                mutation_policy,
            ),
            kind=ResourceKind("member"),
        )

    def delete_organization_member(
        self, organization_id: str, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.DELETE_ORGANIZATION_MEMBER_OPERATION,
            f"{organization_id}/{user_id}",
            mutation_policy,
            "member_deleted",
            lambda: _json_dispatch(
                self._client,
                wire.member_request(organization_id, user_id, method="DELETE"),
                wire.DELETE_ORGANIZATION_MEMBER_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
            kind=ResourceKind("member"),
        )

    def list_organization_assignments(
        self, organization_id: str, permissions: Permissions
    ) -> tuple[MappingRecord, ...]:
        credential = _secure_read(self._client, wire.LIST_ORGANIZATION_ASSIGNMENTS_OPERATION, permissions)
        request = wire.assignments_request(organization_id)
        _, payload, _ = self._client._dataset_call(
            method=request[0],
            path=request[1],
            owning_operation=wire.LIST_ORGANIZATION_ASSIGNMENTS_OPERATION,
            credential=credential,
            permissions=permissions,
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_ASSIGNMENTS_OPERATION, kind=ResourceKind("assignment")
        )

    def sync_member_assignments(
        self,
        organization_id: str,
        user_id: str,
        assignments: list[Mapping[str, object]],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.SYNC_MEMBER_ASSIGNMENTS_OPERATION,
            f"{organization_id}/{user_id}",
            mutation_policy,
            "assignments_synced",
            lambda: _json_dispatch(
                self._client,
                wire.member_assignments_request(organization_id, user_id, assignments),
                wire.SYNC_MEMBER_ASSIGNMENTS_OPERATION,
                permissions,
                mutation_policy,
            ),
            kind=ResourceKind("assignment"),
        )

    def suggest_organizations(self, query: OrganizationSuggestQuery) -> tuple[MappingRecord, ...]:
        request = wire.suggest_organizations_request(query)
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.SUGGEST_ORGANIZATIONS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.SUGGEST_ORGANIZATIONS_OPERATION, kind=ResourceKind("organization-suggestion")
        )

    def _logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        policy: Policy,
        *,
        resize: bool,
    ) -> OrganizationMutationResult:
        operation = wire.RESIZE_ORGANIZATION_LOGO_OPERATION if resize else wire.ORGANIZATION_LOGO_OPERATION
        result: OrganizationMutationResult | None = None
        primary_error: BaseException | None = None
        try:
            result = _mutation(
                operation,
                organization_id,
                policy,
                "logo_resized" if resize else "logo_uploaded",
                lambda: self._upload_dispatch(organization_id, client_input, permissions, policy, resize=resize),
                kind=ResourceKind("organization"),
            )
            return result
        except BaseException as error:
            primary_error = error
            raise
        finally:
            _close_logo(client_input, result, primary_error)

    def organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return self._logo(organization_id, client_input, permissions, mutation_policy, resize=False)

    def resize_organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return self._logo(organization_id, client_input, permissions, mutation_policy, resize=True)

    def _upload_dispatch(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        policy: Policy,
        *,
        resize: bool,
    ) -> tuple[int, object, RuntimeResponse]:
        operation = wire.RESIZE_ORGANIZATION_LOGO_OPERATION if resize else wire.ORGANIZATION_LOGO_OPERATION
        resolved = _require_mutation_permission(self._client._resolved_credential(), operation, permissions)
        method, path, headers, _ = wire.organization_logo_request(organization_id, resize=resize)
        return self._client._dataset_call(
            method=method,
            path=path,
            headers=headers,
            owning_operation=operation,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
            files=(client_input.part(),),
        )

    def list_organization_datasets(self, organization_id: str, query: OrganizationDatasetQuery | None = None):
        request = wire.organization_owned_request(organization_id, "datasets", query)
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_DATASETS_OPERATION
        )
        return wire.parse_page(payload, operation=wire.LIST_ORGANIZATION_DATASETS_OPERATION, kind=ResourceKind.DATASET)

    def list_organization_reuses(self, organization_id: str):
        request = wire.organization_owned_request(organization_id, "reuses")
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_REUSES_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_REUSES_OPERATION, kind=ResourceKind("reuse")
        )

    def list_organization_discussions(self, organization_id: str):
        request = wire.organization_owned_request(organization_id, "discussions")
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_DISCUSSIONS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_DISCUSSIONS_OPERATION, kind=ResourceKind("discussion")
        )

    def org_roles(self) -> tuple[MappingRecord, ...]:
        request = wire.organization_roles_request()
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.ORG_ROLES_OPERATION
        )
        return wire.parse_roles(payload)

    def search_organizations(self, query: OrganizationListQuery | None = None):
        request = wire.search_organizations_request(query)
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.SEARCH_ORGANIZATIONS_OPERATION
        )
        return wire.parse_organization_search(payload)

    def get_organization_extras(self, organization_id: str) -> Mapping[str, object]:
        request = wire.organization_extras_request(organization_id, method="GET")
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.GET_ORGANIZATION_EXTRAS_OPERATION
        )
        return wire.parse_extras(payload)

    def update_organization_extras(
        self,
        organization_id: str,
        values: Mapping[str, object],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.UPDATE_ORGANIZATION_EXTRAS_OPERATION,
            organization_id,
            mutation_policy,
            "extras_updated",
            lambda: _json_dispatch(
                self._client,
                wire.organization_extras_request(organization_id, method="PUT", body=dict(values)),
                wire.UPDATE_ORGANIZATION_EXTRAS_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def delete_organization_extras(
        self, organization_id: str, keys: tuple[str, ...], permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.DELETE_ORGANIZATION_EXTRAS_OPERATION,
            organization_id,
            mutation_policy,
            "extras_deleted",
            lambda: _json_dispatch(
                self._client,
                wire.organization_extras_request(organization_id, method="DELETE", body=list(keys)),
                wire.DELETE_ORGANIZATION_EXTRAS_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
        )

    def list_organization_followers(self, organization_id: str):
        request = wire.followers_request(organization_id, method="GET")
        _, payload, _ = self._client._dataset_call(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_FOLLOWERS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_FOLLOWERS_OPERATION, kind=ResourceKind("user")
        )

    def follow_organization(
        self, organization_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.FOLLOW_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "followed",
            lambda: _json_dispatch(
                self._client,
                wire.followers_request(organization_id, method="POST"),
                wire.FOLLOW_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    def unfollow_organization(
        self, organization_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return _mutation(
            wire.UNFOLLOW_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "unfollowed",
            lambda: _json_dispatch(
                self._client,
                wire.followers_request(organization_id, method="DELETE"),
                wire.UNFOLLOW_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
        )


class AsyncOrganizationsMembershipsService:
    """Typed asynchronous twin of the organization and membership service."""

    def __init__(self, client: AsyncUDataClient) -> None:
        self._client = client

    @property
    def error_type(self) -> type[NativeCatalogError]:
        return NativeCatalogError

    async def list_organizations(self, query: OrganizationListQuery | None = None):
        method, path, headers, _ = wire.list_organizations_request(query)
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, headers=headers, owning_operation=wire.LIST_ORGANIZATIONS_OPERATION
        )
        return wire.parse_organization_page(payload, operation=wire.LIST_ORGANIZATIONS_OPERATION)

    async def create_organization(
        self, client_input: OrganizationCreateInput, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.CREATE_ORGANIZATION_OPERATION,
            client_input.name,
            mutation_policy,
            "created",
            lambda: _json_dispatch_async(
                self._client,
                wire.create_organization_request(client_input),
                wire.CREATE_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def get_organization(self, organization_id: str) -> NativeRecord:
        request = wire.get_organization_request(organization_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.GET_ORGANIZATION_OPERATION
        )
        return wire.parse_organization(payload, operation=wire.GET_ORGANIZATION_OPERATION)

    async def update_organization(
        self,
        organization_id: str,
        client_input: OrganizationUpdateInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.UPDATE_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "updated",
            lambda: _json_dispatch_async(
                self._client,
                wire.update_organization_request(organization_id, client_input),
                wire.UPDATE_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def delete_organization(
        self, organization_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.DELETE_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "deleted",
            lambda: _json_dispatch_async(
                self._client,
                wire.delete_organization_request(organization_id),
                wire.DELETE_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
        )

    async def organization_datasets_csv(self, organization_id: str) -> SiteDocument:
        return await _document_async(
            self._client,
            wire.organization_export_request(organization_id, "datasets"),
            wire.ORGANIZATION_DATASETS_CSV_OPERATION,
        )

    async def organization_dataservices_csv(self, organization_id: str) -> SiteDocument:
        return await _document_async(
            self._client,
            wire.organization_export_request(organization_id, "dataservices"),
            wire.ORGANIZATION_DATASERVICES_CSV_OPERATION,
        )

    async def organization_discussions_csv(self, organization_id: str) -> SiteDocument:
        return await _document_async(
            self._client,
            wire.organization_export_request(organization_id, "discussions"),
            wire.ORGANIZATION_DISCUSSIONS_CSV_OPERATION,
        )

    async def organization_datasets_resources_csv(self, organization_id: str) -> SiteDocument:
        return await _document_async(
            self._client,
            wire.organization_export_request(organization_id, "datasets-resources"),
            wire.ORGANIZATION_DATASETS_RESOURCES_CSV_OPERATION,
        )

    async def rdf_organization(self, organization_id: str) -> SiteDocument:
        return await _document_async(
            self._client, wire.rdf_organization_request(organization_id), wire.RDF_ORGANIZATION_OPERATION
        )

    async def rdf_organization_format(self, organization_id: str, fmt: str) -> SiteDocument:
        return await _document_async(
            self._client,
            wire.rdf_organization_format_request(organization_id, fmt),
            wire.RDF_ORGANIZATION_FORMAT_OPERATION,
        )

    async def available_organization_badges(self) -> tuple[MappingRecord, ...]:
        method, path, headers, _ = wire.available_organization_badges_request()
        _, payload, _ = await self._client._dataset_call_async(
            method=method, path=path, headers=headers, owning_operation=wire.AVAILABLE_ORGANIZATION_BADGES_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.AVAILABLE_ORGANIZATION_BADGES_OPERATION, kind=ResourceKind("badge")
        )

    async def add_organization_badge(
        self, organization_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.ADD_ORGANIZATION_BADGE_OPERATION,
            f"{organization_id}/{badge_kind}",
            mutation_policy,
            "badge_added",
            lambda: _json_dispatch_async(
                self._client,
                wire.organization_badge_request(organization_id, badge_kind),
                wire.ADD_ORGANIZATION_BADGE_OPERATION,
                permissions,
                mutation_policy,
                admin=True,
            ),
        )

    async def delete_organization_badge(
        self, organization_id: str, badge_kind: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.DELETE_ORGANIZATION_BADGE_OPERATION,
            f"{organization_id}/{badge_kind}",
            mutation_policy,
            "badge_deleted",
            lambda: _json_dispatch_async(
                self._client,
                wire.organization_badge_request(organization_id, badge_kind, delete=True),
                wire.DELETE_ORGANIZATION_BADGE_OPERATION,
                permissions,
                mutation_policy,
                admin=True,
            ),
            destructive=True,
        )

    async def get_organization_contact_point(
        self, organization_id: str, query: OrganizationListQuery | None = None
    ) -> tuple[MappingRecord, ...]:
        request = wire.organization_contacts_request(organization_id, query)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.GET_ORGANIZATION_CONTACT_POINT_OPERATION
        )
        return _page_items(payload, operation=wire.GET_ORGANIZATION_CONTACT_POINT_OPERATION)

    async def suggest_org_contact_points(
        self, organization_id: str, query: OrganizationSuggestQuery
    ) -> tuple[MappingRecord, ...]:
        request = wire.organization_contacts_suggest_request(organization_id, query)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.SUGGEST_ORG_CONTACT_POINTS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.SUGGEST_ORG_CONTACT_POINTS_OPERATION, kind=ResourceKind("contact-point")
        )

    async def list_membership_requests(
        self, organization_id: str, permissions: Permissions, query: MembershipRequestQuery | None = None
    ) -> tuple[MappingRecord, ...]:
        credential = await _secure_read_async(self._client, wire.LIST_MEMBERSHIP_REQUESTS_OPERATION, permissions)
        request = wire.membership_requests_request(organization_id, query)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0],
            path=request[1],
            owning_operation=wire.LIST_MEMBERSHIP_REQUESTS_OPERATION,
            credential=credential,
            permissions=permissions,
        )
        return _page_items(payload, operation=wire.LIST_MEMBERSHIP_REQUESTS_OPERATION)

    async def membership_request(
        self,
        organization_id: str,
        client_input: MembershipRequestInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.MEMBERSHIP_REQUEST_OPERATION,
            organization_id,
            mutation_policy,
            "membership_requested",
            lambda: _json_dispatch_async(
                self._client,
                wire.membership_request_request(organization_id, client_input),
                wire.MEMBERSHIP_REQUEST_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def accept_membership(
        self, organization_id: str, request_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.ACCEPT_MEMBERSHIP_OPERATION,
            f"{organization_id}/{request_id}",
            mutation_policy,
            "membership_accepted",
            lambda: _json_dispatch_async(
                self._client,
                wire.membership_action_request(organization_id, request_id, "accept"),
                wire.ACCEPT_MEMBERSHIP_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def refuse_membership(
        self,
        organization_id: str,
        request_id: str,
        client_input: OrganizationRefusalInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.REFUSE_MEMBERSHIP_OPERATION,
            f"{organization_id}/{request_id}",
            mutation_policy,
            "membership_refused",
            lambda: _json_dispatch_async(
                self._client,
                _request_with_body(
                    wire.membership_action_request(organization_id, request_id, "refuse"), client_input.payload()
                ),
                wire.REFUSE_MEMBERSHIP_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def cancel_membership(
        self, organization_id: str, request_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.CANCEL_MEMBERSHIP_OPERATION,
            f"{organization_id}/{request_id}",
            mutation_policy,
            "membership_canceled",
            lambda: _json_dispatch_async(
                self._client,
                wire.membership_action_request(organization_id, request_id, "cancel"),
                wire.CANCEL_MEMBERSHIP_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def invite_organization_member(
        self,
        organization_id: str,
        client_input: OrganizationInvitationInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.INVITE_ORGANIZATION_MEMBER_OPERATION,
            organization_id,
            mutation_policy,
            "member_invited",
            lambda: _json_dispatch_async(
                self._client,
                wire.invite_member_request(organization_id, client_input),
                wire.INVITE_ORGANIZATION_MEMBER_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def update_organization_member(
        self,
        organization_id: str,
        user_id: str,
        client_input: OrganizationMemberInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.UPDATE_ORGANIZATION_MEMBER_OPERATION,
            f"{organization_id}/{user_id}",
            mutation_policy,
            "member_updated",
            lambda: _json_dispatch_async(
                self._client,
                wire.member_request(organization_id, user_id, method="PUT", body=client_input.payload()),
                wire.UPDATE_ORGANIZATION_MEMBER_OPERATION,
                permissions,
                mutation_policy,
            ),
            kind=ResourceKind("member"),
        )

    async def delete_organization_member(
        self, organization_id: str, user_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.DELETE_ORGANIZATION_MEMBER_OPERATION,
            f"{organization_id}/{user_id}",
            mutation_policy,
            "member_deleted",
            lambda: _json_dispatch_async(
                self._client,
                wire.member_request(organization_id, user_id, method="DELETE"),
                wire.DELETE_ORGANIZATION_MEMBER_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
            kind=ResourceKind("member"),
        )

    async def list_organization_assignments(
        self, organization_id: str, permissions: Permissions
    ) -> tuple[MappingRecord, ...]:
        credential = await _secure_read_async(self._client, wire.LIST_ORGANIZATION_ASSIGNMENTS_OPERATION, permissions)
        request = wire.assignments_request(organization_id)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0],
            path=request[1],
            owning_operation=wire.LIST_ORGANIZATION_ASSIGNMENTS_OPERATION,
            credential=credential,
            permissions=permissions,
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_ASSIGNMENTS_OPERATION, kind=ResourceKind("assignment")
        )

    async def sync_member_assignments(
        self,
        organization_id: str,
        user_id: str,
        assignments: list[Mapping[str, object]],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.SYNC_MEMBER_ASSIGNMENTS_OPERATION,
            f"{organization_id}/{user_id}",
            mutation_policy,
            "assignments_synced",
            lambda: _json_dispatch_async(
                self._client,
                wire.member_assignments_request(organization_id, user_id, assignments),
                wire.SYNC_MEMBER_ASSIGNMENTS_OPERATION,
                permissions,
                mutation_policy,
            ),
            kind=ResourceKind("assignment"),
        )

    async def suggest_organizations(self, query: OrganizationSuggestQuery) -> tuple[MappingRecord, ...]:
        request = wire.suggest_organizations_request(query)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.SUGGEST_ORGANIZATIONS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.SUGGEST_ORGANIZATIONS_OPERATION, kind=ResourceKind("organization-suggestion")
        )

    async def _logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        policy: Policy,
        *,
        resize: bool,
    ) -> OrganizationMutationResult:
        operation = wire.RESIZE_ORGANIZATION_LOGO_OPERATION if resize else wire.ORGANIZATION_LOGO_OPERATION
        result: OrganizationMutationResult | None = None
        primary_error: BaseException | None = None
        try:
            result = await _async_mutation(
                operation,
                organization_id,
                policy,
                "logo_resized" if resize else "logo_uploaded",
                lambda: self._upload_dispatch(organization_id, client_input, permissions, policy, resize=resize),
                kind=ResourceKind("organization"),
            )
            return result
        except BaseException as error:
            primary_error = error
            raise
        finally:
            _close_logo(client_input, result, primary_error)

    async def organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await self._logo(organization_id, client_input, permissions, mutation_policy, resize=False)

    async def resize_organization_logo(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await self._logo(organization_id, client_input, permissions, mutation_policy, resize=True)

    async def _upload_dispatch(
        self,
        organization_id: str,
        client_input: OrganizationLogoInput,
        permissions: Permissions,
        policy: Policy,
        *,
        resize: bool,
    ) -> tuple[int, object, RuntimeResponse]:
        operation = wire.RESIZE_ORGANIZATION_LOGO_OPERATION if resize else wire.ORGANIZATION_LOGO_OPERATION
        resolved = await _secure_read_async(self._client, operation, permissions)
        method, path, headers, _ = wire.organization_logo_request(organization_id, resize=resize)
        return await self._client._dataset_call_async(
            method=method,
            path=path,
            headers=headers,
            owning_operation=operation,
            permissions=permissions,
            credential=resolved,
            idempotency_policy=policy.idempotency if policy else None,
            files=(client_input.part(),),
        )

    async def list_organization_datasets(self, organization_id: str, query: OrganizationDatasetQuery | None = None):
        request = wire.organization_owned_request(organization_id, "datasets", query)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_DATASETS_OPERATION
        )
        return wire.parse_page(payload, operation=wire.LIST_ORGANIZATION_DATASETS_OPERATION, kind=ResourceKind.DATASET)

    async def list_organization_reuses(self, organization_id: str):
        request = wire.organization_owned_request(organization_id, "reuses")
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_REUSES_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_REUSES_OPERATION, kind=ResourceKind("reuse")
        )

    async def list_organization_discussions(self, organization_id: str):
        request = wire.organization_owned_request(organization_id, "discussions")
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_DISCUSSIONS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_DISCUSSIONS_OPERATION, kind=ResourceKind("discussion")
        )

    async def org_roles(self) -> tuple[MappingRecord, ...]:
        request = wire.organization_roles_request()
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.ORG_ROLES_OPERATION
        )
        return wire.parse_roles(payload)

    async def search_organizations(self, query: OrganizationListQuery | None = None):
        request = wire.search_organizations_request(query)
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.SEARCH_ORGANIZATIONS_OPERATION
        )
        return wire.parse_organization_search(payload)

    async def get_organization_extras(self, organization_id: str) -> Mapping[str, object]:
        request = wire.organization_extras_request(organization_id, method="GET")
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.GET_ORGANIZATION_EXTRAS_OPERATION
        )
        return wire.parse_extras(payload)

    async def update_organization_extras(
        self,
        organization_id: str,
        values: Mapping[str, object],
        permissions: Permissions,
        mutation_policy: Policy = None,
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.UPDATE_ORGANIZATION_EXTRAS_OPERATION,
            organization_id,
            mutation_policy,
            "extras_updated",
            lambda: _json_dispatch_async(
                self._client,
                wire.organization_extras_request(organization_id, method="PUT", body=dict(values)),
                wire.UPDATE_ORGANIZATION_EXTRAS_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def delete_organization_extras(
        self, organization_id: str, keys: tuple[str, ...], permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.DELETE_ORGANIZATION_EXTRAS_OPERATION,
            organization_id,
            mutation_policy,
            "extras_deleted",
            lambda: _json_dispatch_async(
                self._client,
                wire.organization_extras_request(organization_id, method="DELETE", body=list(keys)),
                wire.DELETE_ORGANIZATION_EXTRAS_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
        )

    async def list_organization_followers(self, organization_id: str):
        request = wire.followers_request(organization_id, method="GET")
        _, payload, _ = await self._client._dataset_call_async(
            method=request[0], path=request[1], owning_operation=wire.LIST_ORGANIZATION_FOLLOWERS_OPERATION
        )
        return wire.parse_records(
            payload, operation=wire.LIST_ORGANIZATION_FOLLOWERS_OPERATION, kind=ResourceKind("user")
        )

    async def follow_organization(
        self, organization_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.FOLLOW_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "followed",
            lambda: _json_dispatch_async(
                self._client,
                wire.followers_request(organization_id, method="POST"),
                wire.FOLLOW_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
        )

    async def unfollow_organization(
        self, organization_id: str, permissions: Permissions, mutation_policy: Policy = None
    ) -> OrganizationMutationResult:
        return await _async_mutation(
            wire.UNFOLLOW_ORGANIZATION_OPERATION,
            organization_id,
            mutation_policy,
            "unfollowed",
            lambda: _json_dispatch_async(
                self._client,
                wire.followers_request(organization_id, method="DELETE"),
                wire.UNFOLLOW_ORGANIZATION_OPERATION,
                permissions,
                mutation_policy,
            ),
            destructive=True,
        )


__all__ = ["AsyncOrganizationsMembershipsService", "SyncOrganizationsMembershipsService"]
