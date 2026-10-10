"""Exact wire, capability, receipt, and settlement coverage for the uData transfer family."""

from __future__ import annotations

import asyncio
import json
from typing import cast

import pytest

from datasluice.connectors.catalog.udata.models.transfers import (
    TransferListQuery,
    TransferMutationResult,
    TransferRequestInput,
    TransferResponseInput,
    linked_identifier,
    segment,
)
from datasluice.connectors.catalog.udata.services import transfers as services
from datasluice.connectors.catalog.udata.services.transfers import (
    AsyncTransfersService,
    SyncTransfersService,
)
from datasluice.connectors.catalog.udata.wire import transfers as wire
from datasluice.contracts.catalog.native.udata import (
    AsyncUDataTransfersService,
    SyncUDataTransfersService,
)
from datasluice.domain.catalog.auth import EffectivePermissions, UDataCredential
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.models import MappingRecord
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, ConfirmationPolicy, IdempotencyPolicy, MutationPolicy
from datasluice.errors.catalog import (
    CatalogError,
    CatalogUnavailableError,
    CatalogValidationError,
    ForbiddenError,
    NativeCatalogError,
    UnauthenticatedError,
)
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse, TransportError
from tests.helpers.udata_test_support import (
    UDATA_CREDENTIAL,
    UDATA_ORIGIN,
    UDATA_PERMISSIONS,
    AsyncRouteRouter,
    RouteTable,
    SyncRouteRouter,
    async_client,
    async_route_table,
    sync_client,
    sync_route_table,
    thawed,
    with_site_route,
)

ORIGIN = UDATA_ORIGIN
PERMISSIONS = UDATA_PERMISSIONS
SUBJECT = TransferRequestInput(
    subject_id="dataset-1",
    subject_class="Dataset",
    recipient_id="organization-1",
    recipient_class="Organization",
)
COMPOSITE_TARGET = "dataset-1:organization-1"
OTHER_CREDENTIAL = UDataCredential(api_key="other-key")
ANONYMOUS_PERMISSIONS = EffectivePermissions.for_credential(
    UDATA_CREDENTIAL, platform=CatalogPlatform.UDATA, authenticated=False
)
OTHER_SCOPE_PERMISSIONS = EffectivePermissions.for_credential(OTHER_CREDENTIAL, platform=CatalogPlatform.UDATA)
DESTRUCTIVE_REQUEST_POLICY = MutationPolicy(
    destructive=True,
    confirmation=ConfirmationPolicy(confirmed=True, operation=wire.RESPOND_TO_TRANSFER_OPERATION, target="transfer-1"),
    concurrency=ConcurrencyPolicy(overwrite=True),
)
RETRYING_ACCEPT_POLICY = MutationPolicy(
    destructive=True,
    confirmation=ConfirmationPolicy(confirmed=True, operation=wire.RESPOND_TO_TRANSFER_OPERATION, target="transfer-1"),
    concurrency=ConcurrencyPolicy(overwrite=True),
    idempotency=IdempotencyPolicy(safe=True, explicit_retry_opt_in=True),
)


def _policy(operation: str, target: str, *, destructive: bool = False) -> MutationPolicy:
    return MutationPolicy(
        destructive=destructive,
        confirmation=ConfirmationPolicy(confirmed=True, operation=operation, target=target),
        concurrency=ConcurrencyPolicy(overwrite=True),
    )


def _receipt_from(error: BaseException) -> MutationReceipt:
    receipt = getattr(error, "mutation_receipt", None)
    assert isinstance(receipt, MutationReceipt)
    return receipt


def _transfer(transfer_id: str = "transfer-1", status: str = "pending") -> dict[str, object]:
    return {
        "id": transfer_id,
        "status": status,
        "subject": {"id": "dataset-1", "class": "Dataset"},
        "recipient": {"id": "organization-1", "class": "Organization"},
        "comment": "please",
    }


def _transfer_routes() -> RouteTable:
    return with_site_route(
        {
            ("GET", f"{ORIGIN}/api/1/transfer/?subject=dataset-1&subject_type=Dataset"): (200, [_transfer()]),
            ("GET", f"{ORIGIN}/api/1/transfer/?recipient=organization-1&status=pending"): (
                200,
                [_transfer(status="accepted")],
            ),
            ("GET", f"{ORIGIN}/api/1/transfer/transfer-1/"): (200, _transfer()),
            ("POST", f"{ORIGIN}/api/1/transfer/"): (201, _transfer()),
            ("POST", f"{ORIGIN}/api/1/transfer/transfer-1/"): (200, _transfer(status="accepted")),
        }
    )


def _transfer_requests(router: SyncRouteRouter | AsyncRouteRouter) -> list[RuntimeRequest]:
    return [request for request in router.requests if "/api/1/transfer/" in request.url]


class _SyncDroppingTransferRouter(SyncRouteRouter):
    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.method == "POST" and "/api/1/transfer/" in request.url:
            self.requests.append(request)
            raise TransportError("connection dropped after the ownership write")
        return super().send(request)


class _AsyncDroppingTransferRouter(AsyncRouteRouter):
    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        if request.method == "POST" and "/api/1/transfer/" in request.url:
            self.requests.append(request)
            raise TransportError("connection dropped after the ownership write")
        return await super().send(request)


def test_transfer_contract_exposes_every_assigned_method_in_both_modes() -> None:
    """A transfer that moves dataset ownership is typed identically in both dispatch loops."""
    sync_names = {
        name
        for name in dir(SyncTransfersService)
        if not name.startswith("_") and callable(getattr(SyncTransfersService, name))
    }
    async_names = {
        name
        for name in dir(AsyncTransfersService)
        if not name.startswith("_") and callable(getattr(AsyncTransfersService, name))
    }
    assert sync_names == async_names == {"list_transfers", "get_transfer", "request_transfer", "respond_to_transfer"}
    with sync_client(sync_route_table(with_site_route({})), None) as client:
        assert isinstance(client.transfers, SyncUDataTransfersService)
        assert isinstance(client.transfers, SyncTransfersService)

    async def run() -> None:
        async with async_client(async_route_table(with_site_route({})), None) as client:
            assert isinstance(client.transfers, AsyncUDataTransfersService)
            assert isinstance(client.transfers, AsyncTransfersService)

    asyncio.run(run())


def test_every_transfer_route_has_an_exact_wire_shape() -> None:
    """The four stock rows keep the exact verb, path, headers, and body the route documents."""
    actual = [
        wire.list_transfers_request(TransferListQuery(subject="dataset-1", subject_type="Dataset")),
        wire.list_transfers_request(TransferListQuery(recipient="organization-1", status="pending")),
        wire.get_transfer_request("transfer-1"),
        wire.request_transfer_request(SUBJECT),
        wire.respond_to_transfer_request("transfer-1", TransferResponseInput("accept")),
        wire.respond_to_transfer_request("transfer-1", TransferResponseInput("refuse", "not now")),
    ]
    assert actual == [
        ("GET", "/api/1/transfer/?subject=dataset-1&subject_type=Dataset", {}, None),
        ("GET", "/api/1/transfer/?recipient=organization-1&status=pending", {}, None),
        ("GET", "/api/1/transfer/transfer-1/", {}, None),
        (
            "POST",
            "/api/1/transfer/",
            {},
            {
                "subject": {"id": "dataset-1", "class": "Dataset"},
                "recipient": {"id": "organization-1", "class": "Organization"},
            },
        ),
        ("POST", "/api/1/transfer/transfer-1/", {}, {"response": "accept"}),
        ("POST", "/api/1/transfer/transfer-1/", {}, {"response": "refuse", "comment": "not now"}),
    ]


def test_transfer_request_and_response_wire_contract_is_exact_through_transport() -> None:
    """Bodies reach the deployment verbatim, with no invented keys and no explicit nulls."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client.transfers.request_transfer(
            SUBJECT,
            PERMISSIONS,
            _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET),
        )
        client.transfers.respond_to_transfer(
            "transfer-1",
            TransferResponseInput("accept"),
            PERMISSIONS,
            _policy(wire.RESPOND_TO_TRANSFER_OPERATION, "transfer-1", destructive=True),
        )

    requests = _transfer_requests(router)
    assert [(request.method, request.url) for request in requests] == [
        ("POST", f"{ORIGIN}/api/1/transfer/"),
        ("POST", f"{ORIGIN}/api/1/transfer/transfer-1/"),
    ]
    assert json.loads(cast("bytes", requests[0].body)) == {
        "subject": {"id": "dataset-1", "class": "Dataset"},
        "recipient": {"id": "organization-1", "class": "Organization"},
    }
    assert json.loads(cast("bytes", requests[1].body)) == {"response": "accept"}
    for request in requests:
        assert dict(request.headers)["X-API-KEY"] == "secret-key"


def test_transfer_reads_decode_losslessly_and_fail_typed_in_both_modes() -> None:
    """Both documented filters are encoded and both decoders keep the native transfer intact."""
    routes = _transfer_routes()
    with sync_client(sync_route_table(routes), UDATA_CREDENTIAL) as client:
        listed = client.transfers.list_transfers(
            TransferListQuery(subject="dataset-1", subject_type="Dataset"), PERMISSIONS
        )
        assert [record.payload for record in listed] == [thawed(_transfer())]
        filtered = client.transfers.list_transfers(
            TransferListQuery(recipient="organization-1", status="pending"), PERMISSIONS
        )
        assert filtered[0].payload["status"] == "accepted"
        assert client.transfers.get_transfer("transfer-1", PERMISSIONS).payload == thawed(_transfer())

    async def run() -> None:
        async with async_client(async_route_table(routes), UDATA_CREDENTIAL) as client:
            listed = await client.transfers.list_transfers(
                TransferListQuery(subject="dataset-1", subject_type="Dataset"), PERMISSIONS
            )
            assert [record.payload for record in listed] == [thawed(_transfer())]
            assert (await client.transfers.get_transfer("transfer-1", PERMISSIONS)).payload == thawed(_transfer())

    asyncio.run(run())


def test_transfer_listing_refuses_a_query_without_a_subject_or_recipient() -> None:
    """The stock parser answers 400 unless one side is named, so the request is never sent."""
    with pytest.raises(ValueError, match="subject or a recipient"):
        TransferListQuery(subject_type="Dataset", status="pending")


@pytest.mark.parametrize("subject_type", ["dataset", "Dataset ", "Dataservicex", "", 4, True])
def test_transfer_listing_rejects_undocumented_subject_classes(subject_type: object) -> None:
    with pytest.raises(ValueError, match="subject_type"):
        TransferListQuery(subject="dataset-1", subject_type=cast("str", subject_type))


@pytest.mark.parametrize("status", ["Pending", "done", "", 4, True])
def test_transfer_listing_rejects_undocumented_statuses(status: object) -> None:
    with pytest.raises(ValueError, match="status"):
        TransferListQuery(recipient="organization-1", status=cast("str", status))


@pytest.mark.parametrize("identifier", ["", ".", "..", "a/b", "a?b", "a#b", 'a"b', "a'b", "a\x00b", "a\nb", 4, None])
def test_transfer_route_identifiers_never_reach_a_path(identifier: object) -> None:
    """A dot segment is removed by RFC 3986 resolution, retargeting the transfer route."""
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        segment(cast("str", identifier), wire.GET_TRANSFER_OPERATION)
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.get_transfer_request(cast("str", identifier))
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.respond_to_transfer_request(cast("str", identifier), TransferResponseInput("refuse"))


def test_accepting_a_transfer_owns_a_dataset_so_ownership_reassignment_is_destructive() -> None:
    """Accepting moves the subject's owner; refusing leaves ownership untouched."""
    assert TransferResponseInput("accept").accepts is True
    assert TransferResponseInput("refuse").accepts is False


def test_a_transfer_subject_and_recipient_must_be_different_identifiers() -> None:
    """A self-transfer is ambiguous about who receives the dataset and is refused locally."""
    with pytest.raises(ValueError, match="different identifiers"):
        TransferRequestInput(
            subject_id="dataset-1",
            subject_class="Dataset",
            recipient_id="dataset-1",
            recipient_class="User",
        )


@pytest.mark.parametrize("comment", ["", "   ", 4, True])
def test_transfer_comments_reject_anything_but_a_non_empty_string(comment: object) -> None:
    with pytest.raises(ValueError, match="comment"):
        TransferRequestInput(
            subject_id="dataset-1",
            subject_class="Dataset",
            recipient_id="organization-1",
            recipient_class="Organization",
            comment=cast("str", comment),
        )


@pytest.mark.parametrize(
    ("subject_class", "recipient_class", "message"),
    [
        ("Dataset", "Dataset", "recipient class"),
        ("Dataset", "Group", "recipient class"),
        ("dataset", "Organization", "subject class"),
    ],
)
def test_transfer_reference_classes_stay_inside_the_documented_registries(
    subject_class: str, recipient_class: str, message: str
) -> None:
    """A recipient that is not a User or an Organization would name an unresolvable target."""
    with pytest.raises(ValueError, match=message):
        TransferRequestInput(
            subject_id="dataset-1",
            subject_class=subject_class,
            recipient_id="organization-1",
            recipient_class=recipient_class,
        )


@pytest.mark.parametrize(
    ("subject_id", "recipient_id"),
    [
        ("../dataset-1", "organization-1"),
        ("dataset-1", "../organization-1"),
        ("dataset-1", "organization-1/../organization-2"),
        ("..", "organization-1"),
        ("dataset-1", ".."),
        ("dataset-1", "organization-1?admin=1"),
    ],
)
def test_both_transfer_identifiers_cross_the_shared_segment_validator(subject_id: str, recipient_id: str) -> None:
    """T-04-ADM-01: the dataset id and the recipient id get the same identifier policy."""
    client_input = TransferRequestInput(
        subject_id=subject_id,
        subject_class="Dataset",
        recipient_id=recipient_id,
        recipient_class="Organization",
    )
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        wire.request_transfer_request(client_input)
    with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
        linked_identifier(
            recipient_id if subject_id == "dataset-1" else subject_id,
            wire.REQUEST_TRANSFER_OPERATION,
            "uData transfer subject",
        )


def test_the_transfer_mutation_target_binds_the_source_and_the_destination() -> None:
    """A confirmation approved for one recipient must not move the dataset to another."""
    assert SUBJECT.mutation_target() == COMPOSITE_TARGET


def test_requesting_a_transfer_is_refused_unless_the_composite_target_is_confirmed() -> None:
    """T-04-ADM-01: a bare dataset id or a near-miss recipient is refused before dispatch."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ForbiddenError, match="bound to this operation and target"):
            client.transfers.request_transfer(
                SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, "dataset-1")
            )
        with pytest.raises(ForbiddenError, match="bound to this operation and target"):
            client.transfers.request_transfer(
                SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, "dataset-1:organization-2")
            )
        with pytest.raises(ForbiddenError, match="bound to this operation and target"):
            client.transfers.request_transfer(
                SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, "organization-1:dataset-1")
            )
        with pytest.raises(ForbiddenError, match="bound to this operation and target"):
            client.transfers.request_transfer(
                SUBJECT, PERMISSIONS, _policy(wire.RESPOND_TO_TRANSFER_OPERATION, COMPOSITE_TARGET)
            )
        accepted = client.transfers.request_transfer(
            SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
        )
    assert accepted.receipt.target.value == COMPOSITE_TARGET
    assert [(request.method, request.url) for request in _transfer_requests(router)] == [
        ("POST", f"{ORIGIN}/api/1/transfer/")
    ]


def test_the_composite_transfer_target_is_enforced_in_async_mode() -> None:
    """The async ownership write carries the same target binding as the sync one."""
    router = async_route_table(_transfer_routes())

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError, match="bound to this operation and target"):
                await client.transfers.request_transfer(
                    SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, "dataset-1")
                )
            accepted = await client.transfers.request_transfer(
                SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
            )
            assert accepted.receipt.target.value == COMPOSITE_TARGET

    asyncio.run(run())
    assert [(request.method, request.url) for request in _transfer_requests(router)] == [
        ("POST", f"{ORIGIN}/api/1/transfer/")
    ]


@pytest.mark.parametrize(
    "permissions",
    [
        None,
        ANONYMOUS_PERMISSIONS,
        OTHER_SCOPE_PERMISSIONS,
        EffectivePermissions.for_credential(UDATA_CREDENTIAL, platform=CatalogPlatform.CKAN),
    ],
)
def test_ownership_writes_require_authenticated_matching_capability_evidence(
    permissions: EffectivePermissions | None,
) -> None:
    """T-04-ADM-01: an unproven caller is refused with zero network calls."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises((ForbiddenError, UnauthenticatedError)) as denied:
            client.transfers.request_transfer(
                SUBJECT,
                cast("EffectivePermissions", permissions),
                _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET),
            )
        with pytest.raises((ForbiddenError, UnauthenticatedError)) as denied_read:
            client.transfers.get_transfer("transfer-1", cast("EffectivePermissions", permissions))
    assert denied.value.operation == wire.REQUEST_TRANSFER_OPERATION
    assert denied_read.value.operation == wire.GET_TRANSFER_OPERATION
    assert _receipt_from(denied.value).outcome == "rejected"
    assert _transfer_requests(router) == []


def test_capability_denial_is_typed_and_zero_network_in_async_mode() -> None:
    """The async ownership write carries the same pre-dispatch capability gate."""
    router = async_route_table(_transfer_routes())

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(ForbiddenError) as denied:
                await client.transfers.request_transfer(
                    SUBJECT, OTHER_SCOPE_PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
                )
            with pytest.raises(ForbiddenError) as denied_read:
                await client.transfers.list_transfers(TransferListQuery(subject="dataset-1"), OTHER_SCOPE_PERMISSIONS)
            assert denied.value.operation == wire.REQUEST_TRANSFER_OPERATION
            assert denied_read.value.operation == wire.LIST_TRANSFERS_OPERATION
            assert denied.value.capability_state == "forbidden"
            assert _receipt_from(denied.value).outcome == "rejected"

    asyncio.run(run())
    assert _transfer_requests(router) == []


def test_transfers_without_an_explicit_confirmed_policy_are_refused_before_dispatch() -> None:
    """There is no client-wide confirmation bypass for an ownership write."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ForbiddenError, match="explicit MutationPolicy"):
            client.transfers.request_transfer(SUBJECT, PERMISSIONS, None)
        with pytest.raises(ForbiddenError, match="not explicitly confirmed"):
            client.transfers.request_transfer(
                SUBJECT,
                PERMISSIONS,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=False, operation=wire.REQUEST_TRANSFER_OPERATION, target=COMPOSITE_TARGET
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=True),
                ),
            )
        with pytest.raises(ForbiddenError, match="explicit overwrite intent"):
            client.transfers.request_transfer(
                SUBJECT,
                PERMISSIONS,
                MutationPolicy(
                    confirmation=ConfirmationPolicy(
                        confirmed=True, operation=wire.REQUEST_TRANSFER_OPERATION, target=COMPOSITE_TARGET
                    ),
                    concurrency=ConcurrencyPolicy(overwrite=False),
                ),
            )
    assert _transfer_requests(router) == []


def test_accepting_a_transfer_requires_the_destructive_tier_and_refusing_forbids_it() -> None:
    """The tier must match the effect, so a standard policy cannot reassign ownership."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(ForbiddenError, match="must declare MutationPolicy"):
            client.transfers.respond_to_transfer(
                "transfer-1",
                TransferResponseInput("accept"),
                PERMISSIONS,
                _policy(wire.RESPOND_TO_TRANSFER_OPERATION, "transfer-1"),
            )
        with pytest.raises(ForbiddenError, match="cannot carry a destructive policy tier"):
            client.transfers.respond_to_transfer(
                "transfer-1", TransferResponseInput("refuse"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
            )
        accepted = client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
        )
    assert accepted.receipt.outcome == "succeeded"
    assert accepted.receipt.audit_metadata["mutation"] == "accepted"


def test_a_transfer_write_never_authorizes_an_implicit_retry() -> None:
    """The stock uData API has no idempotency header, so a retrying policy is refused."""
    router = sync_route_table(_transfer_routes())
    with (
        sync_client(router, UDATA_CREDENTIAL) as client,
        pytest.raises(ForbiddenError, match="retrying dataset mutations safely"),
    ):
        client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), PERMISSIONS, RETRYING_ACCEPT_POLICY
        )
    assert _transfer_requests(router) == []


def test_an_interrupted_ownership_write_settles_ambiguous_never_failed() -> None:
    """T-04-ADM-03: the target may already have moved, so `failed` would be a false claim."""
    router = _SyncDroppingTransferRouter(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(TransportError) as raised:
        client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
        )
    receipt = _receipt_from(raised.value)
    assert receipt.outcome == "ambiguous"
    assert receipt.operation == wire.RESPOND_TO_TRANSFER_OPERATION
    assert receipt.target.value == "transfer-1"


def test_an_interrupted_ownership_write_settles_ambiguous_in_async_mode() -> None:
    """The async loop settles the same ambiguous outcome for the same interruption."""
    router = _AsyncDroppingTransferRouter(_transfer_routes())

    async def run() -> None:
        async with async_client(router, UDATA_CREDENTIAL) as client:
            with pytest.raises(TransportError) as raised:
                await client.transfers.respond_to_transfer(
                    "transfer-1", TransferResponseInput("accept"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
                )
            assert _receipt_from(raised.value).outcome == "ambiguous"

    asyncio.run(run())


def test_a_server_side_ownership_write_failure_settles_ambiguous() -> None:
    """A 5xx after dispatch leaves the deployment state unknown from the caller's seat."""
    router = sync_route_table(
        with_site_route({("POST", f"{ORIGIN}/api/1/transfer/transfer-1/"): (500, {"message": "boom"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(CatalogUnavailableError) as raised:
        client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
        )
    assert _receipt_from(raised.value).outcome == "ambiguous"
    assert _receipt_from(raised.value).audit_metadata["status_code"] == 500


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 410, 422])
def test_a_definitive_refusal_settles_failed_and_asserts_an_unchanged_owner(status: int) -> None:
    """A 4xx refusal is the deployment stating it did not act, so `failed` is truthful."""
    router = sync_route_table(
        with_site_route({("POST", f"{ORIGIN}/api/1/transfer/transfer-1/"): (status, {"message": "refused"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(CatalogError) as raised:
        client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
        )
    assert _receipt_from(raised.value).outcome == "failed"
    assert _receipt_from(raised.value).audit_metadata["status_code"] == status


def test_a_read_only_deployment_refusal_is_deployment_disabled_not_an_auth_failure() -> None:
    """A 423 read-only deployment is a deployment state, never an authentication verdict."""
    router = sync_route_table(
        with_site_route({("POST", f"{ORIGIN}/api/1/transfer/transfer-1/"): (423, {"message": "read only"})})
    )
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(CatalogUnavailableError) as raised:
        client.transfers.respond_to_transfer(
            "transfer-1", TransferResponseInput("accept"), PERMISSIONS, DESTRUCTIVE_REQUEST_POLICY
        )
    assert raised.value.capability_state == "deployment-disabled"
    assert not isinstance(raised.value, (ForbiddenError, UnauthenticatedError))
    assert _receipt_from(raised.value).outcome == "failed"


def test_a_malformed_transfer_confirmation_is_refused_before_any_dispatch() -> None:
    """T-04-ADM-01: an ambiguous target never reaches the deployment."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.transfers.request_transfer(
                TransferRequestInput(
                    subject_id="../dataset-1",
                    subject_class="Dataset",
                    recipient_id="organization-1",
                    recipient_class="Organization",
                ),
                PERMISSIONS,
                _policy(wire.REQUEST_TRANSFER_OPERATION, "../dataset-1:organization-1"),
            )
        with pytest.raises(CatalogValidationError, match="one URL-safe path segment"):
            client.transfers.respond_to_transfer(
                "../transfer-1",
                TransferResponseInput("refuse"),
                PERMISSIONS,
                _policy(wire.RESPOND_TO_TRANSFER_OPERATION, "../transfer-1"),
            )
    assert _transfer_requests(router) == []


def test_an_ownership_write_receipt_captures_both_the_source_and_the_destination() -> None:
    """T-04-ADM-02: repudiation needs the departing owner and the receiving one in one record."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.transfers.request_transfer(
            SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
        )
        refused = client.transfers.respond_to_transfer(
            "transfer-1",
            TransferResponseInput("refuse"),
            PERMISSIONS,
            _policy(wire.RESPOND_TO_TRANSFER_OPERATION, "transfer-1"),
        )
    assert isinstance(result, TransferMutationResult)
    assert result.transfer is not None
    assert result.receipt.target.value == COMPOSITE_TARGET
    assert COMPOSITE_TARGET in json.dumps(result.receipt.to_dict())
    assert result.receipt.audit_metadata["mutation"] == "requested"
    assert result.to_dict()["transfer"] == MappingRecord(_transfer()).to_dict()
    assert refused.receipt.outcome == "succeeded"
    assert refused.receipt.audit_metadata["mutation"] == "refused"
    assert refused.transfer is not None
    assert refused.transfer.payload["status"] == "accepted"


def test_an_ownership_write_receipt_is_metadata_only_and_redacted() -> None:
    """T-04-ADM-02: the retained record must not carry the credential or the raw response body."""
    router = sync_route_table(
        with_site_route(
            {
                ("POST", f"{ORIGIN}/api/1/transfer/"): (
                    201,
                    _transfer() | {"subject_title": "Bearer topsecretvalue"},
                )
            }
        )
    )
    with sync_client(router, UDATA_CREDENTIAL) as client:
        result = client.transfers.request_transfer(
            SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
        )
    rendered = json.dumps(result.to_dict())
    assert "secret-key" not in rendered
    assert "topsecretvalue" not in rendered
    assert set(result.receipt.to_dict()) >= {"operation", "outcome", "target", "audit_metadata"}


def test_a_pre_dispatch_rejection_still_carries_the_exact_composite_target() -> None:
    """T-04-ADM-02: the refused write is bound to the same subject pair as the allowed one."""
    router = sync_route_table(_transfer_routes())
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(ForbiddenError) as raised:
        client.transfers.request_transfer(
            SUBJECT, ANONYMOUS_PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
        )
    receipt = _receipt_from(raised.value)
    assert receipt.outcome == "rejected"
    assert receipt.target.value == COMPOSITE_TARGET
    assert receipt.audit_metadata["status_code"] == 0


@pytest.mark.parametrize(
    "payload",
    [None, "transfer", 4, [], {"status": "pending"}, {"id": "", "status": "pending"}, {"id": "transfer-1"}],
)
def test_a_malformed_transfer_object_fails_the_decoder(payload: object) -> None:
    with pytest.raises(CatalogValidationError):
        wire.parse_transfer(payload, wire.GET_TRANSFER_OPERATION)


@pytest.mark.parametrize(
    "payload",
    [None, {"id": "transfer-1", "status": "pending"}, "transfer", 4, [{"id": "transfer-1"}], [["transfer-1"]]],
)
def test_a_malformed_transfer_collection_fails_the_decoder(payload: object) -> None:
    with pytest.raises(CatalogValidationError):
        wire.parse_transfers(payload, wire.LIST_TRANSFERS_OPERATION)


@pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
def test_a_non_finite_transfer_response_fails_typed_without_a_raw_body(literal: bytes) -> None:
    """NaN and the infinities are not JSON and must never reach a transfer record."""
    body = b'[{"id": "transfer-1", "status": ' + literal + b"}]"
    router = sync_route_table(with_site_route({("GET", f"{ORIGIN}/api/1/transfer/transfer-1/"): (200, body)}))
    with sync_client(router, UDATA_CREDENTIAL) as client, pytest.raises(NativeCatalogError) as raised:
        client.transfers.get_transfer("transfer-1", PERMISSIONS)
    rendered = repr(raised.value) + str(raised.value.__dict__)
    assert literal.decode() not in rendered
    assert "secret-key" not in rendered


def test_transfers_are_bounded_before_dispatch() -> None:
    """T-04-ADM-03: a transfer response cannot be used to exhaust client memory."""
    assert services._MAX_TRANSFER_READ_BYTES == 1_048_576
    assert linked_identifier("dataset-1", wire.LIST_TRANSFERS_OPERATION, "uData transfer subject") == "dataset-1"


def test_transfer_dispatch_uses_the_exact_operation_string_in_both_modes() -> None:
    """The capability gate and the receipt must name the same pinned operation."""
    captured: list[dict[str, object]] = []

    def record(**kwargs: object) -> tuple[int, object, object]:
        captured.append(kwargs)
        return 201, _transfer(), object()

    router = sync_route_table(with_site_route({}))
    with sync_client(router, UDATA_CREDENTIAL) as client:
        client._dataset_call = record  # type: ignore[assignment]
        result = client.transfers.request_transfer(
            SUBJECT, PERMISSIONS, _policy(wire.REQUEST_TRANSFER_OPERATION, COMPOSITE_TARGET)
        )
    assert captured[0]["owning_operation"] == wire.REQUEST_TRANSFER_OPERATION
    assert captured[0]["path"] == "/api/1/transfer/"
    assert result.receipt.operation == wire.REQUEST_TRANSFER_OPERATION
