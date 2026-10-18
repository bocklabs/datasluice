"""Shared mutation receipt and outcome classification for the uData services."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import TYPE_CHECKING, Never, cast

from datasluice.connectors.catalog.udata.settlement import SETTLEMENT_ERRORS
from datasluice.domain.catalog.operations import OperationId
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.safety import ConcurrencyPolicy, IdempotencyPolicy, MutationPolicy
from datasluice.errors.catalog import (
    BudgetExhaustedError,
    CatalogValidationError,
    ForbiddenError,
    UnauthenticatedError,
    attach_catalog_metadata,
)
from datasluice.runtime.transport.base import TransportError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from typing import Protocol

    class _ClosesInput(Protocol):
        def close(self) -> None: ...

    class _CarriesReceipt(Protocol):
        @property
        def receipt(self) -> MutationReceipt: ...


def _operation_id(operation: str) -> OperationId:
    """Convert one profile operation string into its typed identity."""
    platform, _, tail = operation.partition("/")
    service, _, method = tail.partition(".")
    return OperationId(platform=platform, service=service or "native", method=method or tail)


def _failure_event(error: BaseException) -> str:
    """Return the canonical failure outcome event for one error."""
    if isinstance(error, BudgetExhaustedError):
        return "budget_exhausted"
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    return "failed"


def _receipt_policy(policy: MutationPolicy | None) -> MutationPolicy:
    """Create a receipt-only policy that never retains unsupported tokens."""
    if policy is None:
        return MutationPolicy(concurrency=ConcurrencyPolicy(overwrite=True))
    return MutationPolicy(
        destructive=policy.destructive,
        concurrency=ConcurrencyPolicy(overwrite=True),
        idempotency=IdempotencyPolicy(
            safe=policy.idempotency.safe,
            explicit_retry_opt_in=policy.idempotency.explicit_retry_opt_in,
        ),
        dry_run=policy.dry_run,
    )


def _policy_metadata(policy: MutationPolicy | None) -> dict[str, object]:
    """Return the bounded policy facts retained in receipt metadata."""
    if policy is None:
        return {"provided": False}
    confirmation = policy.confirmation
    concurrency = policy.concurrency
    return {
        "provided": True,
        "destructive": policy.destructive,
        "confirmed": confirmation is not None and confirmation.confirmed,
        "overwrite": concurrency is not None and concurrency.overwrite,
        "concurrency_provided": concurrency is not None,
        "retry_safe": policy.idempotency.safe,
        "retry_opt_in": policy.idempotency.explicit_retry_opt_in,
        "idempotency_provided": policy.idempotency.key is not None,
        "dry_run": policy.dry_run.requested,
    }


def _error_status(error: BaseException, response: object | None = None) -> int:
    status = getattr(error, "status_code", None)
    if isinstance(status, int):
        return status
    metadata = getattr(error, "metadata", None)
    if isinstance(metadata, Mapping) and isinstance(metadata.get("status_code"), int):
        return cast("int", metadata["status_code"])
    response_status = getattr(response, "status_code", None)
    return response_status if isinstance(response_status, int) else 0


def _mutation_outcome(error: BaseException, response: object | None = None, *, dispatched: bool = True) -> str:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    metadata = getattr(error, "metadata", None)
    if isinstance(metadata, Mapping) and metadata.get("ambiguous") is True:
        return "ambiguous"
    error_status = _error_status(error, response)
    if 200 <= error_status < 300:
        return "ambiguous"
    if dispatched and isinstance(error, TransportError):
        return "ambiguous"
    if 300 <= error_status < 400:
        return "ambiguous"
    if error_status == 0 and isinstance(error, (UnauthenticatedError, ForbiddenError, CatalogValidationError)):
        return "rejected"
    return "failed"


def _attach_receipt(error: BaseException, receipt: MutationReceipt) -> BaseException:
    """Attach a shared receipt while preserving the original exception type."""
    attach_catalog_metadata(error, {"receipt": receipt.to_dict()})
    error_dict = getattr(error, "__dict__", None)
    if isinstance(error_dict, dict):
        error_dict["mutation_receipt"] = receipt
    return error


def _raise_with_receipt(error: BaseException, receipt: MutationReceipt) -> Never:
    _attach_receipt(error, receipt)
    raise error


def _close_input(
    client_input: _ClosesInput,
    result: _CarriesReceipt | None,
    primary_error: BaseException | None,
) -> None:
    try:
        client_input.close()
    except SETTLEMENT_ERRORS as close_error:
        receipt = result.receipt if result is not None else getattr(primary_error, "mutation_receipt", None)
        if isinstance(receipt, MutationReceipt):
            _attach_receipt(close_error, receipt)
        if primary_error is not None:
            raise primary_error from close_error
        raise


type FailureReceipt = Callable[[BaseException, object | None], MutationReceipt]
type ResultShaper[T] = Callable[[int, object, object], T]


def _run_mutation[T](
    dispatch: Callable[[], tuple[int, object, object]],
    failure_receipt: FailureReceipt,
    completed: ResultShaper[T],
    *,
    precondition: Callable[[], None] | None = None,
    errors: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Run one mutation.

    On a covered error, attach a receipt and raise the original error.
    """
    response: object | None = None
    try:
        if precondition is not None:
            precondition()
        status, payload, response = dispatch()
        return completed(status, payload, response)
    except errors as error:
        _raise_with_receipt(error, failure_receipt(error, response))


async def _run_mutation_async[T](
    dispatch: Callable[[], Awaitable[tuple[int, object, object]]],
    failure_receipt: FailureReceipt,
    completed: ResultShaper[T],
    *,
    precondition: Callable[[], None] | None = None,
    errors: tuple[type[BaseException], ...] = (Exception, asyncio.CancelledError),
) -> T:
    """Run one mutation on the async path.

    The receipt seam matches the sync path.
    """
    response: object | None = None
    try:
        if precondition is not None:
            precondition()
        status, payload, response = await dispatch()
        return completed(status, payload, response)
    except errors as error:
        _raise_with_receipt(error, failure_receipt(error, response))
