"""Immutable uData transfer-request inputs, queries, and bounded mutation results."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.udata.models._segment import path_segment

if TYPE_CHECKING:
    from datasluice.domain.catalog.models import MappingRecord
    from datasluice.domain.catalog.receipts import MutationReceipt

SUBJECT_CLASSES = frozenset({"Dataset", "Reuse", "Dataservice"})
RECIPIENT_CLASSES = frozenset({"User", "Organization"})
TRANSFER_STATUSES = frozenset({"pending", "accepted", "refused"})
RESPONSES = frozenset({"accept", "refuse"})
_TRANSFER_LABEL = "uData transfer"


def segment(value: str, operation: str) -> str:
    """Return *value* quoted as one URL-safe transfer path segment."""
    return path_segment(value, operation, _TRANSFER_LABEL)


def linked_identifier(value: str, operation: str, label: str) -> str:
    """Return *value* after checking it is one safe transfer identifier.

    Transfer subject, recipient, and filter identifiers travel in a JSON body or
    the query string rather than in a URL path, so the shared segment validator
    is applied for its identifier policy while the value itself is returned
    unencoded for its own serializer to encode exactly once.

    Args:
        value: The caller-supplied identifier destined for a body or query value.
        operation: The owning operation name, used to build the error details.
        label: The owning identifier label used in the error message.

    Returns:
        The unchanged identifier.

    Raises:
        CatalogValidationError: If the identifier is not one safe identifier.
    """
    path_segment(value, operation, label)
    return value


def _required_identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a non-empty string.")
    return value


def _documented_choice(value: object, choices: frozenset[str], label: str) -> str:
    if not isinstance(value, str) or value not in choices:
        raise ValueError(f"{label} must be one of the documented choices: {sorted(choices)}.")
    return value


def _optional_comment(value: object, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string when supplied.")
    return value


@dataclass(frozen=True, slots=True)
class TransferListQuery:
    """Stock v1 transfer collection filter surface.

    The upstream parser accepts ``subject``, ``subject_type``, ``recipient``, and
    ``status``, and answers 400 unless ``subject`` or ``recipient`` is supplied. A
    query carrying neither is therefore refused here instead of being dispatched,
    and a query has no valid default because of that rule.
    """

    subject: str | None = None
    subject_type: str | None = None
    recipient: str | None = None
    status: str | None = None

    def __post_init__(self) -> None:
        if self.subject is not None:
            _required_identifier(self.subject, "uData transfer subject filter")
        if self.subject_type is not None:
            _documented_choice(self.subject_type, SUBJECT_CLASSES, "uData transfer subject_type")
        if self.recipient is not None:
            _required_identifier(self.recipient, "uData transfer recipient filter")
        if self.status is not None:
            _documented_choice(self.status, TRANSFER_STATUSES, "uData transfer status")
        if self.subject is None and self.recipient is None:
            raise ValueError("uData transfer listing requires a subject or a recipient filter.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the documented filter parameters, omitting every unset one."""
        params: list[tuple[str, str]] = []
        if self.subject is not None:
            params.append(("subject", self.subject))
        if self.subject_type is not None:
            params.append(("subject_type", self.subject_type))
        if self.recipient is not None:
            params.append(("recipient", self.recipient))
        if self.status is not None:
            params.append(("status", self.status))
        return params


@dataclass(frozen=True, slots=True)
class TransferRequestInput:
    """The documented body POST /api/1/transfer/ resolves into one transfer request.

    ``subject`` and ``recipient`` are the ``{"id": ..., "class": ...}`` references
    the stock route resolves through its model registry. A transfer request moves
    its subject towards a new owner, so both identifiers and both class
    discriminators are required explicitly and the mutation target binds the pair
    rather than the subject alone.
    """

    subject_id: str
    subject_class: str
    recipient_id: str
    recipient_class: str
    comment: str | None = None

    def __post_init__(self) -> None:
        _required_identifier(self.subject_id, "uData transfer subject identifier")
        _documented_choice(self.subject_class, SUBJECT_CLASSES, "uData transfer subject class")
        _required_identifier(self.recipient_id, "uData transfer recipient identifier")
        _documented_choice(self.recipient_class, RECIPIENT_CLASSES, "uData transfer recipient class")
        _optional_comment(self.comment, "uData transfer comment")
        if self.subject_id == self.recipient_id:
            raise ValueError("A uData transfer subject and recipient must be different identifiers.")

    def payload(self) -> dict[str, object]:
        """Build the exact documented request body, omitting an absent comment."""
        body: dict[str, object] = {
            "subject": {"id": self.subject_id, "class": self.subject_class},
            "recipient": {"id": self.recipient_id, "class": self.recipient_class},
        }
        if self.comment is not None:
            body["comment"] = self.comment
        return body

    def mutation_target(self) -> str:
        """Return the composite target binding the subject and recipient identifiers."""
        return f"{self.subject_id}:{self.recipient_id}"


@dataclass(frozen=True, slots=True)
class TransferResponseInput:
    """The documented body POST /api/1/transfer/<id>/ answers a transfer with.

    ``accept`` reassigns ownership of the transfer subject to the recipient, so it
    is the destructive half of the route; ``refuse`` leaves ownership untouched.
    """

    response: str
    comment: str | None = None

    def __post_init__(self) -> None:
        _documented_choice(self.response, RESPONSES, "uData transfer response")
        _optional_comment(self.comment, "uData transfer response comment")

    @property
    def accepts(self) -> bool:
        """Return whether this response reassigns ownership of the transfer subject."""
        return self.response == "accept"

    def payload(self) -> dict[str, object]:
        """Build the exact documented response body, omitting an absent comment."""
        body: dict[str, object] = {"response": self.response}
        if self.comment is not None:
            body["comment"] = self.comment
        return body


@dataclass(frozen=True, slots=True)
class TransferMutationResult:
    """Transfer mutation output retaining a bounded transfer record and a redacted receipt."""

    receipt: MutationReceipt
    transfer: MappingRecord | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "receipt": self.receipt.to_dict(),
            "transfer": self.transfer.to_dict() if self.transfer is not None else None,
        }
