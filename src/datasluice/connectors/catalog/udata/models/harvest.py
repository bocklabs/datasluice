"""Immutable uData harvest source, job, budgeted-wait, and bulk records."""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from datasluice.connectors.catalog.udata.models._segment import path_segment
from datasluice.domain.catalog.models import MappingRecord, _freeze_json, _thaw_json
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.resilience import TimeBudget
from datasluice.errors.catalog import CatalogValidationError
from datasluice.exceptions import DataSluiceError
from datasluice.runtime.bulk import BulkItemReceipt, BulkPlan, BulkSummary

if TYPE_CHECKING:
    from datasluice.domain.catalog.receipts import BulkCheckpoint

PLATFORM = "udata"
HARVEST_OPERATION = "udata/api-v1.harvest-moderation-and-admin-operations"

HARVEST_FREQUENCIES = frozenset({"manual", "monthly", "weekly", "daily"})
HARVEST_VALIDATION_STATES = frozenset({"pending", "accepted", "refused"})
HARVEST_JOB_STATUSES = frozenset(
    {"pending", "initializing", "initialized", "processing", "done", "done-errors", "failed"}
)
HARVEST_TERMINAL_JOB_STATUSES = frozenset({"done", "done-errors", "failed"})
HARVEST_ITEM_STATUSES = frozenset({"pending", "started", "done", "failed", "skipped", "archived"})

HARVEST_BULK_ACTIONS = frozenset({"delete-source", "unschedule-source", "run-source"})
HARVEST_DESTRUCTIVE_BULK_ACTIONS = frozenset({"delete-source", "unschedule-source"})

ALLOWED_CRAWL_SCHEMES = frozenset({"http", "https"})
_PRIVATE_HOST_NAMES = frozenset({"localhost", "ip6-localhost", "ip6-loopback"})
_PRIVATE_HOST_SUFFIXES = (
    ".localhost",
    ".local",
    ".lan",
    ".internal",
    ".intranet",
    ".corp",
    ".localdomain",
    ".home.arpa",
    ".onion",
    ".test",
    ".invalid",
    ".example",
)
_PUBLIC_HOSTNAME = re.compile(r"^(?=.{1,253}$)([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}\.?$")
_ABSOLUTE_URL = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")
_MAX_ENDPOINT_LENGTH = 2048
_CRON_FIELD = re.compile(r"^[0-9*,/\-]+$")
_MAX_PAGE = 100_000
_MAX_PAGE_SIZE = 100
_MAX_BULK_TARGETS = 64
_MAX_BULK_PARALLELISM = 8
_MAX_POLLS = 1024
_MAX_POLL_INTERVAL = 300.0
_MAX_TEXT = 255


def _endpoint_rejection(code: str) -> CatalogValidationError:
    return CatalogValidationError(
        "The harvest source endpoint is not a permitted crawl target.",
        operation=HARVEST_OPERATION,
        platform=PLATFORM,
        capability_state="forbidden",
        safe_action=(
            "Point the harvest source at a public http(s) endpoint, or allow the private target "
            "explicitly on this input."
        ),
        metadata={"endpoint_classification": code},
    )


def _public_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if address.is_private or address.is_loopback or address.is_link_local:
        return False
    if address.is_multicast or address.is_reserved or address.is_unspecified:
        return False
    return bool(address.is_global)


def _private_host(host: str) -> bool:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return not _public_address(address)
    if host in _PRIVATE_HOST_NAMES or host.endswith(_PRIVATE_HOST_SUFFIXES):
        return True
    return _PUBLIC_HOSTNAME.fullmatch(host) is None


def crawl_endpoint(value: object, *, allow_private: bool = False) -> str:
    """Return *value* after validating it as a crawl target a server may fetch.

    A harvest source endpoint is fetched by the deployment, never by this
    client, so an attacker-supplied value is a server-side request-forgery
    primitive. The scheme allowlist, the rejection of embedded credentials, and
    the port range are always enforced; the private/loopback/link-local target
    rule is only relaxed by the caller's explicit per-input allowance.

    Args:
        value: The caller-supplied endpoint URL.
        allow_private: Whether this input explicitly permits a non-public target.

    Returns:
        The validated endpoint, unchanged.

    Raises:
        CatalogValidationError: If the endpoint is not a permitted crawl target.
            The classification is bounded metadata; the host is never retained.
    """
    if not isinstance(value, str) or not value:
        raise _endpoint_rejection("missing")
    if len(value) > _MAX_ENDPOINT_LENGTH:
        raise _endpoint_rejection("length")
    if any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value):
        raise _endpoint_rejection("format")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in ALLOWED_CRAWL_SCHEMES:
        raise _endpoint_rejection("scheme")
    if "@" in parsed.netloc:
        raise _endpoint_rejection("userinfo")
    try:
        port = parsed.port
    except ValueError:
        raise _endpoint_rejection("port") from None
    if port is not None and not 1 <= port <= 65535:
        raise _endpoint_rejection("port")
    if not parsed.hostname:
        raise _endpoint_rejection("host")
    if not allow_private and _private_host(parsed.hostname):
        raise _endpoint_rejection("locality")
    return value


def _validated_endpoint_tree(value: object, *, allow_private: bool) -> None:
    if isinstance(value, str):
        if _ABSOLUTE_URL.match(value):
            crawl_endpoint(value, allow_private=allow_private)
    elif isinstance(value, Mapping):
        for nested in value.values():
            _validated_endpoint_tree(nested, allow_private=allow_private)
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _validated_endpoint_tree(nested, allow_private=allow_private)


def _frozen_mapping(value: Mapping[str, object], label: str) -> Mapping[str, object]:
    try:
        frozen = _freeze_json(dict(value), f"udata.{label}")
    except DataSluiceError as error:
        raise ValueError(f"uData {label} must contain JSON-safe values only.") from error
    if not isinstance(frozen, Mapping):
        raise ValueError(f"uData {label} must be a mapping when supplied.")
    return frozen


def _validate_text(value: object, label: str, *, required: bool = False) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"uData harvest {label} must be a non-empty string.")
    if isinstance(value, str) and len(value) > _MAX_TEXT:
        raise ValueError(f"uData harvest {label} must be at most {_MAX_TEXT} characters.")
    return value


def _validate_boolean(value: object, label: str) -> bool | None:
    if value is not None and type(value) is not bool:
        raise ValueError(f"uData harvest {label} must be a boolean when supplied.")
    return value


def _validate_page(value: object, label: str) -> None:
    if type(value) is not int or not 1 <= value <= _MAX_PAGE:
        raise ValueError(f"uData harvest {label} must be an integer from 1 through {_MAX_PAGE}.")


def _validate_page_size(value: object, label: str) -> None:
    if type(value) is not int or not 1 <= value <= _MAX_PAGE_SIZE:
        raise ValueError(f"uData harvest {label} must be an integer from 1 through {_MAX_PAGE_SIZE}.")


def source_segment(value: str, operation: str = HARVEST_OPERATION) -> str:
    """Return *value* encoded as one URL-safe harvest source path segment."""
    return path_segment(value, operation, "uData harvest source")


def job_segment(value: str, operation: str = HARVEST_OPERATION) -> str:
    """Return *value* encoded as one URL-safe harvest job path segment."""
    return path_segment(value, operation, "uData harvest job")


def linked_identifier(value: str, operation: str, label: str) -> str:
    """Return *value* after applying the identifier policy to a body reference.

    Harvest owner and organization references travel in the JSON body rather
    than in the path, so the segment policy is applied for its identifier rules
    and the value itself is returned unencoded.
    """
    path_segment(value, operation, label)
    return value


@dataclass(frozen=True, slots=True)
class HarvestSourceInput:
    """Presence-aware write payload for the harvest source routes.

    ``url`` is the crawl target the deployment fetches, so it is validated
    before the value ever reaches a request builder. ``allow_private_endpoint``
    is the explicit per-input consent that relaxes the non-public target rule;
    it is never sent to the deployment and never appears in a receipt.
    """

    name: str
    url: str
    backend: str
    description: str | None = None
    owner_id: str | None = None
    organization_id: str | None = None
    frequency: str | None = None
    active: bool | None = None
    autoarchive: bool | None = None
    config: Mapping[str, object] | None = None
    allow_private_endpoint: bool = False

    def __post_init__(self) -> None:
        _validate_text(self.name, "name", required=True)
        object.__setattr__(self, "url", crawl_endpoint(self.url, allow_private=self.allow_private_endpoint))
        _validate_text(self.backend, "backend", required=True)
        _validate_text(self.description, "description")
        if self.owner_id is not None:
            object.__setattr__(
                self, "owner_id", linked_identifier(self.owner_id, HARVEST_OPERATION, "uData harvest owner")
            )
        if self.organization_id is not None:
            object.__setattr__(
                self,
                "organization_id",
                linked_identifier(self.organization_id, HARVEST_OPERATION, "uData harvest organization"),
            )
        if self.frequency is not None and self.frequency not in HARVEST_FREQUENCIES:
            raise ValueError(f"uData harvest frequency must be one of {sorted(HARVEST_FREQUENCIES)}.")
        _validate_boolean(self.active, "active")
        _validate_boolean(self.autoarchive, "autoarchive")
        if type(self.allow_private_endpoint) is not bool:
            raise ValueError("uData harvest private endpoint allowance must be a boolean.")
        if self.config is not None:
            if not isinstance(self.config, Mapping):
                raise ValueError("uData harvest config must be a mapping when supplied.")
            frozen = _frozen_mapping(self.config, "harvest source config")
            _validated_endpoint_tree(frozen, allow_private=self.allow_private_endpoint)
            object.__setattr__(self, "config", frozen)

    @property
    def endpoint_scope(self) -> str:
        """Return the bounded endpoint classification recorded on receipts."""
        return "private-allowed" if self.allow_private_endpoint else "public"

    def payload(self) -> dict[str, object]:
        """Encode the exact write JSON body with omission semantics."""
        body: dict[str, object] = {"name": self.name, "url": self.url, "backend": self.backend}
        if self.description is not None:
            body["description"] = self.description
        if self.owner_id is not None:
            body["owner"] = self.owner_id
        if self.organization_id is not None:
            body["organization"] = self.organization_id
        if self.frequency is not None:
            body["frequency"] = self.frequency
        if self.active is not None:
            body["active"] = self.active
        if self.autoarchive is not None:
            body["autoarchive"] = self.autoarchive
        if self.config is not None:
            body["config"] = _thaw_json(self.config)
        return body


@dataclass(frozen=True, slots=True)
class HarvestScheduleInput:
    """The cron expression posted to the harvest schedule route."""

    cron: str

    def __post_init__(self) -> None:
        if not isinstance(self.cron, str) or not self.cron.strip():
            raise ValueError("uData harvest schedule cron must be a non-empty string.")
        fields = self.cron.split()
        if len(fields) != 5:
            raise ValueError("uData harvest schedule cron requires five whitespace-separated fields.")
        for field_value in fields:
            if len(field_value) > 32 or _CRON_FIELD.fullmatch(field_value) is None:
                raise ValueError("uData harvest schedule cron fields accept digits, '*', ',', '/', and '-' only.")

    def payload(self) -> str:
        """Encode the exact schedule JSON string body."""
        return self.cron


@dataclass(frozen=True, slots=True)
class HarvestValidationInput:
    """The state and comment posted to the harvest validation route."""

    state: str
    comment: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, str) or self.state not in HARVEST_VALIDATION_STATES:
            raise ValueError(f"uData harvest validation state must be one of {sorted(HARVEST_VALIDATION_STATES)}.")
        if self.state == "refused" and not (isinstance(self.comment, str) and self.comment.strip()):
            raise ValueError("uData harvest rejection validation requires a comment.")
        _validate_text(self.comment, "validation comment")

    def payload(self) -> dict[str, object]:
        """Encode the exact validation JSON body with omission semantics."""
        body: dict[str, object] = {"state": self.state}
        if self.comment is not None:
            body["comment"] = self.comment
        return body


@dataclass(frozen=True, slots=True)
class HarvestSourceQuery:
    """The documented harvest source index query surface.

    Only ``owner``, ``organization``, ``q``, ``deleted``, and the pager are
    accepted; the source index parser declares no sort argument.
    """

    q: str | None = None
    page: int = 1
    page_size: int = 20
    owner_id: str | None = None
    organization_id: str | None = None
    deleted: bool = False

    def __post_init__(self) -> None:
        _validate_page(self.page, "source list page")
        _validate_page_size(self.page_size, "source list page_size")
        _validate_text(self.q, "source list q")
        if self.owner_id is not None:
            object.__setattr__(
                self, "owner_id", linked_identifier(self.owner_id, HARVEST_OPERATION, "uData harvest owner")
            )
        if self.organization_id is not None:
            object.__setattr__(
                self,
                "organization_id",
                linked_identifier(self.organization_id, HARVEST_OPERATION, "uData harvest organization"),
            )
        if type(self.deleted) is not bool:
            raise ValueError("uData harvest source list deleted flag must be a boolean.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact source index query-string pairs."""
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.q is not None:
            params.append(("q", self.q))
        if self.owner_id is not None:
            params.append(("owner", self.owner_id))
        if self.organization_id is not None:
            params.append(("organization", self.organization_id))
        if self.deleted:
            params.append(("deleted", "true"))
        return params


@dataclass(frozen=True, slots=True)
class HarvestJobQuery:
    """The documented harvest job collection query surface."""

    page: int = 1
    page_size: int = 20

    def __post_init__(self) -> None:
        _validate_page(self.page, "job list page")
        _validate_page_size(self.page_size, "job list page_size")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact job collection query-string pairs."""
        return [("page", str(self.page)), ("page_size", str(self.page_size))]


@dataclass(frozen=True, slots=True)
class HarvestJobItemsQuery:
    """The documented harvest job item collection query surface."""

    page: int = 1
    page_size: int = 20
    status: str | None = None

    def __post_init__(self) -> None:
        _validate_page(self.page, "job item list page")
        _validate_page_size(self.page_size, "job item list page_size")
        if self.status is not None and self.status not in HARVEST_ITEM_STATUSES:
            raise ValueError(f"uData harvest item status must be one of {sorted(HARVEST_ITEM_STATUSES)}.")

    def query_params(self) -> list[tuple[str, str]]:
        """Encode the exact job item collection query-string pairs."""
        params: list[tuple[str, str]] = [("page", str(self.page)), ("page_size", str(self.page_size))]
        if self.status is not None:
            params.append(("status", self.status))
        return params


@dataclass(frozen=True, slots=True)
class HarvestPage:
    """One bounded harvest page with native pager field presence retained."""

    records: tuple[MappingRecord, ...]
    page: int | None = None
    page_size: int | None = None
    previous_page: str | None = None
    next_page: str | None = None
    total: int | None = None
    present_fields: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not isinstance(self.records, tuple) or not all(isinstance(record, MappingRecord) for record in self.records):
            raise ValueError("uData harvest page records must be a tuple of mapping records.")
        if not isinstance(self.present_fields, frozenset):
            raise ValueError("uData harvest page presence must be a frozenset of pager field names.")

    def to_dict(self) -> dict[str, object]:
        """Return the page as a JSON-safe projection without raw response bodies."""
        return {
            "records": [record.to_dict() for record in self.records],
            "page": self.page,
            "page_size": self.page_size,
            "previous_page": self.previous_page,
            "next_page": self.next_page,
            "total": self.total,
            "present_fields": sorted(self.present_fields),
        }


@dataclass(frozen=True, slots=True)
class UDataJobHandle:
    """Immutable typed handle for one queued harvest job and its bounded wait.

    Harvest dispatch is asynchronous on the deployment, so a queued operation
    returns this handle instead of blocking. The handle owns the caller's wait
    budget, the poll interval, and the poll ceiling, so a job can only be
    awaited under an explicit finite lease and never by an unbounded loop.
    """

    source_id: str
    job_id: str | None = None
    status: str = "pending"
    budget: TimeBudget = TimeBudget()
    poll_interval: float = 1.0
    max_polls: int = 16
    polls: int = 0
    exhausted: bool = False
    abandoned: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("uData job handles require a non-empty source identifier.")
        if self.job_id is not None and (not isinstance(self.job_id, str) or not self.job_id):
            raise ValueError("uData job handle job identifiers must be a non-empty string when supplied.")
        if not isinstance(self.status, str) or self.status not in HARVEST_JOB_STATUSES:
            raise ValueError(f"uData job handle status must be one of {sorted(HARVEST_JOB_STATUSES)}.")
        if not isinstance(self.budget, TimeBudget):
            raise ValueError("uData job handle wait budgets must use TimeBudget.")
        if type(self.poll_interval) not in (int, float) or not 0 <= float(self.poll_interval) <= _MAX_POLL_INTERVAL:
            raise ValueError(f"uData job handle poll intervals must be between 0 and {_MAX_POLL_INTERVAL} seconds.")
        object.__setattr__(self, "poll_interval", float(self.poll_interval))
        if type(self.max_polls) is not int or not 1 <= self.max_polls <= _MAX_POLLS:
            raise ValueError(f"uData job handle poll ceilings must be an integer from 1 through {_MAX_POLLS}.")
        if type(self.polls) is not int or self.polls < 0:
            raise ValueError("uData job handle poll counts must be a non-negative integer.")
        if not all(type(state) is bool for state in (self.exhausted, self.abandoned)):
            raise ValueError("uData job handle lease state must use booleans.")

    @property
    def terminal(self) -> bool:
        """Return whether the observed job status is terminal."""
        return self.status in HARVEST_TERMINAL_JOB_STATUSES

    def observed(self, status: str, *, exhausted: bool = False, abandoned: bool = False) -> UDataJobHandle:
        """Return the handle advanced by one bounded observation."""
        if not isinstance(status, str) or status not in HARVEST_JOB_STATUSES:
            raise ValueError(f"uData job handle status must be one of {sorted(HARVEST_JOB_STATUSES)}.")
        return UDataJobHandle(
            source_id=self.source_id,
            job_id=self.job_id,
            status=status,
            budget=self.budget,
            poll_interval=self.poll_interval,
            max_polls=self.max_polls,
            polls=self.polls + 1,
            exhausted=exhausted,
            abandoned=abandoned,
        )

    def abandoned_lease(self, *, exhausted: bool = False) -> UDataJobHandle:
        """Return the handle after the caller stopped watching a still-running job.

        A caller lease is finite by construction, so releasing it mid-run is a
        normal outcome rather than a failure. Recording it keeps the outstanding
        remote work detectable and lets the same handle be resumed later.

        Args:
            exhausted: Whether the lease ended because its time budget ran out.

        Returns:
            The same handle with its outstanding work marked abandoned.
        """
        return UDataJobHandle(
            source_id=self.source_id,
            job_id=self.job_id,
            status=self.status,
            budget=self.budget,
            poll_interval=self.poll_interval,
            max_polls=self.max_polls,
            polls=self.polls,
            exhausted=exhausted,
            abandoned=True,
        )

    def to_dict(self) -> dict[str, object]:
        """Return the handle as a JSON-safe projection without endpoints or secrets."""
        return {
            "source_id": self.source_id,
            "job_id": self.job_id,
            "status": self.status,
            "terminal": self.terminal,
            "polls": self.polls,
            "max_polls": self.max_polls,
            "poll_interval": self.poll_interval,
            "budget_seconds": self.budget.total,
            "exhausted": self.exhausted,
            "abandoned": self.abandoned,
        }


@dataclass(frozen=True, slots=True)
class HarvestJobWaitResult:
    """The outcome of one bounded harvest-job wait."""

    handle: UDataJobHandle
    record: MappingRecord | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.handle, UDataJobHandle):
            raise ValueError("uData job wait results require a typed job handle.")
        if self.record is not None and not isinstance(self.record, MappingRecord):
            raise ValueError("uData job wait result records require a mapping record.")

    def to_dict(self) -> dict[str, object]:
        """Return the wait outcome as a JSON-safe projection."""
        return {
            "handle": self.handle.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }


@dataclass(frozen=True, slots=True)
class HarvestMutationResult:
    """Harvest mutation output: a redacted receipt plus any returned record."""

    receipt: MutationReceipt
    record: MappingRecord | None = None
    handle: UDataJobHandle | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, MutationReceipt):
            raise ValueError("uData harvest mutation results require a shared mutation receipt.")
        if self.record is not None and not isinstance(self.record, MappingRecord):
            raise ValueError("uData harvest mutation result records require a mapping record.")
        if self.handle is not None and not isinstance(self.handle, UDataJobHandle):
            raise ValueError("uData harvest mutation result handles require a typed job handle.")

    def to_dict(self) -> dict[str, object]:
        """Return the mutation result as a JSON-safe projection."""
        return {
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
            "handle": self.handle.to_dict() if self.handle is not None else None,
        }


@dataclass(frozen=True, slots=True)
class HarvestPreviewResult:
    """The receipt-bearing outcome of one server dry-run harvest preview."""

    receipt: MutationReceipt
    record: MappingRecord | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.receipt, MutationReceipt):
            raise ValueError("uData harvest preview results require a shared mutation receipt.")
        if self.record is not None and not isinstance(self.record, MappingRecord):
            raise ValueError("uData harvest preview result records require a mapping record.")

    def to_dict(self) -> dict[str, object]:
        """Return the preview outcome as a JSON-safe projection."""
        return {
            "dry_run": True,
            "receipt": self.receipt.to_dict(),
            "record": self.record.to_dict() if self.record is not None else None,
        }


@dataclass(frozen=True, slots=True)
class HarvestBulkTargets:
    """A bounded ordered set of harvest sources for one admin bulk plan."""

    source_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.source_ids, tuple):
            object.__setattr__(self, "source_ids", tuple(self.source_ids))
        if not 1 <= len(self.source_ids) <= _MAX_BULK_TARGETS:
            raise ValueError(
                f"uData harvest bulk targets must hold between 1 and {_MAX_BULK_TARGETS} source identifiers."
            )
        seen: set[str] = set()
        for source_id in self.source_ids:
            if not isinstance(source_id, str) or not source_id:
                raise ValueError("uData harvest bulk target identifiers must be non-empty strings.")
            if source_id in seen:
                raise ValueError(f"uData harvest bulk targets repeat the identifier {source_id!r}.")
            seen.add(source_id)

    def to_dict(self) -> dict[str, object]:
        """Return the bulk targets as a JSON-safe projection."""
        return {"source_ids": list(self.source_ids)}


def bulk_target(action: str, source_ids: Sequence[str]) -> str:
    """Return the bounded exact confirmation target for one ordered bulk plan.

    The digest binds the confirmation to the action and to the exact ordered
    source set without retaining any identifier in a receipt target.
    """
    if action not in HARVEST_BULK_ACTIONS:
        raise ValueError(f"uData harvest bulk action must be one of {sorted(HARVEST_BULK_ACTIONS)}.")
    if not isinstance(source_ids, Sequence):
        raise ValueError("uData harvest bulk targets require a sequence of identifiers.")
    digest = sha256("\n".join(source_ids).encode()).hexdigest()[:24]
    return f"{action}:{digest}"


def bulk_plan(action: str, source_ids: Sequence[str], *, preview: bool) -> BulkPlan:
    """Return the shared runtime bulk plan for one harvest admin action."""
    from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind

    if action not in HARVEST_BULK_ACTIONS:
        raise ValueError(f"uData harvest bulk action must be one of {sorted(HARVEST_BULK_ACTIONS)}.")
    if type(preview) is not bool:
        raise ValueError("uData harvest bulk plan preview state must be a boolean.")
    items = tuple(
        CatalogId(
            platform=CatalogPlatform.UDATA,
            resource_kind=ResourceKind("harvest-source"),
            value=source_id,
        )
        for source_id in source_ids
    )
    return BulkPlan(operation=HARVEST_OPERATION, items=items, preview=preview)


@dataclass(frozen=True, slots=True)
class HarvestBulkPreview:
    """A local non-dispatching preview of one bounded harvest admin bulk plan."""

    action: str
    targets: HarvestBulkTargets
    destructive: bool

    def __post_init__(self) -> None:
        if self.action not in HARVEST_BULK_ACTIONS:
            raise ValueError(f"uData harvest bulk action must be one of {sorted(HARVEST_BULK_ACTIONS)}.")
        if not isinstance(self.targets, HarvestBulkTargets):
            raise ValueError("uData harvest bulk previews require typed bulk targets.")
        if type(self.destructive) is not bool:
            raise ValueError("uData harvest bulk destructive state must be a boolean.")
        if self.destructive != (self.action in HARVEST_DESTRUCTIVE_BULK_ACTIONS):
            raise ValueError("uData harvest bulk destructive state must match the requested action.")

    @property
    def plan(self) -> BulkPlan:
        """Return the non-dispatching plan this preview describes."""
        return bulk_plan(self.action, self.targets.source_ids, preview=True)

    @property
    def confirmation_target(self) -> str:
        """Return the exact confirmation target this plan must be confirmed against."""
        return bulk_target(self.action, self.targets.source_ids)

    def to_dict(self) -> dict[str, object]:
        """Return the preview as a JSON-safe projection without dispatching."""
        return {
            "action": self.action,
            "operation": HARVEST_OPERATION,
            "targets": self.targets.to_dict(),
            "destructive": self.destructive,
            "plan": self.plan.to_dict(),
            "dispatched": False,
        }


@dataclass(frozen=True, slots=True)
class HarvestBulkResult:
    """The terminal outcome of one bounded harvest admin bulk run."""

    action: str
    plan: BulkPlan
    receipts: tuple[BulkItemReceipt, ...]
    summary: BulkSummary
    checkpoints: tuple[Mapping[str, object], ...] = ()
    resume: BulkCheckpoint | None = None

    def __post_init__(self) -> None:
        if self.action not in HARVEST_BULK_ACTIONS:
            raise ValueError(f"uData harvest bulk action must be one of {sorted(HARVEST_BULK_ACTIONS)}.")
        if not isinstance(self.plan, BulkPlan):
            raise ValueError("uData harvest bulk results require a typed bulk plan.")
        if not isinstance(self.receipts, tuple) or not all(isinstance(item, BulkItemReceipt) for item in self.receipts):
            raise ValueError("uData harvest bulk results require typed per-item receipts.")
        if not isinstance(self.summary, BulkSummary):
            raise ValueError("uData harvest bulk results require a typed bulk summary.")

    def to_dict(self) -> dict[str, object]:
        """Return the bulk outcome as a JSON-safe projection without error objects."""
        return {
            "action": self.action,
            "operation": self.plan.operation,
            "targets": [self.plan.items[item.index].value for item in self.receipts],
            "item_outcomes": [item.receipt.outcome for item in self.receipts],
            "summary": {
                "total": self.summary.total,
                "succeeded": self.summary.succeeded,
                "failed": self.summary.failed,
                "skipped": self.summary.skipped,
                "settled": self.summary.settled,
                "outstanding": self.summary.outstanding,
                "dispatches": self.summary.dispatches,
                "state": self.summary.state,
                "cancelled": self.summary.cancelled,
                "budget_exhausted": self.summary.budget_exhausted,
                "elapsed_seconds": self.summary.elapsed_seconds,
                "budget_seconds": self.summary.budget_seconds,
                "reason": self.summary.reason,
            },
            "checkpoints": list(self.checkpoints),
            "resumption_cursor": self.resume.resumption_cursor if self.resume is not None else None,
        }
