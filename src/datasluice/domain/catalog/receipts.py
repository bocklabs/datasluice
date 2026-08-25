"""Immutable mutation and bulk execution receipts for catalog contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from datasluice.domain.catalog.ids import CatalogId
from datasluice.domain.catalog.models import _contract_error, _freeze_json, _object_dict, _thaw_json
from datasluice.domain.catalog.redaction import contains_credential_content, redact_string

_ATOMICITIES = frozenset({"atomic", "independent"})
_OUTCOMES = frozenset({"succeeded", "failed", "cancelled", "skipped"})
_SENSITIVE_KEY_PARTS = (
    "authorization",
    "credential",
    "token",
    "secret",
    "password",
    "passwd",
    "pwd",
    "cookie",
    "api_key",
    "apikey",
    "private_key",
    "access_key",
    "consumer_key",
    "client_key",
    "signature",
    "header",
    "body",
)


def _contains_credential_value(value: str) -> bool:
    return contains_credential_content(value)


def _required_text(value: object, path: str) -> str:
    if not isinstance(value, str) or not value:
        raise _contract_error(path)
    return value


def _optional_text(value: object, path: str) -> str | None:
    if value is not None and not isinstance(value, str):
        raise _contract_error(path)
    return value


def _validate_redacted_metadata(value: object, path: str) -> object:
    if isinstance(value, str) and _contains_credential_value(value) and redact_string(value) != value:
        raise _contract_error(path)
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if not isinstance(key, str):
                raise _contract_error(path)
            normalized_key = key.lower().replace("-", "_")
            if any(part in normalized_key for part in _SENSITIVE_KEY_PARTS):
                raise _contract_error(path)
            _validate_redacted_metadata(nested, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for nested in value:
            _validate_redacted_metadata(nested, path)
    return _freeze_json(value, path)


def _freeze_audit_metadata(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise _contract_error("mutation_receipt.audit_metadata")
    frozen = _validate_redacted_metadata(value, "mutation_receipt.audit_metadata")
    if not isinstance(frozen, Mapping):
        raise _contract_error("mutation_receipt.audit_metadata")
    return frozen


@dataclass(frozen=True)
class MutationReceipt:
    """An immutable, redacted receipt for one catalog mutation."""

    operation: str
    outcome: str
    target: CatalogId
    version_token: str | None = None
    request_id: str | None = None
    atomicity: str = "independent"
    operation_atomicity: str = "independent"
    audit_metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _required_text(self.operation, "mutation_receipt.operation")
        if self.outcome not in _OUTCOMES:
            raise _contract_error("mutation_receipt.outcome")
        if not isinstance(self.target, CatalogId):
            raise _contract_error("mutation_receipt.target")
        _optional_text(self.version_token, "mutation_receipt.version_token")
        _optional_text(self.request_id, "mutation_receipt.request_id")
        if self.atomicity not in _ATOMICITIES or self.operation_atomicity not in _ATOMICITIES:
            raise _contract_error("mutation_receipt.atomicity")
        if self.atomicity == "atomic" and self.operation_atomicity != "atomic":
            raise _contract_error("mutation_receipt.atomicity")
        object.__setattr__(self, "audit_metadata", _freeze_audit_metadata(self.audit_metadata))

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe redacted mutation receipt envelope."""
        return {
            "schema_version": 1,
            "kind": "mutation_receipt",
            "operation": self.operation,
            "outcome": self.outcome,
            "target": self.target.to_dict(),
            "version_token": self.version_token,
            "request_id": self.request_id,
            "atomicity": self.atomicity,
            "operation_atomicity": self.operation_atomicity,
            "audit_metadata": _thaw_json(self.audit_metadata),
        }

    @classmethod
    def from_dict(cls, value: object) -> MutationReceipt:
        """Decode one strict schema-v1 mutation receipt envelope."""
        data = _object_dict(value, "mutation_receipt")
        if set(data) != {
            "schema_version",
            "kind",
            "operation",
            "outcome",
            "target",
            "version_token",
            "request_id",
            "atomicity",
            "operation_atomicity",
            "audit_metadata",
        }:
            raise _contract_error("mutation_receipt")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "mutation_receipt":
            raise _contract_error("mutation_receipt")
        return cls(
            operation=_required_text(data["operation"], "mutation_receipt.operation"),
            outcome=_required_text(data["outcome"], "mutation_receipt.outcome"),
            target=CatalogId.from_dict(data["target"]),
            version_token=_optional_text(data["version_token"], "mutation_receipt.version_token"),
            request_id=_optional_text(data["request_id"], "mutation_receipt.request_id"),
            atomicity=_required_text(data["atomicity"], "mutation_receipt.atomicity"),
            operation_atomicity=_required_text(data["operation_atomicity"], "mutation_receipt.operation_atomicity"),
            audit_metadata=_object_dict(data["audit_metadata"], "mutation_receipt.audit_metadata"),
        )


@dataclass(frozen=True)
class BulkPlan:
    """An ordered immutable plan for a bounded set of catalog mutations."""

    operation: str
    items: tuple[CatalogId, ...]
    preview: bool = False
    atomicity: str = "independent"
    cancellation_requested: bool = False
    resumption_cursor: str | None = None

    def __post_init__(self) -> None:
        _required_text(self.operation, "bulk_plan.operation")
        if not isinstance(self.items, tuple):
            object.__setattr__(self, "items", tuple(self.items))
        if not all(isinstance(item, CatalogId) for item in self.items):
            raise _contract_error("bulk_plan.items")
        if type(self.preview) is not bool or type(self.cancellation_requested) is not bool:
            raise _contract_error("bulk_plan")
        if self.atomicity not in _ATOMICITIES:
            raise _contract_error("bulk_plan.atomicity")
        _optional_text(self.resumption_cursor, "bulk_plan.resumption_cursor")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe bulk plan envelope preserving item order."""
        return {
            "schema_version": 1,
            "kind": "bulk_plan",
            "operation": self.operation,
            "items": [item.to_dict() for item in self.items],
            "preview": self.preview,
            "atomicity": self.atomicity,
            "cancellation_requested": self.cancellation_requested,
            "resumption_cursor": self.resumption_cursor,
        }

    @classmethod
    def from_dict(cls, value: object) -> BulkPlan:
        """Decode one strict schema-v1 bulk plan envelope."""
        data = _object_dict(value, "bulk_plan")
        if set(data) != {
            "schema_version",
            "kind",
            "operation",
            "items",
            "preview",
            "atomicity",
            "cancellation_requested",
            "resumption_cursor",
        }:
            raise _contract_error("bulk_plan")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "bulk_plan":
            raise _contract_error("bulk_plan")
        items = data["items"]
        if (
            not isinstance(items, list)
            or type(data["preview"]) is not bool
            or type(data["cancellation_requested"]) is not bool
        ):
            raise _contract_error("bulk_plan")
        return cls(
            operation=_required_text(data["operation"], "bulk_plan.operation"),
            items=tuple(CatalogId.from_dict(item) for item in items),
            preview=data["preview"],
            atomicity=_required_text(data["atomicity"], "bulk_plan.atomicity"),
            cancellation_requested=data["cancellation_requested"],
            resumption_cursor=_optional_text(data["resumption_cursor"], "bulk_plan.resumption_cursor"),
        )


@dataclass(frozen=True)
class BulkItemReceipt:
    """One ordered mutation outcome recorded during a bulk plan."""

    index: int
    receipt: MutationReceipt

    def __post_init__(self) -> None:
        if type(self.index) is not int or self.index < 0 or not isinstance(self.receipt, MutationReceipt):
            raise _contract_error("bulk_item_receipt")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe per-item receipt envelope."""
        return {
            "schema_version": 1,
            "kind": "bulk_item_receipt",
            "index": self.index,
            "receipt": self.receipt.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> BulkItemReceipt:
        """Decode one strict schema-v1 per-item receipt envelope."""
        data = _object_dict(value, "bulk_item_receipt")
        if set(data) != {"schema_version", "kind", "index", "receipt"}:
            raise _contract_error("bulk_item_receipt")
        if (
            data["schema_version"] != 1
            or type(data["schema_version"]) is not int
            or data["kind"] != "bulk_item_receipt"
        ):
            raise _contract_error("bulk_item_receipt")
        index = data["index"]
        if type(index) is not int:
            raise _contract_error("bulk_item_receipt.index")
        return cls(index=index, receipt=MutationReceipt.from_dict(data["receipt"]))


@dataclass(frozen=True)
class BulkCheckpoint:
    """A resumable immutable record of one partially executed bulk plan."""

    plan: BulkPlan
    item_receipts: tuple[BulkItemReceipt, ...] = ()
    cancellation_requested: bool = False
    resumption_cursor: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.plan, BulkPlan):
            raise _contract_error("bulk_checkpoint.plan")
        if not isinstance(self.item_receipts, tuple):
            object.__setattr__(self, "item_receipts", tuple(self.item_receipts))
        if not all(isinstance(receipt, BulkItemReceipt) for receipt in self.item_receipts):
            raise _contract_error("bulk_checkpoint.item_receipts")
        indices = tuple(receipt.index for receipt in self.item_receipts)
        if (
            indices != tuple(sorted(indices))
            or len(set(indices)) != len(indices)
            or any(index >= len(self.plan.items) for index in indices)
        ):
            raise _contract_error("bulk_checkpoint.item_receipts")
        if type(self.cancellation_requested) is not bool:
            raise _contract_error("bulk_checkpoint.cancellation_requested")
        _optional_text(self.resumption_cursor, "bulk_checkpoint.resumption_cursor")

    def to_dict(self) -> dict[str, object]:
        """Return a fresh JSON-safe resumable bulk checkpoint envelope."""
        return {
            "schema_version": 1,
            "kind": "bulk_checkpoint",
            "plan": self.plan.to_dict(),
            "item_receipts": [receipt.to_dict() for receipt in self.item_receipts],
            "cancellation_requested": self.cancellation_requested,
            "resumption_cursor": self.resumption_cursor,
        }

    @classmethod
    def from_dict(cls, value: object) -> BulkCheckpoint:
        """Decode one strict schema-v1 resumable bulk checkpoint envelope."""
        data = _object_dict(value, "bulk_checkpoint")
        if set(data) != {
            "schema_version",
            "kind",
            "plan",
            "item_receipts",
            "cancellation_requested",
            "resumption_cursor",
        }:
            raise _contract_error("bulk_checkpoint")
        if data["schema_version"] != 1 or type(data["schema_version"]) is not int or data["kind"] != "bulk_checkpoint":
            raise _contract_error("bulk_checkpoint")
        item_receipts = data["item_receipts"]
        if not isinstance(item_receipts, list) or type(data["cancellation_requested"]) is not bool:
            raise _contract_error("bulk_checkpoint")
        return cls(
            plan=BulkPlan.from_dict(data["plan"]),
            item_receipts=tuple(BulkItemReceipt.from_dict(receipt) for receipt in item_receipts),
            cancellation_requested=data["cancellation_requested"],
            resumption_cursor=_optional_text(data["resumption_cursor"], "bulk_checkpoint.resumption_cursor"),
        )
