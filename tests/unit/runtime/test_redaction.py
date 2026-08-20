"""Tests for catalog output redaction primitives and routing."""

from __future__ import annotations

import pytest

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.exceptions import DataSluiceError
from datasluice.runtime.redaction import redact_event_metadata, redact_for_output

_redaction = pytest.importorskip("datasluice.domain.catalog.redaction")
contains_credential_content = _redaction.contains_credential_content
redact_mapping = _redaction.redact_mapping
redact_string = _redaction.redact_string


def _receipt(metadata: dict[str, object]) -> MutationReceipt:
    return MutationReceipt(
        operation="datasets.update",
        outcome="succeeded",
        target=CatalogId(CatalogPlatform.CKAN, ResourceKind.DATASET, "weather"),
        audit_metadata=metadata,
    )


def test_credential_shaped_strings_are_detected_and_redacted() -> None:
    bearer = "authorization: Bearer aBcDeFgH1234"
    query = "https://portal.example/data?access_token=aBcDeFgH1234;signature=abc123"

    assert contains_credential_content(bearer)
    assert redact_string(bearer) == "authorization: Bearer ***"
    assert contains_credential_content(query)
    assert redact_string(query) == "https://portal.example/data?access_token=***;signature=***"


def test_mapping_recurses_into_benign_keys_without_changing_safe_strings() -> None:
    metadata = {"status": "ready", "details": {"response": {"value": "Bearer aBcDeFgH1234"}}}

    assert redact_mapping(metadata) == {
        "status": "ready",
        "details": {"response": {"value": "Bearer ***"}},
    }


def test_receipt_rejects_benign_key_with_bearer_value_and_accepts_scrubbed_twin() -> None:
    with pytest.raises(DataSluiceError):
        _receipt({"details": {"response": {"value": "Bearer aBcDeFgH1234"}}})

    receipt = _receipt({"details": {"response": {"value": "Bearer ***"}}})
    assert MutationReceipt.from_dict(receipt.to_dict()) == receipt


def test_redaction_bounds_recursion_and_metadata_entries() -> None:
    nested: dict[str, object] = {"value": "Bearer aBcDeFgH1234"}
    for index in range(10):
        nested = {f"level_{index}": nested}
    oversized = {f"entry_{index}": "x" * 300 for index in range(33)}

    redacted_nested = redact_mapping(nested)
    redacted_oversized = redact_mapping(oversized)

    assert "aBcDeFgH1234" not in repr(redacted_nested)
    assert len(redacted_oversized) == 32
    assert "[TRUNCATED]" in redacted_oversized.values()
    assert all(len(value) <= 256 for value in redacted_oversized.values() if isinstance(value, str))


def test_runtime_gate_redacts_values_and_has_one_escape_hatch(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "Bearer aBcDeFgH1234"

    assert redact_for_output(secret) == "Bearer ***"
    assert redact_event_metadata({"message": secret}) == {"message": "Bearer ***"}

    monkeypatch.setenv("DATASLUICE_NO_REDACT", "1")
    assert redact_for_output(secret) == secret
