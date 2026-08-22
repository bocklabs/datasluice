"""Tests for catalog output redaction primitives, routing, and log filtering."""

from __future__ import annotations

import hashlib
import logging
import sys
from collections.abc import Mapping
from typing import cast

import pytest

from datasluice.domain.catalog.ids import CatalogId, CatalogPlatform, ResourceKind
from datasluice.domain.catalog.receipts import MutationReceipt
from datasluice.domain.catalog.redaction import (
    contains_credential_content,
    redact_mapping,
    redact_string,
)
from datasluice.exceptions import DataSluiceError
from datasluice.logging import RedactingFilter
from datasluice.runtime.redaction import redact_event_metadata, redact_for_output


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


def test_sensitive_query_keywords_share_one_source_with_key_redaction() -> None:
    query = "?access_key=AKIA1234567890&pwd=hunter2&cookie=session-cookie&client_secret=s3cr3t-value"

    assert contains_credential_content(query)
    assert redact_string(query) == "?access_key=***&pwd=***&cookie=***&client_secret=***"


def test_userinfo_credentials_are_masked_in_both_directions() -> None:
    uri = "https://user:secret-value@portal.example/dataset"

    assert contains_credential_content(uri)
    assert redact_string(uri) == "https://user:***@portal.example/dataset"
    scrubbed = "https://user:***@portal.example/dataset"
    assert contains_credential_content(scrubbed)
    assert redact_string(scrubbed) == scrubbed


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


def test_receipt_rejects_basic_and_key_value_shapes_under_benign_keys() -> None:
    with pytest.raises(DataSluiceError):
        _receipt({"summary": "Basic aBcDeFgH1234"})
    with pytest.raises(DataSluiceError):
        _receipt({"meta": {"nested": {"note": "access_token=aBcDeFgH1234"}}})


def test_receipt_accepts_its_own_scrubbed_query_output() -> None:
    receipt = _receipt({"url": "https://portal.example/data?access_token=***"})

    assert MutationReceipt.from_dict(receipt.to_dict()) == receipt


def test_round_trip_rejects_mutated_audit_metadata() -> None:
    receipt = _receipt({"details": {"response": {"value": "Bearer ***"}}})
    payload = receipt.to_dict()
    audit_metadata = cast(dict[str, object], payload["audit_metadata"])
    details = cast(dict[str, object], audit_metadata["details"])
    response = cast(dict[str, object], details["response"])
    response["value"] = "Bearer aBcDeFgH1234"

    with pytest.raises(DataSluiceError):
        MutationReceipt.from_dict(payload)


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


def test_redaction_limits_are_exact_boundaries() -> None:
    exact_entries = {f"entry_{index}": "x" * 300 for index in range(32)}
    redacted_exact = redact_mapping(exact_entries)

    assert len(redacted_exact) == 32
    assert "[TRUNCATED]" not in redacted_exact.keys()
    assert "[TRUNCATED]" not in redacted_exact.values()

    max_text = "a" * 256
    assert redact_string(max_text) == max_text

    deep: dict[str, object] = {"value": 1}
    for index in range(7):
        deep = {f"level_{index}": deep}
    assert cast(Mapping[str, object], redact_mapping(deep)) == deep

    digest = hashlib.sha256(b"fixture").hexdigest()
    assert redact_string(digest) == digest
    assert not contains_credential_content(digest)


def test_sequence_values_are_normalized_to_tuples() -> None:
    redacted = redact_mapping({"notes": ["Bearer aBcDeFgH1234", 1]})

    assert redacted["notes"] == ("Bearer ***", 1)


def test_non_string_keys_are_skipped_without_raising() -> None:
    mixed = cast(Mapping[str, object], {1: "Bearer aBcDeFgH1234", "": "empty-key", "ok": "fine"})

    assert redact_mapping(mixed) == {"ok": "fine"}


class _ExplodingRepr:
    def __repr__(self) -> str:
        raise RuntimeError("repr failed")


def test_unknown_objects_pass_through_the_gate_and_render_inside_mappings() -> None:
    instance = _ExplodingRepr()

    assert redact_for_output(instance) is instance
    assert redact_mapping({"detail": instance}) == {"detail": "[TRUNCATED]"}


def test_bytes_are_decoded_and_scrubbed_by_the_gate() -> None:
    assert redact_for_output(b"Bearer aBcDeFgH1234") == "Bearer ***"


def test_runtime_gate_redacts_values_and_has_one_escape_hatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATASLUICE_NO_REDACT", raising=False)
    secret = "Bearer aBcDeFgH1234"

    assert redact_for_output(secret) == "Bearer ***"
    assert redact_event_metadata({"message": secret}) == {"message": "Bearer ***"}

    monkeypatch.setenv("DATASLUICE_NO_REDACT", "1")
    assert redact_for_output(secret) == secret
    assert redact_event_metadata({"message": secret}) == {"message": secret}


def test_redacting_filter_never_touches_log_record_internals() -> None:
    try:
        raise ValueError("boom")
    except ValueError:
        exc_info = sys.exc_info()
    record = logging.LogRecord(
        "datasluice",
        logging.ERROR,
        __file__,
        11,
        "failed %s after %s",
        ({"authorization": "secret-value"}, 2),
        exc_info,
    )
    record.__dict__["api_token"] = "super-secret-value"
    record.__dict__["custom_note"] = "api_key=raw-credential-value"

    assert RedactingFilter().filter(record)

    formatter = logging.Formatter()
    assert record.exc_info == exc_info
    assert record.exc_info is not None
    assert "ValueError: boom" in formatter.formatException(record.exc_info)
    assert record.msg == "failed %s after %s"
    assert record.pathname == __file__
    assert record.lineno == 11
    assert record.args == ({"authorization": "***"}, 2)
    assert record.__dict__["api_token"] == "***"
    assert record.__dict__["custom_note"] == "api_key=***"


def test_redacting_filter_survives_hostile_payloads_without_raising() -> None:
    hostile: Mapping[str, object] = cast(Mapping[str, object], {1: "Bearer aBcDeFgH1234"})
    record = logging.LogRecord(
        "datasluice",
        logging.DEBUG,
        __file__,
        1,
        "state %s and %s",
        (hostile, _ExplodingRepr()),
        None,
    )

    assert RedactingFilter().filter(record)

    args = cast(tuple[object, ...], record.args)
    assert args[0] == {}
    assert isinstance(args[1], _ExplodingRepr)
