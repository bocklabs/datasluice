from __future__ import annotations

import json
from dataclasses import replace
from typing import cast

import pytest

from datasluice.connectors.catalog.udata import mapping
from datasluice.connectors.catalog.udata.wire import organizations, users
from datasluice.domain.catalog.ids import ResourceKind
from datasluice.domain.catalog.models import MappingRecord, NativeRecord
from tests.integration.connectors.catalog import test_udata_controlled as controlled


def _raw_item() -> dict[str, object]:
    return {
        "id": "record-1",
        "description": "public description " * 40,
        "token": "private-token",
        "nested": {"url": "https://example.test/?api_key=private-key"},
        "rows": [{"value": index} for index in range(64)],
        **{f"field_{index}": index for index in range(64)},
    }


def _reject_retained_serializer(record: object) -> dict[str, object]:
    raise AssertionError("Differential comparisons must use runtime mappings.")


def test_controlled_page_differential_uses_complete_runtime_fields_and_existing_redaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = {"data": [_raw_item()], "page": 1, "page_size": 20, "total": 1}
    page = organizations.parse_page(raw, operation="udata/test-read", kind=ResourceKind.DATASET)
    monkeypatch.setattr(NativeRecord, "to_dict", _reject_retained_serializer)

    controlled._assert_page_matches_raw("list_organization_datasets", raw, page)
    controlled._assert_page_read_matches_raw("my_org_topics", raw, page)
    assert page.items[0].payload["field_63"] == 63
    assert page.items[0].payload["token"] == "***"
    assert "private-token" not in json.dumps(page.to_evidence_dict())


@pytest.mark.parametrize("enveloped", [False, True])
def test_controlled_sequence_differential_uses_runtime_fields_without_retained_serialization(
    enveloped: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw: object = {"data": [_raw_item()]} if enveloped else [_raw_item()]
    records = users.parse_mapping_list(raw, operation="udata/test-read", kind="reuse")
    monkeypatch.setattr(MappingRecord, "to_dict", _reject_retained_serializer)

    controlled._assert_sequence_read_matches_raw("my_reuses", raw, records)
    controlled._assert_records_match_raw("list_organization_reuses", raw, records)
    assert records[0].payload["field_63"] == 63


@pytest.mark.parametrize("change", ["missing_field", "changed_field", "missing_row", "changed_row"])
def test_controlled_differential_still_checks_fields_and_values_beyond_retained_limits(change: str) -> None:
    raw = [_raw_item()]
    record = users.parse_mapping_list(raw, operation="udata/test-read", kind="reuse")[0]
    payload = dict(record.payload)
    if change == "missing_field":
        payload.pop("field_63")
    elif change == "changed_field":
        payload["field_63"] = 100
    else:
        rows = list(cast("list[object]", payload["rows"]))
        if change == "missing_row":
            rows.pop()
        else:
            rows[-1] = {"value": 100}
        payload["rows"] = rows
    changed = replace(record, payload=payload)

    with pytest.raises(AssertionError):
        controlled._assert_sequence_read_matches_raw("my_reuses", raw, (changed,))


def test_controlled_page_differential_still_checks_pagination() -> None:
    raw = {"data": [_raw_item()], "page": 1, "page_size": 20, "total": 1}
    page = organizations.parse_page(raw, operation="udata/test-read", kind=ResourceKind.DATASET)
    assert page.platform is not None
    mismatched = mapping.UDataPageEnvelope(
        items=page.items,
        page=page.page,
        platform=page.platform,
        native_page=replace(page.native_page, total=2),
    )

    with pytest.raises(AssertionError):
        controlled._assert_page_matches_raw("list_organization_datasets", raw, mismatched)
