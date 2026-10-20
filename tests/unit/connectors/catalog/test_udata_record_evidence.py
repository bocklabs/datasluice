from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

import pytest

from datasluice.connectors.catalog.udata import mapping
from datasluice.connectors.catalog.udata.wire import (
    activity_discussions,
    contact_visualization,
    dataservices,
    datasets,
    organizations,
    posts_reports,
    reuses,
    spatial,
    taxonomies,
    topics,
)
from datasluice.domain.catalog.redaction import MAX_METADATA_ENTRIES, TRUNCATED

if TYPE_CHECKING:
    from collections.abc import Callable

    from datasluice.domain.catalog.models import MappingRecord


@pytest.mark.parametrize(
    "parser",
    [
        activity_discussions.parse_mapping,
        posts_reports.parse_mapping,
        reuses.parse_mapping,
        spatial.parse_mapping,
        dataservices.parse_mapping,
        topics.parse_mapping,
        taxonomies.parse_mapping,
    ],
)
def test_family_records_preserve_runtime_data_but_exclude_all_values_from_evidence(
    parser: Callable[[object, str], MappingRecord],
) -> None:
    payload = {
        "id": "private-identifier",
        "name": "Private Person",
        "email": "private@example.test",
        "content": "private-discussion-or-notification",
        "url": "http://private-origin.example.test/",
        "locale": {"fr": "private-translated-value"},
        "extension-with-private-name": "unrecognized-private-value",
        "rows": [{"token": "private-credential"}] * 100,
    }
    record = parser(payload, "udata/test-evidence")

    evidence = json.dumps(record.to_evidence_dict())
    assert "private" not in evidence
    assert "object" in evidence
    assert "string" in evidence
    assert TRUNCATED in evidence
    assert record.payload["name"] == "Private Person"
    assert record.payload["email"] == "private@example.test"
    assert len(cast("tuple[object, ...]", record.payload["rows"])) == 100
    assert "private@example.test" not in json.dumps(record.to_dict())


@pytest.mark.parametrize(
    "parser", [contact_visualization.parse_contact_point, contact_visualization.parse_visualization]
)
def test_contact_and_visualization_evidence_excludes_names_contacts_and_private_origins(
    parser: Callable[[object], MappingRecord],
) -> None:
    record = parser(
        {
            "id": "private-identifier",
            "name": "Private Person",
            "title": "Private title",
            "email": "private@example.test",
            "url": "http://private-origin.example.test/",
        }
    )

    assert "private" not in json.dumps(record.to_evidence_dict()).lower()


def test_native_dataset_and_organization_evidence_excludes_payload_and_extensions() -> None:
    payload = {
        "id": "private-identifier",
        "name": "Private Person",
        "title": "Private title",
        "slug": "private-title",
        "custom_extension_name": "private-extension-value",
    }
    records = (
        mapping.parse_dataset_summary(payload),
        datasets.parse_dataset_detail(payload),
        organizations.parse_organization(payload),
    )

    for record in records:
        assert "private" not in json.dumps(record.to_evidence_dict()).lower()
        assert "custom_extension_name" not in json.dumps(record.to_evidence_dict())


def test_native_page_evidence_excludes_private_pagination_and_deployment_metadata() -> None:
    payload = {"id": "private-identifier", "title": "Private title", "long_text": "p" * 1024}
    page = mapping.parse_native_page(
        {
            "data": [payload] * 100,
            "page": 1,
            "next_page": "http://private-origin.example.test/?token=private-token",
        }
    )
    envelope = mapping.shape_dataset_page(page)

    evidence = envelope.to_evidence_dict()
    assert len(cast("list[object]", evidence["items"])) == MAX_METADATA_ENTRIES
    assert evidence["item_count"] == 100
    assert evidence["truncated"] is True
    assert "private" not in json.dumps(evidence)
    long_text = envelope.items[0].payload["long_text"]
    assert isinstance(long_text, str)
    assert len(long_text) == 256
    assert envelope.items[0].payload["title"] == "Private title"
