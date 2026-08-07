"""Unit tests for the Socrata adapter mapper."""

from __future__ import annotations

from datasluice.connectors.socrata.mapper import map_resource


def test_map_resource_reads_format_from_view_type() -> None:
    view = {"id": "abcd-1234", "name": "My Dataset", "type": "dataset", "url": "https://data.example.test/d/abcd-1234"}
    resource = map_resource(view)
    assert resource.format is not None


def test_map_resource_format_none_when_type_absent() -> None:
    view = {"id": "abcd-1234", "name": "No Type"}
    resource = map_resource(view)
    assert resource.format is None
