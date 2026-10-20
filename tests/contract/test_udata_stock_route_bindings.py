from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest

from datasluice.connectors.catalog.udata.models.harvest import (
    HARVEST_OPERATION,
    HarvestJobItemsQuery,
    HarvestJobQuery,
    HarvestScheduleInput,
    HarvestSourceInput,
    HarvestSourceQuery,
    HarvestValidationInput,
)
from datasluice.connectors.catalog.udata.wire import harvest, resources, root_profile
from scripts import check_udata_phase_gate as gate

ROOT = Path(__file__).resolve().parents[2]


def _bindings() -> dict[str, Any]:
    document = json.loads(gate.STOCK_BINDINGS_PATH.read_text(encoding="utf-8"))
    return {binding["capability_id"]: binding for binding in document["bindings"]}


def test_stock_root_bindings_preserve_fixture_routes_and_grouped_ownership() -> None:
    contract = json.loads(
        (ROOT / "src/datasluice/contracts/catalog/fixtures/udata/root_profile.json").read_text(encoding="utf-8")
    )
    bindings = _bindings()
    for row in contract["rows"]:
        binding = bindings[row["operation"]]
        assert binding["method"] == row["method"]
        assert binding["path"] == row["path"].replace("<format>", "<_format>")
        assert binding["operation_id"] == (
            root_profile.SET_SITE_OPERATION if row["method"] == "PATCH" else root_profile.ROOT_OPERATION
        )
    owned = {
        capability for capability, binding in bindings.items() if binding["operation_id"] == root_profile.ROOT_OPERATION
    }
    assert owned == {row["operation"] for row in contract["rows"] if row["method"] == "GET"}


def test_stock_harvest_bindings_preserve_all_grouped_wire_routes() -> None:
    source = HarvestSourceInput(name="source", url="https://example.org/feed", backend="csv")
    requests = {
        "list_harvest_sources": harvest.list_sources_request(HarvestSourceQuery()),
        "create_harvest_source": harvest.create_source_request(source),
        "get_harvest_source": harvest.get_source_request("source"),
        "update_harvest_source": harvest.update_source_request("source", source),
        "delete_harvest_source": harvest.delete_source_request("source"),
        "validate_harvest_source": harvest.validate_source_request("source", HarvestValidationInput("accepted")),
        "run_harvest_source": harvest.run_source_request("source"),
        "schedule_harvest_source": harvest.schedule_source_request("source", HarvestScheduleInput("0 6 * * *")),
        "unschedule_harvest_source": harvest.unschedule_source_request("source"),
        "preview_harvest_source_config": harvest.preview_source_config_request(source),
        "preview_harvest_source": harvest.preview_source_request("source"),
        "list_harvest_jobs": harvest.list_jobs_request("source", HarvestJobQuery()),
        "get_harvest_job": harvest.get_job_request("job"),
        "list_harvest_job_items": harvest.list_job_items_request("job", HarvestJobItemsQuery()),
        "harvest_backends": harvest.backends_request(),
    }
    bindings = _bindings()
    for name, (method, path, _, _) in requests.items():
        binding = bindings[f"udata.v1.{name}"]
        assert binding["operation_id"] == HARVEST_OPERATION
        assert binding["method"] == method
        expected_path = re.sub(r"<[^>]+>", lambda match: "job" if "ident" in match[0] else "source", binding["path"])
        assert expected_path == path.partition("?")[0]
    owned = {capability for capability, binding in bindings.items() if binding["operation_id"] == HARVEST_OPERATION}
    assert owned == {f"udata.v1.{name}" for name in requests}


def test_stock_resource_alias_does_not_duplicate_v2_dataset_route() -> None:
    document = json.loads(gate.STOCK_BINDINGS_PATH.read_text(encoding="utf-8"))
    assert resources.V2_DATASET_GET_OPERATION in document["non_route_operations"]
    assert _bindings()["udata.v2.get_dataset"]["operation_id"] == "udata/api-v2.get-dataset"
    assert not any(binding["operation_id"] == resources.V2_DATASET_GET_OPERATION for binding in document["bindings"])


def test_repository_stock_bijection_matches_approved_captures() -> None:
    if not gate.COVERAGE_PATH.exists():
        pytest.skip("the approved captures require the local-only .planning tree")
    gate.require_stock_bijection()
