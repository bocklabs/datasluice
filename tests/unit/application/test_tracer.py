"""Public facade tracer from direct locator to materialized Artifact."""

from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
from typing import Any

import pytest

from datasluice.sync._identity import canonical_identity

_PUBLIC_CONTRACTS = ("Artifact", "CatalogResourceLocator", "DataSluice", "DirectResourceLocator", "ResourceLocator")
datasluice = importlib.import_module("datasluice")
if not all(hasattr(datasluice, name) for name in _PUBLIC_CONTRACTS):
    if os.environ.get("DATASLUICE_TDD_RED") == "1":
        missing = sorted(name for name in _PUBLIC_CONTRACTS if not hasattr(datasluice, name))
        raise AssertionError(f"missing public Phase 8 contracts: {missing}")
    pytest.skip("public Phase 8 contracts pending GREEN phase", allow_module_level=True)


def _public_contract(name: str) -> Any:
    return getattr(datasluice, name)


Artifact = _public_contract("Artifact")
CatalogResourceLocator = _public_contract("CatalogResourceLocator")
DataSluice = _public_contract("DataSluice")
DataSluiceError = _public_contract("DataSluiceError")
DirectResourceLocator = _public_contract("DirectResourceLocator")
ResourceLocator = _public_contract("ResourceLocator")


def _fixture(name: str) -> dict[str, object]:
    return json.loads((Path(__file__).parents[2] / "fixtures" / "contracts" / name).read_text())


def test_public_direct_locator_materialization_tracer(tmp_path) -> None:
    source = tmp_path / "observations.csv"
    source.write_text("id,name\n1,Ada\n2,Grace\n")
    locator = DirectResourceLocator(
        uri=f"{source.as_uri()}?api_key=keep-external&page=1",
        extensions={"org.datasluice.contract": {"fixture": "runtime"}},
    )
    data_sluice = DataSluice()

    opened = data_sluice.open(locator)

    assert opened.is_open is False
    resource = data_sluice.resolve(locator)
    artifact = opened.materialize("memory://tracer-output")
    payload = artifact.to_dict()

    assert artifact.uri.endswith(f"/{canonical_identity(resource)}.parquet")
    assert artifact.content_digest.algorithm == "sha256"
    assert artifact.blob_digest.algorithm == "sha256"
    assert payload["provenance"]["source_locator"]["uri"].endswith("api_key=***&page=1")
    assert "keep-external" not in json.dumps(payload)
    assert Artifact.from_dict(payload).to_dict() == payload
    assert opened.is_open is False
    with pytest.raises(DataSluiceError):
        opened.materialize("memory://tracer-output")


def test_schema_v1_golden_codecs_are_exact_and_strict() -> None:
    locator_fixture = _fixture("locator-v1.json")
    artifact_fixture = _fixture("artifact-v1.json")

    direct = DirectResourceLocator.from_dict(locator_fixture["direct"])
    catalog = CatalogResourceLocator.from_dict(locator_fixture["catalog"])

    assert direct.to_dict() == locator_fixture["direct"]
    assert catalog.to_dict() == locator_fixture["catalog"]
    assert "ResourceLocator" in datasluice.__all__
    assert Artifact.from_dict(artifact_fixture).to_dict() == artifact_fixture

    with pytest.raises(DataSluiceError):
        DirectResourceLocator(uri="https://user:password@example.test/data.csv")
    with pytest.raises(DataSluiceError):
        Artifact.from_dict({**artifact_fixture, "unexpected": "value"})
