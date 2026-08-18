"""Public facade tracer from a direct locator to a materialized Artifact."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from datasluice import Artifact, DataSluice, DataSluiceError, DirectResourceLocator, resource_locator_from_dict
from datasluice.sync._identity import canonical_identity


def _fixture(name: str) -> dict[str, object]:
    return json.loads((Path("tests/fixtures/contracts") / name).read_text())


def test_public_direct_locator_materialization_tracer(tmp_path: Path) -> None:
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

    assert isinstance(artifact, Artifact)
    assert artifact.uri.endswith(f"/{canonical_identity(resource)}.parquet")
    assert artifact.content_digest.algorithm == "sha256"
    assert artifact.blob_digest.algorithm == "sha256"
    provenance = cast("dict[str, object]", payload["provenance"])
    source_locator = cast("dict[str, object]", provenance["source_locator"])
    assert str(source_locator["uri"]).endswith("api_key=***&page=1")
    assert "keep-external" not in json.dumps(payload)
    assert Artifact.from_dict(payload).to_dict() == payload
    assert opened.is_open is False
    with pytest.raises(DataSluiceError):
        opened.materialize("memory://tracer-output")


def test_schema_v1_golden_codecs_are_exact_and_strict() -> None:
    locator_fixture = _fixture("locator-v1.json")
    artifact_fixture = _fixture("artifact-v1.json")

    direct = DirectResourceLocator.from_dict(locator_fixture["direct"])

    assert direct.to_dict() == locator_fixture["direct"]
    assert Artifact.from_dict(artifact_fixture).to_dict() == artifact_fixture


def test_retired_catalog_locator_kind_is_rejected_by_the_public_factory() -> None:
    locator_fixture = _fixture("locator-v1.json")

    with pytest.raises(DataSluiceError):
        resource_locator_from_dict(locator_fixture["catalog"])


def test_direct_locator_rejects_embedded_credentials() -> None:
    with pytest.raises(DataSluiceError):
        DirectResourceLocator(uri="https://user:password@example.test/data.csv")
