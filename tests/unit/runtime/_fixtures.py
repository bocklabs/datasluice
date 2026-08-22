"""Shared runtime test fixtures for client and OAuth suites."""

from __future__ import annotations

import json
from datetime import date

from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.operations import (
    Atomicity,
    AuthClass,
    CapabilityClass,
    ConcurrencyRequirement,
    Idempotency,
    MutationClass,
    OperationId,
    OperationSpec,
    OperationTier,
)
from datasluice.domain.catalog.profiles import DeclaredCapabilityProfile


def _profile() -> DeclaredCapabilityProfile:
    operation = OperationId("reference", "datasets", "get")
    return DeclaredCapabilityProfile(
        profile_version="v1",
        schema_version="v1",
        platform_api_version="v1",
        official_source_uri="https://example.test/source",
        source_accessed_at=date(2026, 8, 20),
        fixture_fingerprint="fixture-v1",
        operations={
            operation: OperationSpec(
                id=operation,
                tier=OperationTier.NORMALIZED,
                request_type="DatasetRequest",
                response_type="DatasetRecord",
                auth_class=AuthClass.PUBLIC,
                mutation_class=MutationClass.READ,
                idempotency=Idempotency.SAFE,
                concurrency=ConcurrencyRequirement.NONE,
                atomicity=Atomicity.NONE,
                capability_class=CapabilityClass.CORE,
            )
        },
    )


def _request(operation: OperationId | None = None) -> CatalogOperationRequest:
    return CatalogOperationRequest(
        operation or OperationId("reference", "datasets", "get"), {"url": "http://127.0.0.1:8000/datasets/fixture"}
    )


def _guard(operation: OperationId | None = None) -> CatalogOperationGuard:
    return CatalogOperationGuard(operation_id=operation or OperationId("reference", "datasets", "get"))


def _envelope() -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "kind": "result_envelope",
            "items": [
                {
                    "schema_version": 1,
                    "kind": "dataset",
                    "id": {
                        "schema_version": 1,
                        "kind": "catalog_id",
                        "platform": "reference",
                        "resource_kind": "dataset",
                        "value": "fixture",
                    },
                    "name": "Fixture dataset",
                    "description": None,
                    "extensions": {},
                }
            ],
            "page": None,
            "warnings": [],
            "platform": None,
        }
    ).encode()
