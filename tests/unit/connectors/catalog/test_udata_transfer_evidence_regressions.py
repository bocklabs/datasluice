from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from datasluice.connectors.catalog.udata.wire import transfers
from tests.integration.connectors.catalog import test_udata_controlled as controlled


@pytest.mark.parametrize("cleanup_status", [204, 404, 500])
@pytest.mark.parametrize("primary_failure", [False, True])
def test_transfer_lifecycle_always_deletes_dataset_and_preserves_primary_error(
    monkeypatch: pytest.MonkeyPatch, cleanup_status: int, primary_failure: bool
) -> None:
    monkeypatch.setenv("UDATA_EVIDENCE_ADMIN_TOKEN", "admin")
    monkeypatch.setenv("UDATA_EVIDENCE_MEMBER_TOKEN", "member")
    calls: list[tuple[str, str]] = []
    primary = RuntimeError("lifecycle failed")

    def raw(token: str, method: str, path: str, **kwargs: Any) -> tuple[int, object, dict[str, str]]:
        calls.append((method, path))
        return cleanup_status, {}, {}

    def seed(token: str, suffix: str, identifiers: list[str]) -> str:
        identifiers.append("dataset")
        return "dataset"

    def sync_lifecycle(*args: Any) -> None:
        if primary_failure:
            raise primary

    async def async_lifecycle(*args: Any) -> None:
        return None

    monkeypatch.setattr(controlled, "_direct_request", raw)
    monkeypatch.setattr(controlled, "_seed_transfer_dataset", seed)
    monkeypatch.setattr(controlled, "_seed_member_id", lambda token: "member-id")
    monkeypatch.setattr(controlled, "_assert_absent_transfer_read", lambda *args: None)
    monkeypatch.setattr(controlled, "_exercise_sync_transfer_lifecycle", sync_lifecycle)
    monkeypatch.setattr(controlled, "_exercise_async_transfer_lifecycle", async_lifecycle)

    if primary_failure:
        with pytest.raises(RuntimeError) as raised:
            controlled.test_controlled_transfers_state_machine_matches_raw_routes_and_cleans_up()
        assert raised.value is primary
        if cleanup_status == 500:
            assert "cleanup failed" in primary.__notes__[0]
    elif cleanup_status == 500:
        with pytest.raises(ExceptionGroup, match="cleanup failed"):
            controlled.test_controlled_transfers_state_machine_matches_raw_routes_and_cleans_up()
    else:
        controlled.test_controlled_transfers_state_machine_matches_raw_routes_and_cleans_up()
    assert calls == [("DELETE", "/api/1/datasets/dataset/")]


def _raw_transfer_response(
    documents: dict[str, dict[str, str]], mismatch: str | None, method: str, path: str, **kwargs: Any
) -> tuple[int, object, dict[str, str]]:
    if method == "POST" and path == controlled._TRANSFERS_PATH:
        return 201, dict(documents["raw"]), {}
    if method == "POST":
        assert path == "/api/1/transfer/raw/"
        assert kwargs["body"] == {"response": "refuse"}
        if mismatch == "refusal":
            return 403, {}, {}
        if mismatch != "terminal":
            documents["raw"]["status"] = "refused"
        return 200, dict(documents["raw"]), {}
    if "?" in path:
        assert path == "/api/1/transfer/?subject=dataset"
        return 200, [dict(document) for document in documents.values()], {}
    identifier = path.rstrip("/").rsplit("/", 1)[-1]
    return 200, dict(documents[identifier]), {}


@pytest.mark.parametrize("mismatch", [None, "get", "list", "refusal", "terminal"])
def test_async_transfer_lifecycle_compares_reads_and_refuses_raw_twin(
    monkeypatch: pytest.MonkeyPatch, mismatch: str | None
) -> None:
    documents = {identifier: {"id": identifier, "status": "pending"} for identifier in ("typed", "raw")}
    calls: list[tuple[str, str]] = []
    typed_calls: list[str] = []

    def record(document: dict[str, str]) -> controlled.MappingRecord:
        return transfers.parse_transfer(document, operation="udata/api-v1.get-transfer")

    class Service:
        async def request_transfer(self, *args: Any) -> SimpleNamespace:
            return SimpleNamespace(transfer=record(documents["typed"]))

        async def get_transfer(self, identifier: str, *args: Any) -> controlled.MappingRecord:
            typed_calls.append("get")
            document = dict(documents[identifier])
            if mismatch == "get":
                document["status"] = "refused"
            return record(document)

        async def list_transfers(self, *args: Any) -> tuple[controlled.MappingRecord, ...]:
            typed_calls.append("list")
            identifiers = ("typed",) if mismatch == "list" else ("typed", "raw")
            return tuple(record(documents[identifier]) for identifier in identifiers)

        async def respond_to_transfer(self, identifier: str, *args: Any) -> SimpleNamespace:
            typed_calls.append("respond")
            documents[identifier]["status"] = "refused"
            return SimpleNamespace(transfer=record(documents[identifier]))

    @asynccontextmanager
    async def client(settings: Any) -> Any:
        yield SimpleNamespace(transfers=Service())

    def raw(token: str, method: str, path: str, **kwargs: Any) -> tuple[int, object, dict[str, str]]:
        calls.append((method, path))
        return _raw_transfer_response(documents, mismatch, method, path, **kwargs)

    monkeypatch.setattr(controlled, "create_async_client", client)
    monkeypatch.setattr(controlled, "_direct_request", raw)
    credential = controlled.UDataCredential(api_key="member")
    permissions = controlled.EffectivePermissions.for_credential(credential, platform=controlled.CatalogPlatform.UDATA)
    lifecycle = controlled._exercise_async_transfer_lifecycle(
        "admin", permissions, permissions, credential, "dataset", "member-id", "suffix"
    )
    if mismatch:
        with pytest.raises(AssertionError):
            asyncio.run(lifecycle)
    else:
        asyncio.run(lifecycle)
        assert typed_calls == ["get", "list", "respond"]
        assert calls == [
            ("POST", "/api/1/transfer/"),
            ("GET", "/api/1/transfer/typed/"),
            ("GET", "/api/1/transfer/raw/"),
            ("GET", "/api/1/transfer/?subject=dataset"),
            ("POST", "/api/1/transfer/raw/"),
            ("GET", "/api/1/transfer/typed/"),
            ("GET", "/api/1/transfer/raw/"),
        ]
