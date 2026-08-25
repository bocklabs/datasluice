"""Deterministic loopback coverage for the exhaustive CKAN vocabulary/license surface."""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest

from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient, declared_ckan_profile
from datasluice.connectors.catalog.ckan.inventory import CKAN_ACTIONS
from datasluice.connectors.catalog.ckan.mapping import RECORD_KINDS, RESULT_KINDS
from datasluice.connectors.catalog.ckan.results import CKANMutationResult
from datasluice.connectors.catalog.ckan.services.vocabularies_licenses import (
    AsyncVocabulariesLicensesService,
    SyncVocabulariesLicensesService,
    TranslationBatch,
)
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.models import MappingRecord, NativeRecord, ValueRecord
from datasluice.domain.catalog.operations import OperationId
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.transport.base import RuntimeRequest, RuntimeResponse

LOOPBACK_ORIGIN = "http://127.0.0.1:9001"
VOCAB_READ_ID = "ckan/action-api-v3.tags-vocabularies-licenses-list-show"
VOCAB_WRITE_ID = "ckan/action-api-v3.tags-vocabularies-licenses-create-update-delete"

EXPECTED_VOCABULARY_ACTIONS = frozenset(
    {
        "tag_list",
        "tag_show",
        "tag_search",
        "tag_autocomplete",
        "tag_create",
        "tag_delete",
        "vocabulary_list",
        "vocabulary_show",
        "vocabulary_create",
        "vocabulary_update",
        "vocabulary_delete",
        "license_list",
        "format_autocomplete",
        "term_translation_show",
        "term_translation_update",
        "term_translation_update_many",
    }
)

TAG_RESULT: dict[str, object] = {"id": "tag-1", "name": "health"}
VOCABULARY_RESULT: dict[str, object] = {"id": "vocab-1", "name": "genres", "tags": [TAG_RESULT]}
LICENSE_ROW: dict[str, object] = {"id": "cc-by", "title": "CC BY 4.0", "url": "https://creativecommons.org"}


def _success_body(result: object) -> bytes:
    return json.dumps({"success": True, "result": result}).encode("utf-8")


class SyncCaptureTransport:
    """A deterministic loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    def close(self) -> None:
        self.close_count += 1


class AsyncCaptureTransport:
    """A deterministic async loopback capture transport recording every sent request."""

    def __init__(self, *, status_code: int = 200, body: bytes = b"{}") -> None:
        self.status_code = status_code
        self.body = body
        self.requests: list[RuntimeRequest] = []
        self.close_count = 0

    async def send(self, request: RuntimeRequest) -> RuntimeResponse:
        self.requests.append(request)
        return RuntimeResponse(
            status_code=self.status_code,
            headers={"Content-Type": "application/json"},
            body=self.body,
        )

    async def aclose(self) -> None:
        self.close_count += 1


def _client(transport: SyncCaptureTransport) -> SyncCKANClient:
    return SyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
    )


def _async_client(transport: AsyncCaptureTransport) -> AsyncCKANClient:
    return AsyncCKANClient(
        transport,
        declared_ckan_profile(),
        origin=LOOPBACK_ORIGIN,
        owns_transport=False,
    )


def test_vocabulary_manifest_holds_exactly_the_documented_sixteen_actions() -> None:
    """Manifest-driven completeness: exact name set across the split read/write ids."""
    entries = [entry for entry in CKAN_ACTIONS.entries if entry.group == "vocabularies_licenses"]
    assert {entry.name for entry in entries} == EXPECTED_VOCABULARY_ACTIONS
    assert len(entries) == 16
    assert {entry.owning_operation_id for entry in entries} == {VOCAB_READ_ID, VOCAB_WRITE_ID}
    reads = [entry for entry in entries if entry.mutation_class == "read"]
    standards = [entry for entry in entries if entry.mutation_class == "standard"]
    destructive = [entry for entry in entries if entry.mutation_class == "destructive"]
    assert len(reads) == 9
    assert len(standards) == 7
    assert len(destructive) == 0
    for entry in entries:
        spec = RESULT_KINDS.get(entry.name)
        assert spec is not None, f"{entry.name} is absent from the mapping truth table"
        outcome, family = spec
        assert outcome == entry.result_kind
        if family is not None:
            assert family in RECORD_KINDS


def test_every_manifest_vocabulary_action_exposes_a_typed_method_on_both_mode_services() -> None:
    """Each registered vocabulary action names a callable member on both projections."""
    sync_surface = {name for name in dir(SyncVocabulariesLicensesService) if not name.startswith("_")}
    async_surface = {name for name in dir(AsyncVocabulariesLicensesService) if not name.startswith("_")}
    for action in EXPECTED_VOCABULARY_ACTIONS:
        assert action in sync_surface, f"sync surface misses {action}"
        assert action in async_surface, f"async surface misses {action}"


def test_vocabulary_surfaces_stay_in_structural_lockstep_across_modes() -> None:
    """Sync/async projections expose identical members with mode-correct dispatch."""
    sync_members = {name for name in dir(SyncVocabulariesLicensesService) if not name.startswith("__")}
    async_members = {name for name in dir(AsyncVocabulariesLicensesService) if not name.startswith("__")}
    assert sync_members == async_members
    public = {name for name in vars(SyncVocabulariesLicensesService) if not name.startswith("_")}
    for name in public:
        assert inspect.iscoroutinefunction(getattr(AsyncVocabulariesLicensesService, name)), name
        assert not inspect.iscoroutinefunction(getattr(SyncVocabulariesLicensesService, name)), name


def test_tag_reads_pass_q_and_vocabulary_id_verbatim() -> None:
    """D-04 fidelity: documented tag query parameters cross the wire untranslated."""
    transport = SyncCaptureTransport(body=_success_body({"count": 1, "results": [TAG_RESULT]}))
    client = _client(transport)

    envelope = client.vocabularies_licenses.tag_search(q="hea", vocabulary_id="genres", limit=5, offset=2)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/tag_search")
    assert json.loads(request.body or b"{}") == {"q": "hea", "vocabulary_id": "genres", "limit": 5, "offset": 2}
    mapping = next(item for item in envelope.items if isinstance(item, MappingRecord))
    results = dict(mapping.payload)["results"]
    assert isinstance(results, tuple) and dict(results[0])["name"] == "health"

    autocomplete_transport = SyncCaptureTransport(body=_success_body([TAG_RESULT]))
    autocomplete_client = _client(autocomplete_transport)
    autocomplete_client.vocabularies_licenses.tag_autocomplete(q="he", vocabulary_id="genres")
    assert json.loads(autocomplete_transport.requests[0].body or b"{}") == {"q": "he", "vocabulary_id": "genres"}


def test_deprecated_fields_parameter_is_unrepresentable_on_typed_signatures() -> None:
    """D-01: the deprecated fields name raises TypeError at the boundary at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body({"count": 0, "results": []}))
    client = _client(transport)

    with pytest.raises(TypeError):
        client.vocabularies_licenses.tag_search(fields=["name"])  # ty: ignore[unknown-argument]

    with pytest.raises(TypeError):
        client.vocabularies_licenses.tag_autocomplete(fields=["name"])  # ty: ignore[unknown-argument]

    assert transport.requests == []


def test_umbrella_payload_with_fields_key_raises_validation_error_before_dispatch() -> None:
    """Umbrella payload-dict calls get an explicit typed refusal at zero I/O."""
    transport = SyncCaptureTransport(body=_success_body({"count": 0, "results": []}))
    client = _client(transport)
    operation = CatalogOperationRequest(
        operation_id=OperationId(
            platform="ckan", service="action-api-v3", method="tags-vocabularies-licenses-list-show"
        ),
        payload={"action": "tag_search", "fields": ["name"]},
    )
    guard = CatalogOperationGuard(operation_id=operation.operation_id)

    with pytest.raises(CatalogValidationError) as excinfo:
        client.vocabularies_licenses.tags_vocabularies_and_licenses(operation, guard)

    assert transport.requests == []
    assert "fields" in str(excinfo.value)


def test_vocabulary_crud_decodes_vocabulary_kind_records() -> None:
    """Vocabulary reads and mutations decode their own declared kind."""
    show_transport = SyncCaptureTransport(body=_success_body(VOCABULARY_RESULT))
    show_client = _client(show_transport)
    shown = show_client.vocabularies_licenses.vocabulary_show(id="vocab-1")
    record = next(item for item in shown.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "vocabulary"
    assert record.id.value == "vocab-1"

    create_transport = SyncCaptureTransport(body=_success_body(VOCABULARY_RESULT))
    create_client = _client(create_transport)
    created = create_client.vocabularies_licenses.vocabulary_create(name="genres")
    created_record = next(item for item in created.result.items if isinstance(item, NativeRecord))
    assert created_record.resource_kind.value == "vocabulary"
    assert json.loads(create_transport.requests[0].body or b"{}") == {"name": "genres"}

    update_transport = SyncCaptureTransport(body=_success_body(VOCABULARY_RESULT))
    update_client = _client(update_transport)
    updated = update_client.vocabularies_licenses.vocabulary_update(id="vocab-1", name="kinds")
    updated_record = next(item for item in updated.result.items if isinstance(item, NativeRecord))
    assert updated_record.resource_kind.value == "vocabulary"


def test_tag_crud_and_lists_decode_their_declared_shapes() -> None:
    """Tag creates decode tag records; free lists and formats stay value-shaped."""
    create_transport = SyncCaptureTransport(body=_success_body(TAG_RESULT))
    create_client = _client(create_transport)
    created = create_client.vocabularies_licenses.tag_create(name="health", vocabulary_id="genres")
    record = next(item for item in created.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "tag"
    assert json.loads(create_transport.requests[0].body or b"{}") == {"name": "health", "vocabulary_id": "genres"}

    list_transport = SyncCaptureTransport(body=_success_body(["health", "transit"]))
    list_client = _client(list_transport)
    listing = list_client.vocabularies_licenses.tag_list(q="ea")
    assert all(isinstance(item, ValueRecord) for item in listing.items)


def test_remaining_tag_vocabulary_and_translation_actions_cross_the_wire() -> None:
    show_transport = SyncCaptureTransport(body=_success_body(TAG_RESULT))
    shown = _client(show_transport).vocabularies_licenses.tag_show(id="tag-1")
    assert isinstance(shown.items[0], NativeRecord)
    assert show_transport.requests[0].url.endswith("/api/3/action/tag_show")
    assert json.loads(show_transport.requests[0].body or b"{}") == {"id": "tag-1"}

    tag_delete_transport = SyncCaptureTransport(body=_success_body(None))
    _client(tag_delete_transport).vocabularies_licenses.tag_delete(id="tag-1", vocabulary_id="vocab-1")
    assert tag_delete_transport.requests[0].url.endswith("/api/3/action/tag_delete")
    assert json.loads(tag_delete_transport.requests[0].body or b"{}") == {
        "id": "tag-1",
        "vocabulary_id": "vocab-1",
    }

    vocabulary_delete_transport = SyncCaptureTransport(body=_success_body(None))
    _client(vocabulary_delete_transport).vocabularies_licenses.vocabulary_delete(id="vocab-1")
    assert vocabulary_delete_transport.requests[0].url.endswith("/api/3/action/vocabulary_delete")
    assert json.loads(vocabulary_delete_transport.requests[0].body or b"{}") == {"id": "vocab-1"}

    translation_transport = SyncCaptureTransport(
        body=_success_body([{"term": "health", "lang_code": "de", "term_translation": "Gesundheit"}])
    )
    translations = _client(translation_transport).vocabularies_licenses.term_translation_show(
        terms=["health"], lang_codes=["de"]
    )
    assert isinstance(translations.items[0], MappingRecord)
    assert translation_transport.requests[0].url.endswith("/api/3/action/term_translation_show")
    assert json.loads(translation_transport.requests[0].body or b"{}") == {
        "terms": ["health"],
        "lang_codes": ["de"],
    }


def test_license_list_and_format_autocomplete_decode_read_only_envelopes() -> None:
    """License rows arrive as mapping records; format completions as scalar values."""
    license_transport = SyncCaptureTransport(body=_success_body([LICENSE_ROW]))
    license_client = _client(license_transport)
    licenses = license_client.vocabularies_licenses.license_list()
    row = next(item for item in licenses.items if isinstance(item, MappingRecord))
    assert dict(row.payload)["id"] == "cc-by"

    format_transport = SyncCaptureTransport(body=_success_body(["CSV", "XLSX"]))
    format_client = _client(format_transport)
    formats = format_client.vocabularies_licenses.format_autocomplete(q="cs")
    assert all(isinstance(item, ValueRecord) for item in formats.items)


def test_term_translation_batch_sends_payload_keys_verbatim() -> None:
    """The batch payload crosses untouched with its receipt on the standard tier."""
    transport = SyncCaptureTransport(body=_success_body({"success": {"rows_updated": 2}}))
    client = _client(transport)

    batch: TranslationBatch = [
        {"term": "health", "lang_code": "de", "term_translation": "Gesundheit"},
        {"term": "transit", "lang_code": "de", "term_translation": "Transit"},
    ]
    result = client.vocabularies_licenses.term_translation_update_many(data=batch)

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/term_translation_update_many")
    assert json.loads(request.body or b"{}") == {"data": batch}
    assert isinstance(result, CKANMutationResult)
    assert result.receipt.outcome == "succeeded"

    single_transport = SyncCaptureTransport(body=_success_body(None))
    single_client = _client(single_transport)
    single_client.vocabularies_licenses.term_translation_update(
        term="health", lang_code="de", term_translation="Gesundheit"
    )
    assert json.loads(single_transport.requests[0].body or b"{}") == {
        "term": "health",
        "lang_code": "de",
        "term_translation": "Gesundheit",
    }


def test_async_vocabulary_mutations_mirror_the_sync_semantics() -> None:
    """The async twin keeps verbatim batches and own-kind decoding."""
    transport = AsyncCaptureTransport(body=_success_body(VOCABULARY_RESULT))
    client = _async_client(transport)

    result = asyncio.run(client.vocabularies_licenses.vocabulary_create(name="genres"))

    request = transport.requests[0]
    assert request.url.endswith("/api/3/action/vocabulary_create")
    assert json.loads(request.body or b"{}") == {"name": "genres"}
    record = next(item for item in result.result.items if isinstance(item, NativeRecord))
    assert record.resource_kind.value == "vocabulary"
    assert result.receipt.outcome == "succeeded"
