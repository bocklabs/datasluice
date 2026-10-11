"""Both-mode CKAN tag, vocabulary, and license projections across the split v2 ids.

Every typed method declares its owning v2 OperationId from the checked-in manifest
and passes documented CKAN 2.11 parameters verbatim (D-04). The deprecated
``fields`` parameter of ``tag_search``/``tag_autocomplete`` is unrepresentable on
typed signatures and refused pre-dispatch on umbrella payloads (D-01, identical
discipline to the dataset group). Vocabulary CRUD decodes the connector-declared
vocabulary kind; translation batches cross the wire untouched.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

from datasluice.connectors.catalog.ckan.clients import (
    _async_typed_mutation,
    _async_typed_read,
    _AsyncGroupService,
    _sync_typed_mutation,
    _sync_typed_read,
    _SyncGroupService,
)
from datasluice.connectors.catalog.ckan.mapping import PLATFORM, TAG, VOCABULARY
from datasluice.connectors.catalog.ckan.services._shared import drop_unset
from datasluice.domain.catalog.ids import CatalogId

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient
    from datasluice.connectors.catalog.ckan.results import CKANMutationResult
    from datasluice.contracts.catalog.native.ckan import CKANResultItem
    from datasluice.domain.catalog.models import ResultEnvelope
    from datasluice.domain.catalog.safety import MutationPolicy

_VOCABULARY_GROUP = "vocabularies_licenses"

type FieldSpecList = list[Mapping[str, object]]
type TranslationBatch = list[Mapping[str, object]]
type TermTranslationTerms = list[str]
type WireParams = dict[str, object]


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "tag_create":
        return CatalogId(PLATFORM, TAG, str(params.get("name", "")))
    if action == "tag_delete":
        return CatalogId(PLATFORM, TAG, str(params.get("id", "")))
    if action == "vocabulary_create":
        return CatalogId(PLATFORM, VOCABULARY, str(params.get("name", "")))
    if action in {"vocabulary_update", "vocabulary_delete"}:
        return CatalogId(PLATFORM, VOCABULARY, str(params.get("id", "")))
    if action == "term_translation_update":
        return CatalogId(PLATFORM, VOCABULARY, str(params.get("term", "")))
    return CatalogId(PLATFORM, VOCABULARY, str(action))


class SyncVocabulariesLicensesService(_SyncGroupService):
    """Synchronous vocabulary/license projection carrying sixteen typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "vocabularies_licenses")

    def tag_list(
        self, *, vocabulary_id: str | None = None, q: str | None = None, all_fields: bool | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List tags optionally scoped to one vocabulary."""
        params = drop_unset({"vocabulary_id": vocabulary_id, "q": q, "all_fields": all_fields})
        return self._invoke_read("tag_list", params)

    def tag_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one tag by name or id."""
        return self._invoke_read("tag_show", {"id": id})

    def tag_search(
        self,
        *,
        q: str | None = None,
        vocabulary_id: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search tags with the documented query parameters."""
        params = drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit, "offset": offset})
        return self._invoke_read("tag_search", params)

    def tag_autocomplete(
        self, *, q: str | None = None, vocabulary_id: str | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete tags with the documented query parameters."""
        params = drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit})
        return self._invoke_read("tag_autocomplete", params)

    def vocabulary_list(self) -> ResultEnvelope[CKANResultItem]:
        """List every vocabulary on the deployment."""
        return self._invoke_read("vocabulary_list", {})

    def vocabulary_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one vocabulary by id or name."""
        return self._invoke_read("vocabulary_show", {"id": id})

    def license_list(self) -> ResultEnvelope[CKANResultItem]:
        """List the deployment's licenses as read-only envelopes."""
        return self._invoke_read("license_list", {})

    def format_autocomplete(self, *, q: str | None = None, limit: int | None = None) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete resource formats."""
        return self._invoke_read("format_autocomplete", drop_unset({"q": q, "limit": limit}))

    def term_translation_show(
        self, *, terms: TermTranslationTerms, lang_codes: list[str] | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Show translations for the listed terms."""
        params: WireParams = {"terms": terms}
        params.update(drop_unset({"lang_codes": lang_codes}))
        return self._invoke_read("term_translation_show", params)

    def tag_create(
        self, *, name: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a free tag or add a tag to one vocabulary."""
        return self._invoke_mutation("tag_create", drop_unset({"name": name, "vocabulary_id": vocabulary_id}), policy)

    def tag_delete(
        self, *, id: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete a free tag or remove a tag from one vocabulary."""
        return self._invoke_mutation("tag_delete", drop_unset({"id": id, "vocabulary_id": vocabulary_id}), policy)

    def vocabulary_create(
        self, *, name: str, tags: FieldSpecList | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a vocabulary optionally seeded with tags."""
        return self._invoke_mutation("vocabulary_create", drop_unset({"name": name, "tags": tags}), policy)

    def vocabulary_update(
        self,
        *,
        id: str,
        name: str | None = None,
        tags: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one vocabulary's name or tag membership."""
        params: WireParams = {"id": id}
        params.update(drop_unset({"name": name, "tags": tags}))
        return self._invoke_mutation("vocabulary_update", params, policy)

    def vocabulary_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one empty vocabulary on the standard tier."""
        return self._invoke_mutation("vocabulary_delete", {"id": id}, policy)

    def term_translation_update(
        self, *, term: str, lang_code: str, term_translation: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update one term translation with documented language codes."""
        params: WireParams = {"term": term, "lang_code": lang_code, "term_translation": term_translation}
        return self._invoke_mutation("term_translation_update", params, policy)

    def term_translation_update_many(
        self, *, data: TranslationBatch, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Send a batch of term translations verbatim to the deployment."""
        return self._invoke_mutation("term_translation_update_many", {"data": data}, policy)

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return _sync_typed_read(self._client, _VOCABULARY_GROUP, action, params)

    def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        return _sync_typed_mutation(self._client, _VOCABULARY_GROUP, action, params, policy, _mutation_target)


class AsyncVocabulariesLicensesService(_AsyncGroupService):
    """Asynchronous vocabulary/license projection carrying sixteen typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "vocabularies_licenses")

    async def tag_list(
        self, *, vocabulary_id: str | None = None, q: str | None = None, all_fields: bool | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List tags optionally scoped to one vocabulary."""
        params = drop_unset({"vocabulary_id": vocabulary_id, "q": q, "all_fields": all_fields})
        return await self._invoke_read("tag_list", params)

    async def tag_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one tag by name or id."""
        return await self._invoke_read("tag_show", {"id": id})

    async def tag_search(
        self,
        *,
        q: str | None = None,
        vocabulary_id: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> ResultEnvelope[CKANResultItem]:
        """Search tags with the documented query parameters."""
        params = drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit, "offset": offset})
        return await self._invoke_read("tag_search", params)

    async def tag_autocomplete(
        self, *, q: str | None = None, vocabulary_id: str | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete tags with the documented query parameters."""
        params = drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit})
        return await self._invoke_read("tag_autocomplete", params)

    async def vocabulary_list(self) -> ResultEnvelope[CKANResultItem]:
        """List every vocabulary on the deployment."""
        return await self._invoke_read("vocabulary_list", {})

    async def vocabulary_show(self, *, id: str) -> ResultEnvelope[CKANResultItem]:
        """Show one vocabulary by id or name."""
        return await self._invoke_read("vocabulary_show", {"id": id})

    async def license_list(self) -> ResultEnvelope[CKANResultItem]:
        """List the deployment's licenses as read-only envelopes."""
        return await self._invoke_read("license_list", {})

    async def format_autocomplete(
        self, *, q: str | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete resource formats."""
        return await self._invoke_read("format_autocomplete", drop_unset({"q": q, "limit": limit}))

    async def term_translation_show(
        self, *, terms: TermTranslationTerms, lang_codes: list[str] | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Show translations for the listed terms."""
        params: WireParams = {"terms": terms}
        params.update(drop_unset({"lang_codes": lang_codes}))
        return await self._invoke_read("term_translation_show", params)

    async def tag_create(
        self, *, name: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a free tag or add a tag to one vocabulary."""
        params = drop_unset({"name": name, "vocabulary_id": vocabulary_id})
        return await self._invoke_mutation("tag_create", params, policy)

    async def tag_delete(
        self, *, id: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete a free tag or remove a tag from one vocabulary."""
        params = drop_unset({"id": id, "vocabulary_id": vocabulary_id})
        return await self._invoke_mutation("tag_delete", params, policy)

    async def vocabulary_create(
        self, *, name: str, tags: FieldSpecList | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a vocabulary optionally seeded with tags."""
        params = drop_unset({"name": name, "tags": tags})
        return await self._invoke_mutation("vocabulary_create", params, policy)

    async def vocabulary_update(
        self,
        *,
        id: str,
        name: str | None = None,
        tags: FieldSpecList | None = None,
        policy: MutationPolicy | None = None,
    ) -> CKANMutationResult:
        """Update one vocabulary's name or tag membership."""
        params: WireParams = {"id": id}
        params.update(drop_unset({"name": name, "tags": tags}))
        return await self._invoke_mutation("vocabulary_update", params, policy)

    async def vocabulary_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one empty vocabulary on the standard tier."""
        return await self._invoke_mutation("vocabulary_delete", {"id": id}, policy)

    async def term_translation_update(
        self, *, term: str, lang_code: str, term_translation: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update one term translation with documented language codes."""
        params: WireParams = {"term": term, "lang_code": lang_code, "term_translation": term_translation}
        return await self._invoke_mutation("term_translation_update", params, policy)

    async def term_translation_update_many(
        self, *, data: TranslationBatch, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Send a batch of term translations verbatim to the deployment."""
        return await self._invoke_mutation("term_translation_update_many", {"data": data}, policy)

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        return await _async_typed_read(self._client, _VOCABULARY_GROUP, action, params)

    async def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        return await _async_typed_mutation(self._client, _VOCABULARY_GROUP, action, params, policy, _mutation_target)
