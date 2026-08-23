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
from typing import TYPE_CHECKING, cast

from datasluice.connectors.catalog.ckan.clients import (
    _AsyncNativeService,
    _operation_id_from,
    _SyncNativeService,
)
from datasluice.connectors.catalog.ckan.inventory import ActionEntry
from datasluice.connectors.catalog.ckan.mapping import PLATFORM, TAG, VOCABULARY
from datasluice.connectors.catalog.ckan.results import CKANMutationResult, require_mutation_tier
from datasluice.contracts.catalog.native.ckan import CKANResultItem
from datasluice.contracts.catalog.protocols import CatalogOperationGuard, CatalogOperationRequest
from datasluice.domain.catalog.ids import CatalogId
from datasluice.domain.catalog.models import ResultEnvelope
from datasluice.domain.catalog.safety import MutationPolicy
from datasluice.errors.catalog import CatalogValidationError
from datasluice.runtime.mutation import build_mutation_receipt

if TYPE_CHECKING:
    from datasluice.connectors.catalog.ckan.clients import AsyncCKANClient, SyncCKANClient

_VOCABULARY_GROUP = "vocabularies_licenses"

type FieldSpecList = list[Mapping[str, object]]
type TranslationBatch = list[Mapping[str, object]]
type TermTranslationTerms = list[str]
type WireParams = dict[str, object]

_DEPRECATED_PARAMETERS: Mapping[str, frozenset[str]] = {
    "tag_search": frozenset({"fields"}),
    "tag_autocomplete": frozenset({"fields"}),
}


def _drop_unset(params: dict[str, object | None]) -> dict[str, object]:
    return {key: value for key, value in params.items() if value is not None}


def _reject_deprecated(action: str, payload: Mapping[str, object]) -> None:
    banned = _DEPRECATED_PARAMETERS.get(action)
    if not banned:
        return
    clash = sorted(banned.intersection(payload))
    if clash:
        raise CatalogValidationError(
            f"The parameter(s) {clash} are officially deprecated for {action} and are not accepted.",
            operation=f"{PLATFORM.value}/{action}",
            platform=PLATFORM.value,
            safe_action="Use q with an optional vocabulary_id; the fields parameter has no replacement.",
        )


def _mutation_target(action: str, params: Mapping[str, object]) -> CatalogId:
    if action == "tag_create":
        return CatalogId(PLATFORM, TAG, str(params["name"]))
    if action == "tag_delete":
        return CatalogId(PLATFORM, TAG, str(params["id"]))
    if action == "vocabulary_create":
        return CatalogId(PLATFORM, VOCABULARY, str(params["name"]))
    if action in {"vocabulary_update", "vocabulary_delete"}:
        return CatalogId(PLATFORM, VOCABULARY, str(params["id"]))
    if action == "term_translation_update":
        return CatalogId(PLATFORM, VOCABULARY, str(params["term"]))
    return CatalogId(PLATFORM, VOCABULARY, str(action))


class SyncVocabulariesLicensesService(_SyncNativeService):
    """Synchronous vocabulary/license projection carrying sixteen typed actions."""

    __slots__ = ()

    def __init__(self, client: SyncCKANClient) -> None:
        super().__init__(client, "vocabularies_licenses")

    def tags_vocabularies_and_licenses(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Read or manage tags, vocabularies, and licenses with deprecation discipline."""
        action = operation.payload.get("action")
        if isinstance(action, str):
            _reject_deprecated(action, operation.payload)
        return super().tags_vocabularies_and_licenses(operation, guard)

    def tag_list(
        self, *, vocabulary_id: str | None = None, q: str | None = None, all_fields: bool | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List tags optionally scoped to one vocabulary."""
        params = _drop_unset({"vocabulary_id": vocabulary_id, "q": q, "all_fields": all_fields})
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
        params = _drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit, "offset": offset})
        return self._invoke_read("tag_search", params)

    def tag_autocomplete(
        self, *, q: str | None = None, vocabulary_id: str | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete tags with the documented query parameters."""
        params = _drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit})
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
        return self._invoke_read("format_autocomplete", _drop_unset({"q": q, "limit": limit}))

    def term_translation_show(
        self, *, terms: TermTranslationTerms, lang_from: str | None = None, lang_to: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Show translations for the listed terms."""
        params: WireParams = {"terms": terms}
        params.update(_drop_unset({"lang_from": lang_from, "lang_to": lang_to}))
        return self._invoke_read("term_translation_show", params)

    def tag_create(
        self, *, name: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a free tag or add a tag to one vocabulary."""
        return self._invoke_mutation("tag_create", _drop_unset({"name": name, "vocabulary_id": vocabulary_id}), policy)

    def tag_delete(
        self, *, id: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete a free tag or remove a tag from one vocabulary."""
        return self._invoke_mutation("tag_delete", _drop_unset({"id": id, "vocabulary_id": vocabulary_id}), policy)

    def vocabulary_create(
        self, *, name: str, tags: FieldSpecList | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a vocabulary optionally seeded with tags."""
        return self._invoke_mutation("vocabulary_create", _drop_unset({"name": name, "tags": tags}), policy)

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
        params.update(_drop_unset({"name": name, "tags": tags}))
        return self._invoke_mutation("vocabulary_update", params, policy)

    def vocabulary_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one empty vocabulary on the standard tier."""
        return self._invoke_mutation("vocabulary_delete", {"id": id}, policy)

    def term_translation_update(
        self, *, term: str, lang_from: str, lang_to: str, translation: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update one term translation with documented language codes."""
        params: WireParams = {
            "term": term,
            "lang_from": lang_from,
            "lang_to": lang_to,
            "translation": translation,
        }
        return self._invoke_mutation("term_translation_update", params, policy)

    def term_translation_update_many(
        self, *, data: TranslationBatch, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Send a batch of term translations verbatim to the deployment."""
        return self._invoke_mutation("term_translation_update_many", {"data": data}, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _VOCABULARY_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_VOCABULARY_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))

    def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: SyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], client._dispatch(operation, guard, entry=entry))
        receipt = build_mutation_receipt(
            owning_id, _mutation_target(entry.name, params), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)


class AsyncVocabulariesLicensesService(_AsyncNativeService):
    """Asynchronous vocabulary/license projection carrying sixteen typed actions."""

    __slots__ = ()

    def __init__(self, client: AsyncCKANClient) -> None:
        super().__init__(client, "vocabularies_licenses")

    async def tags_vocabularies_and_licenses(
        self, operation: CatalogOperationRequest, guard: CatalogOperationGuard
    ) -> ResultEnvelope[CKANResultItem]:
        """Read or manage tags, vocabularies, and licenses with deprecation discipline."""
        action = operation.payload.get("action")
        if isinstance(action, str):
            _reject_deprecated(action, operation.payload)
        return await super().tags_vocabularies_and_licenses(operation, guard)

    async def tag_list(
        self, *, vocabulary_id: str | None = None, q: str | None = None, all_fields: bool | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """List tags optionally scoped to one vocabulary."""
        params = _drop_unset({"vocabulary_id": vocabulary_id, "q": q, "all_fields": all_fields})
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
        params = _drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit, "offset": offset})
        return await self._invoke_read("tag_search", params)

    async def tag_autocomplete(
        self, *, q: str | None = None, vocabulary_id: str | None = None, limit: int | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Autocomplete tags with the documented query parameters."""
        params = _drop_unset({"q": q, "vocabulary_id": vocabulary_id, "limit": limit})
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
        return await self._invoke_read("format_autocomplete", _drop_unset({"q": q, "limit": limit}))

    async def term_translation_show(
        self, *, terms: TermTranslationTerms, lang_from: str | None = None, lang_to: str | None = None
    ) -> ResultEnvelope[CKANResultItem]:
        """Show translations for the listed terms."""
        params: WireParams = {"terms": terms}
        params.update(_drop_unset({"lang_from": lang_from, "lang_to": lang_to}))
        return await self._invoke_read("term_translation_show", params)

    async def tag_create(
        self, *, name: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a free tag or add a tag to one vocabulary."""
        params = _drop_unset({"name": name, "vocabulary_id": vocabulary_id})
        return await self._invoke_mutation("tag_create", params, policy)

    async def tag_delete(
        self, *, id: str, vocabulary_id: str | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Delete a free tag or remove a tag from one vocabulary."""
        params = _drop_unset({"id": id, "vocabulary_id": vocabulary_id})
        return await self._invoke_mutation("tag_delete", params, policy)

    async def vocabulary_create(
        self, *, name: str, tags: FieldSpecList | None = None, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Create a vocabulary optionally seeded with tags."""
        params = _drop_unset({"name": name, "tags": tags})
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
        params.update(_drop_unset({"name": name, "tags": tags}))
        return await self._invoke_mutation("vocabulary_update", params, policy)

    async def vocabulary_delete(self, *, id: str, policy: MutationPolicy | None = None) -> CKANMutationResult:
        """Delete one empty vocabulary on the standard tier."""
        return await self._invoke_mutation("vocabulary_delete", {"id": id}, policy)

    async def term_translation_update(
        self, *, term: str, lang_from: str, lang_to: str, translation: str, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Update one term translation with documented language codes."""
        params: WireParams = {
            "term": term,
            "lang_from": lang_from,
            "lang_to": lang_to,
            "translation": translation,
        }
        return await self._invoke_mutation("term_translation_update", params, policy)

    async def term_translation_update_many(
        self, *, data: TranslationBatch, policy: MutationPolicy | None = None
    ) -> CKANMutationResult:
        """Send a batch of term translations verbatim to the deployment."""
        return await self._invoke_mutation("term_translation_update_many", {"data": data}, policy)

    def _typed_entry(self, action: str) -> ActionEntry:
        entry = self._client._inventory.lookup(action)
        if entry.group != _VOCABULARY_GROUP:
            raise CatalogValidationError(
                f"The action {action!r} does not belong to the {_VOCABULARY_GROUP!r} group.",
                operation=entry.owning_operation_id,
                platform=PLATFORM.value,
                safe_action="Call the action through its owning native group projection.",
            )
        return entry

    async def _invoke_read(self, action: str, params: dict[str, object]) -> ResultEnvelope[CKANResultItem]:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        operation = CatalogOperationRequest(operation_id=_operation_id_from(entry.owning_operation_id), payload=params)
        guard = CatalogOperationGuard(operation_id=operation.operation_id, profile=client._profile)
        return cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))

    async def _invoke_mutation(
        self, action: str, params: dict[str, object], policy: MutationPolicy | None
    ) -> CKANMutationResult:
        entry = self._typed_entry(action)
        client: AsyncCKANClient = self._client
        owning_id = _operation_id_from(entry.owning_operation_id)
        effective = require_mutation_tier(entry.mutation_class, owning_id, policy)
        assert effective is not None
        operation = CatalogOperationRequest(operation_id=owning_id, payload=params, mutation_policy=effective)
        guard = CatalogOperationGuard(operation_id=owning_id, profile=client._profile)
        envelope = cast(ResultEnvelope[CKANResultItem], await client._dispatch(operation, guard, entry=entry))
        receipt = build_mutation_receipt(
            owning_id, _mutation_target(entry.name, params), effective, "succeeded", {"action": entry.name}
        )
        return CKANMutationResult(result=envelope, receipt=receipt)
