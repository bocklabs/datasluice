"""Exact stock extension request builders and bounded decoders for uData 17.6."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from urllib.parse import urlencode

from datasluice.connectors.catalog.udata.models.extensions import AvatarRequest, TagSuggestQuery, segment
from datasluice.domain.catalog.models import MappingRecord
from datasluice.errors.catalog import CatalogValidationError, NativeCatalogError

PLATFORM = "udata"
EXTENSIONS_OPERATION = "udata/api-v1.extensions"
REASON_CATEGORIES_OPERATION = "udata/api-v1.access-type-reason-categories"
SUGGEST_TAGS_OPERATION = "udata/api-v1.suggest-tags"
AVATAR_OPERATION = "udata/api-v1.avatar"
CAPTCHETAT_OPERATION = "udata/api-v2.captchetat"
EXTENSION_OPERATIONS = frozenset(
    {
        REASON_CATEGORIES_OPERATION,
        SUGGEST_TAGS_OPERATION,
        AVATAR_OPERATION,
        CAPTCHETAT_OPERATION,
    }
)
_REASON_CATEGORIES_PATH = "/api/1/access_type/reason_categories/"
_TAGS_SUGGEST_PATH = "/api/1/tags/suggest/"
_AVATARS_PATH = "/api/1/avatars/"
_CAPTCHETAT_PATH = "/api/2/captchetat/"
AVATAR_MAX_BYTES = 1024 * 1024
_AVATAR_MEDIA_TYPES = frozenset({"image/gif", "image/jpeg", "image/png", "image/webp"})
_PINNED_SCHEMA_ACTION = "Verify the response against the pinned uData 17.6 schema."
_PROBE_QUERY = "a"
_PROBE_SIZE = 1
_PROBE_IDENTIFIER = "udata"
_PROBE_PIXELS = 1

type Request = tuple[str, str, dict[str, str], object | None]


def _request(path: str) -> Request:
    return "GET", path, {}, None


def require_extension_operation(operation: object) -> str:
    """Validate one operation identity against the closed stock extension set.

    Args:
        operation: The candidate operation identity.

    Returns:
        The validated operation identity.

    Raises:
        CatalogValidationError: If the operation is not one of the stock
            extension operations assigned to this family.
    """
    if not isinstance(operation, str) or operation not in EXTENSION_OPERATIONS:
        raise CatalogValidationError(
            "The uData extension operation is not a documented stock extension operation.",
            operation=EXTENSIONS_OPERATION,
            platform=PLATFORM,
            safe_action="Pass one stock extension operation identity assigned to this family.",
        )
    return operation


def reason_categories_request() -> Request:
    """Encode GET /api/1/access_type/reason_categories/."""
    return _request(_REASON_CATEGORIES_PATH)


def suggest_tags_request(query: TagSuggestQuery) -> Request:
    """Encode GET /api/1/tags/suggest/ with the exact query string."""
    return _request(f"{_TAGS_SUGGEST_PATH}?{urlencode(query.query_params())}")


def avatar_request(client_input: AvatarRequest) -> Request:
    """Encode GET /api/1/avatars/<identifier>/<size>/."""
    return _request(
        f"{_AVATARS_PATH}{segment(client_input.identifier, AVATAR_OPERATION)}/{client_input.size}/",
    )


def captchetat_request() -> Request:
    """Encode GET /api/2/captchetat/."""
    return _request(_CAPTCHETAT_PATH)


def probe_tag_suggest_query() -> TagSuggestQuery:
    """Return the fixed bounded harmless probe query for the tag-suggest route."""
    return TagSuggestQuery(q=_PROBE_QUERY, size=_PROBE_SIZE)


def probe_avatar_request() -> AvatarRequest:
    """Return the fixed bounded harmless probe request for the avatar route."""
    return AvatarRequest(identifier=_PROBE_IDENTIFIER, size=_PROBE_PIXELS)


def parse_mapping_sequence(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    """Decode a documented stock extension list response into bounded records."""
    if not isinstance(payload, list) or not all(isinstance(item, Mapping) for item in payload):
        raise CatalogValidationError(
            "The uData extension response must be a list of objects.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return tuple(MappingRecord(item) for item in payload)


def parse_reason_categories(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    """Decode the access-type reason-category list."""
    return parse_mapping_sequence(payload, operation)


def parse_tag_suggestions(payload: object, operation: str) -> tuple[MappingRecord, ...]:
    """Decode the tag-suggestion list."""
    return parse_mapping_sequence(payload, operation)


def parse_captchetat(payload: object, operation: str) -> MappingRecord:
    """Decode the captcha-state document, failing closed on an ambiguous shape."""
    if not isinstance(payload, Mapping):
        raise CatalogValidationError(
            "The uData captcha-state response must be an object.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    active = payload.get("active")
    if "active" in payload and type(active) is not bool:
        raise CatalogValidationError(
            "The uData captcha-state active field must be a boolean.",
            operation=operation,
            platform=PLATFORM,
            safe_action=_PINNED_SCHEMA_ACTION,
        )
    return MappingRecord(payload)


def parse_avatar(body: object, *, response_media_type: str | None, operation: str) -> MappingRecord:
    """Bound one avatar image to its media type, size, and digest.

    The no-raw-body contract holds for image rows too: only the approved
    media type, byte count, and SHA-256 digest are retained.
    """
    if not isinstance(body, (bytes, bytearray)):
        raise NativeCatalogError(
            "The uData avatar response must be buffered bytes.",
            operation=operation,
            platform=PLATFORM,
        )
    media_type = (response_media_type or "").split(";", 1)[0].strip().lower()
    if media_type not in _AVATAR_MEDIA_TYPES:
        raise NativeCatalogError(
            f"The uData avatar media type {media_type!r} is not an approved image contract.",
            operation=operation,
            platform=PLATFORM,
            metadata={
                "safe_action": (
                    f"Request one of the approved avatar media types: {', '.join(sorted(_AVATAR_MEDIA_TYPES))}."
                )
            },
        )
    data = bytes(body)
    return MappingRecord(
        {
            "media_type": media_type,
            "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    )
