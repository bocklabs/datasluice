"""Pre-flight reject policy for unsupported ``Query`` filter fields (D-P5-06).

The reject gate runs at the top of every connector's ``search()`` BEFORE any
transport call (ARCH-08). It validates that every set ``Query`` filter field
is a member of the connector's
:class:`~datasluice.domain.capabilities.CatalogCapabilities.supported_query_fields`
frozenset and raises :class:`~datasluice.exceptions.UnsupportedQueryFieldError`
otherwise, so a caller cannot push an unsupported filter to the portal.

Determinism contract (D-P5-09): when multiple unsupported fields are set, the
reported field is the FIRST by ``Query`` declaration order (``text`` ->
``tags`` -> ``organizations`` -> ``groups`` -> ``res_format`` ->
``license_id`` -> ``sort``), never set-iteration order. An empty list, an
empty string, or ``None`` is treated as "unset" so an explicit ``tags=[]``
does not raise.
"""

from __future__ import annotations

from datasluice.domain import Query
from datasluice.exceptions import UnsupportedQueryFieldError

_QUERY_DECLARATION_ORDER: tuple[str, ...] = (
    "text",
    "tags",
    "organizations",
    "groups",
    "res_format",
    "license_id",
    "sort",
)


def _reject_unsupported_fields(query: Query, supported: frozenset[str], portal_name: str) -> None:
    """Raise ``UnsupportedQueryFieldError`` if *query* uses a field not in *supported*.

    Args:
        query: The portal-agnostic :class:`Query` to validate.
        supported: The connector's supported filter-field names
            (typically ``CatalogCapabilities.supported_query_fields``).
        portal_name: Canonical portal name for the error message
            (e.g. ``"ckan"``, ``"datagouv"``).

    Raises:
        UnsupportedQueryFieldError: When at least one set field on *query* is
            not in *supported*. The reported ``field`` is the first by Query
            declaration order.
    """
    supported_sorted = sorted(supported)
    for field_name in _QUERY_DECLARATION_ORDER:
        if _is_set(getattr(query, field_name)) and field_name not in supported:
            raise UnsupportedQueryFieldError(
                field=field_name,
                supported_fields=supported_sorted,
                portal_name=portal_name,
            )


def _is_set(value: object) -> bool:
    """Return ``True`` when *value* counts as a set filter field.

    ``None``, empty list, and empty string are treated as unset so the reject
    policy does not fire on default-initialized fields.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return value != ""
    if isinstance(value, list | tuple | set | frozenset):
        return len(value) > 0
    return True
