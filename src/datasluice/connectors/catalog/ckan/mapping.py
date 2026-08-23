"""Decoding of CKAN Action API wire payloads into typed catalog values."""

from __future__ import annotations

from collections.abc import Mapping

from datasluice.connectors.catalog.ckan.errors import map_envelope_error
from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.errors.catalog import NativeCatalogError

PLATFORM = CatalogPlatform.CKAN


def parse_action_envelope(
    payload: object,
    *,
    operation: str,
    platform: CatalogPlatform | str = PLATFORM,
) -> object:
    """Decode one CKAN Action API JSON body into its bare result value or a typed failure.

    Args:
        payload: The decoded JSON body of an Action API response.
        operation: The dispatching operation identifier for typed failures.
        platform: The platform identity carried by typed failures.

    Returns:
        The bare ``result`` value of a ``success:true`` envelope, verbatim.

    Raises:
        NativeCatalogError: If the payload is not an envelope-shaped JSON object.
        CatalogError: If the envelope reports ``success:false``.
    """
    if not isinstance(payload, Mapping):
        raise _invalid_response(operation, platform, "The deployment sent a non-object Action API payload.")
    success = payload.get("success")
    if type(success) is not bool:
        raise _invalid_response(operation, platform, "The deployment sent an envelope without a boolean success flag.")
    if success:
        return payload.get("result")
    error = payload.get("error")
    if not isinstance(error, Mapping):
        raise _invalid_response(operation, platform, "The deployment sent a failed envelope without an error object.")
    raise map_envelope_error(dict(error), operation=operation, platform=platform)


def _invalid_response(operation: str, platform: CatalogPlatform | str, detail: str) -> NativeCatalogError:
    return NativeCatalogError(detail, operation=operation, platform=platform)
