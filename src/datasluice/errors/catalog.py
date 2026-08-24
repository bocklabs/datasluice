"""Normalized and native error contracts for catalog connectors."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from types import MappingProxyType
from typing import Never

from datasluice.domain.catalog.ids import CatalogPlatform
from datasluice.domain.catalog.redaction import MAX_TEXT_LENGTH, redact_mapping, redact_string
from datasluice.exceptions import DataSluiceError


def _platform_value(platform: CatalogPlatform | str) -> str:
    value = platform.value if isinstance(platform, CatalogPlatform) else platform
    if not isinstance(value, str) or not value:
        raise ValueError("Catalog error platform must be a non-empty string.")
    return value


def _bounded_metadata(value: Mapping[str, object] | None, *, _depth: int = 0) -> Mapping[str, object]:
    """Return total, redacted error metadata, truncating values beyond public bounds."""
    if value is None:
        return MappingProxyType({})
    redacted = redact_mapping(value, _depth=_depth)
    return MappingProxyType(redacted)


class CatalogError(DataSluiceError):
    """Base normalized error with a portable safe next action."""

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        platform: CatalogPlatform | str,
        capability_state: str | None = None,
        safe_action: str,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        if not isinstance(message, str) or not message:
            raise ValueError("Catalog error messages must be non-empty strings.")
        if not isinstance(operation, str) or not operation:
            raise ValueError("Catalog error operations must be non-empty strings.")
        if not isinstance(safe_action, str) or not safe_action:
            raise ValueError("Catalog errors require a safe next action.")
        super().__init__(message)
        self.operation = operation
        self.platform = _platform_value(platform)
        self.capability_state = capability_state
        self.safe_action = safe_action
        self.metadata = _bounded_metadata(metadata)


class NativeCatalogError(DataSluiceError):
    """A redacted, bounded platform-native failure retained for native services."""

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        platform: CatalogPlatform | str,
        status_code: int | None = None,
        vendor_code: str | None = None,
        retry_after: float | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        if not isinstance(message, str) or not message:
            raise ValueError("Native catalog error messages must be non-empty strings.")
        if not isinstance(operation, str) or not operation:
            raise ValueError("Native catalog error operations must be non-empty strings.")
        if status_code is not None and (type(status_code) is not int or not 100 <= status_code <= 599):
            raise ValueError("Native catalog error status codes must be valid HTTP status codes.")
        if vendor_code is not None and (not isinstance(vendor_code, str) or len(vendor_code) > MAX_TEXT_LENGTH):
            raise ValueError("Native catalog error vendor codes must be bounded strings.")
        if retry_after is not None and (
            (type(retry_after) is not int and type(retry_after) is not float)
            or not isfinite(retry_after)
            or retry_after < 0
        ):
            raise ValueError("Native catalog error Retry-After must be a non-negative number.")
        super().__init__(redact_string(message))
        self.operation = operation
        self.platform = _platform_value(platform)
        self.status_code = status_code
        self.vendor_code = vendor_code
        self.retry_after = float(retry_after) if retry_after is not None else None
        self.metadata = _bounded_metadata(metadata)


class UnsupportedCapabilityError(CatalogError):
    """Raised when a deployment does not support a requested operation."""


class UnauthenticatedError(CatalogError):
    """Raised when credentials are absent, invalid, or expired."""


class ForbiddenError(CatalogError):
    """Raised when known credentials lack the required permission or role."""


class CatalogNotFoundError(CatalogError):
    """Raised when a catalog target does not exist."""


class CatalogValidationError(CatalogError):
    """Raised when a catalog request is invalid before or after dispatch."""


class CatalogConflictError(CatalogError):
    """Raised when a concurrency token or mutation state conflicts."""


class CatalogRateLimitError(CatalogError):
    """Raised when a catalog deployment requests deferred retry."""

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        platform: CatalogPlatform | str,
        capability_state: str | None = None,
        safe_action: str,
        retry_after: float | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        if retry_after is not None and (
            (type(retry_after) is not int and type(retry_after) is not float)
            or not isfinite(retry_after)
            or retry_after < 0
        ):
            raise ValueError("Retry-After must be a non-negative number.")
        super().__init__(
            message,
            operation=operation,
            platform=platform,
            capability_state=capability_state,
            safe_action=safe_action,
            metadata=metadata,
        )
        self.retry_after = float(retry_after) if retry_after is not None else None


class BudgetExhaustedError(CatalogError):
    """Raised when an operation exceeds its finite runtime time budget."""

    def __init__(
        self,
        message: str,
        *,
        operation: str,
        platform: CatalogPlatform | str,
        capability_state: str | None = None,
        safe_action: str,
        elapsed_seconds: float,
        budget_seconds: float,
        retry_state: Mapping[str, object] | None = None,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        for value, name in ((elapsed_seconds, "Elapsed"), (budget_seconds, "Budget")):
            if (type(value) is not int and type(value) is not float) or not isfinite(value) or value < 0:
                raise ValueError(f"{name} seconds must be finite non-negative numbers.")
        if budget_seconds <= 0:
            raise ValueError("Budget seconds must be a positive number.")
        super().__init__(
            message,
            operation=operation,
            platform=platform,
            capability_state=capability_state,
            safe_action=safe_action,
            metadata=metadata,
        )
        self.elapsed_seconds = float(elapsed_seconds)
        self.budget_seconds = float(budget_seconds)
        self.retry_state = _bounded_metadata(retry_state)


class CatalogUnavailableError(CatalogError):
    """Raised when a catalog deployment or circuit is unavailable."""


def map_catalog_error(native: NativeCatalogError) -> CatalogError:
    """Map one native failure to a normalized portable error without losing its cause."""
    if not isinstance(native, NativeCatalogError):
        raise TypeError("Catalog error mapping requires NativeCatalogError.")
    status_code = native.status_code
    error_type: type[CatalogError]
    capability_state: str | None = None
    safe_action: str
    if status_code == 401:
        error_type = UnauthenticatedError
        capability_state = "unauthorized"
        safe_action = "Provide valid credentials and retry the operation."
    elif status_code == 403:
        error_type = ForbiddenError
        capability_state = "forbidden"
        safe_action = "Use credentials with the required scope or role."
    elif status_code == 404:
        error_type = CatalogNotFoundError
        safe_action = "Confirm the target identifier and deployment."
    elif status_code == 409:
        error_type = CatalogConflictError
        safe_action = "Refresh the target version token before retrying."
    elif status_code == 422:
        error_type = CatalogValidationError
        safe_action = "Correct the request according to the platform validation details."
    elif status_code == 429:
        error_type = CatalogRateLimitError
        safe_action = "Wait for Retry-After before retrying a safe operation."
    elif status_code is None or status_code >= 500:
        error_type = CatalogUnavailableError
        capability_state = "unavailable"
        safe_action = "Retry after the deployment is available."
    else:
        error_type = CatalogValidationError
        safe_action = "Correct the request before retrying."
    error: CatalogError
    if error_type is CatalogRateLimitError:
        error = CatalogRateLimitError(
            str(native),
            operation=native.operation,
            platform=native.platform,
            capability_state=capability_state,
            safe_action=safe_action,
            retry_after=native.retry_after,
            metadata=native.metadata,
        )
    else:
        error = error_type(
            str(native),
            operation=native.operation,
            platform=native.platform,
            capability_state=capability_state,
            safe_action=safe_action,
            metadata=native.metadata,
        )
    error.__cause__ = native
    return error


def raise_mapped_catalog_error(native: NativeCatalogError) -> Never:
    """Raise the normalized form of a native failure with explicit exception chaining."""
    raise map_catalog_error(native) from native
