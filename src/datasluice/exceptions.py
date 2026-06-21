"""Exception hierarchy for DataSluice."""

from __future__ import annotations


class DataSluiceError(Exception):
    """Base exception for all DataSluice errors."""


class PortalError(DataSluiceError):
    """Raised when a portal returns an error or is unreachable."""


class AdapterError(DataSluiceError):
    """Raised when an adapter cannot fulfil a request."""


class AdapterNotFoundError(AdapterError):
    """Raised when no adapter is registered for a portal type."""


class PortalDetectionError(DataSluiceError):
    """Raised when the portal type cannot be auto-detected."""


class AuthenticationError(DataSluiceError):
    """Raised when authentication credentials are missing or invalid."""


class RateLimitError(PortalError):
    """Raised when the portal rate-limits requests."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class NotFoundError(PortalError):
    """Raised when a requested dataset or resource does not exist."""


class DownloadError(DataSluiceError):
    """Raised when a resource download fails."""


class ChecksumMismatchError(DownloadError):
    """Raised when a downloaded file's checksum does not match."""

    def __init__(self, message: str, expected: str | None = None, actual: str | None = None) -> None:
        super().__init__(message)
        self.expected = expected
        self.actual = actual


class FormatError(DataSluiceError):
    """Raised when a resource cannot be parsed in the expected format."""


class ConfigError(DataSluiceError):
    """Raised when configuration is invalid or incomplete."""
