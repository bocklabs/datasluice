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


class RetryableHTTPError(PortalError):
    """Raised on HTTP 5xx responses that should be retried."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


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


class DecompressionError(FormatError):
    """Raised when a compressed resource cannot be decompressed (D-P4-21).

    Compression is format-adjacent: a decompression failure means the bytes
    could not be decoded into the underlying format, so it hangs off
    :class:`FormatError`.
    """


class ConfigError(DataSluiceError):
    """Raised when configuration is invalid or incomplete."""


class UnsupportedAccessError(DownloadError):
    """Raised when a resource's access kind has no reader implementation (D-P4-09).

    Phase 4 implements HttpDownload, ObjectStorage, and LocalFile readers.
    QueryAccess raises this error; Phase 5 adds query readers.
    """


class StreamClosedError(DataSluiceError):
    """Raised when operating on a closed :class:`BatchStream` (D-P4-21).

    A direct child of :class:`DataSluiceError` because it is an operational
    use-after-close error — the download already succeeded and the format is
    fine; the caller used the stream after ``__exit__`` or an explicit
    :meth:`BatchStream.close`.
    """


class SchemaUnificationError(DataSluiceError):
    """Raised when batch schemas cannot be unified under pyarrow promotion (D-P4-21).

    A direct child of :class:`DataSluiceError` because it is a data-reconciliation
    error — neither a download nor a format failure. datasluice relies on
    ``pa.concat_tables(promote_options="permissive")``; the hard-fail cases
    (tz-aware vs tz-naive timestamps, struct field mismatch) surface here.
    """
