"""Exception hierarchy for DataSluice."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datasluice.domain import DetectionResult


class DataSluiceError(Exception):
    """Base exception for all DataSluice errors."""


class ResourceResolutionError(DataSluiceError):
    """Raised when a public resource locator cannot select exactly one resource."""


class OpenedResourceConsumedError(DataSluiceError):
    """Raised when an opened resource is consumed or closed more than once."""


class PortalError(DataSluiceError):
    """Raised when a portal returns an error or is unreachable."""


class AdapterError(DataSluiceError):
    """Raised when an adapter cannot fulfil a request."""


class AdapterNotFoundError(AdapterError):
    """Raised when no adapter is registered for a portal type."""


class PortalDetectionError(DataSluiceError):
    """Raised when the portal type cannot be auto-detected.

    Attributes:
        detection_result: Optional :class:`DetectionResult` that triggered the
            failure. Carries the evidence trail so -03's
            ``session.portal()`` can surface why detection failed without
            re-running it. Backward-compatible: defaults to ``None``.
    """

    def __init__(self, message: str, detection_result: DetectionResult | None = None) -> None:
        super().__init__(message)
        self.detection_result = detection_result


class AuthenticationError(DataSluiceError):
    """Raised when authentication credentials are missing or invalid."""


class RateLimitError(PortalError):
    """Raised when the portal rate-limits requests."""

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class RetryableHTTPError(PortalError):
    """Raised on HTTP 5xx responses, or transport-level failures, that should be retried.

    ``status_code`` defaults to ``0`` for transport-level errors (timeouts, connection
    failures) that have no HTTP response status.
    """

    def __init__(self, message: str, status_code: int = 0) -> None:
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
    """Raised when a compressed resource cannot be decompressed.

    Compression is format-adjacent: a decompression failure means the bytes
    could not be decoded into the underlying format, so it hangs off
    :class:`FormatError`.
    """


class TransformError(FormatError):
    """Raised when a transform step cannot be applied.

        Transform failures are data-shape/decoding failures (an unsafe cast, a
        missing column, an unsupported nesting), so they hang off
        :class:`FormatError` — matching the :class:`DecompressionError` precedent
    . No new exception root.
    """


class ConfigError(DataSluiceError):
    """Raised when configuration is invalid or incomplete."""


class StateStoreError(DataSluiceError):
    """Raised when a state store cannot read or write durable sync state.

    A direct child of :class:`DataSluiceError` because store I/O failures are
    neither portal, download, nor format errors: a corrupt or wrong-version
    JSON envelope is a data-integrity failure of the local/remote state file.
    Fails loud (never silently treats corrupt state as "no state") because
    staleness is worse than a loud failure.
    """


class SyncStateConflictError(StateStoreError):
    """Raised when a state write loses an optimistic compare-and-swap race.

    The version read before the write had already been replaced by a
    concurrent writer; the caller must re-read and re-apply their mutation
    rather than silently overwrite another writer's state.
    """


class UnsupportedAccessError(DownloadError):
    """Raised when a resource's access kind has no reader implementation.

    implements HttpDownload, ObjectStorage, and LocalFile readers.
    QueryAccess raises this error; adds query readers.
    """


class StreamClosedError(DataSluiceError):
    """Raised when operating on a closed :class:`BatchStream`.

    A direct child of :class:`DataSluiceError` because it is an operational
    use-after-close error — the download already succeeded and the format is
    fine; the caller used the stream after ``__exit__`` or an explicit
    :meth:`BatchStream.close`.
    """


class SchemaUnificationError(DataSluiceError):
    """Raised when batch schemas cannot be unified under pyarrow promotion.

    A direct child of :class:`DataSluiceError` because it is a data-reconciliation
    error — neither a download nor a format failure. datasluice relies on
    ``pa.concat_tables(promote_options="permissive")``; the hard-fail cases
    (tz-aware vs tz-naive timestamps, struct field mismatch) surface here.
    """


class UnsupportedQueryFieldError(DataSluiceError):
    """Raised when a caller sets a ``Query`` filter field the connector rejects.

    A direct child of :class:`DataSluiceError` (sibling to :class:`AdapterError`),
    NOT under :class:`PortalError`: the reject policy fires pre-flight, before
    any portal contact, so the portal never returned an error.

    Attributes:
        field: The unsupported filter field name (e.g. ``"groups"``).
        supported_fields: Sorted list of supported alternatives, read from
            :class:`CatalogCapabilities.supported_query_fields`.
        portal_name: Canonical portal name (e.g. ``"datagouv"``).
    """

    def __init__(self, *, field: str, supported_fields: list[str], portal_name: str) -> None:
        alts = ", ".join(supported_fields) if supported_fields else "(none)"
        message = f"Field {field!r} is not supported by the {portal_name} connector. Supported filter fields: {alts}."
        super().__init__(message)
        self.field = field
        self.supported_fields = supported_fields
        self.portal_name = portal_name
