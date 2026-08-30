"""Strict anonymous uData version gate resolving the pinned 17.6 contract."""

from __future__ import annotations

import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic
from typing import Literal

from datasluice.connectors.catalog.udata.settings import normalize_origin
from datasluice.domain.catalog.auth import credential_scope
from datasluice.errors.catalog import CatalogUnavailableError, NativeCatalogError, map_catalog_error
from datasluice.runtime.clients import AsyncCatalogTransport
from datasluice.runtime.transport.base import CatalogTransport, RuntimeRequest

SITE_PATH = "/api/1/site/"
SITE_OPERATION_ID = "udata/api-v1.root-and-effective-profile-probe"
_VERSION_PATTERN = re.compile(r"\d+\.\d+\.\d+\Z")
_SITE_KEYS = frozenset({"feed_size", "id", "keywords", "metrics", "title", "version"})
_SITE_PROBE_MAX_BYTES = 64 * 1024
_VERSION_STATE = Literal["exact", "missing", "malformed", "ambiguous"]


class UDataVersionError(CatalogUnavailableError):
    """The deployment did not prove the exact pinned uData API version."""

    def __init__(
        self,
        *,
        version_state: str,
        message: str,
        safe_action: str,
    ) -> None:
        """Record the bounded version state and its typed caller remedy."""
        super().__init__(
            message,
            operation=SITE_OPERATION_ID,
            platform="udata",
            capability_state="unavailable",
            metadata={"version_state": version_state},
            safe_action=safe_action,
        )
        self.version_state = version_state


@dataclass(frozen=True, slots=True)
class SiteVersion:
    """The bounded allowlisted site-probe evidence retained by the gate."""

    version: str
    site_id: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not _VERSION_PATTERN.fullmatch(self.version):
            raise ValueError("Site version evidence must be a strict semantic version string.")
        if self.site_id is not None and (not isinstance(self.site_id, str) or not self.site_id):
            raise ValueError("Site identity evidence must be a non-empty string when supplied.")


def parse_site_version(payload: object) -> SiteVersion:
    """Classify one site payload against the exact pinned version contract.

    Args:
        payload: The decoded JSON body of ``GET /api/1/site/``.

    Returns:
        The bounded site evidence when the payload proves the exact version.

    Raises:
        UDataVersionError: When the version key is missing, malformed, or
            ambiguous. The error carries the bounded ``version_state`` and a
            typed caller remedy.
    """
    if not isinstance(payload, dict):
        raise UDataVersionError(
            version_state="malformed",
            message="The uData site probe did not return a JSON object payload.",
            safe_action="Retry against a stock uData deployment serving /api/1/site/ JSON.",
        )
    unknown_version_shaped = [
        key
        for key, value in payload.items()
        if key in _SITE_KEYS and key != "version" and isinstance(value, str) and _VERSION_PATTERN.fullmatch(value)
    ]
    version = payload.get("version")
    if "version" not in payload:
        raise UDataVersionError(
            version_state="missing",
            message="The uData site probe response omitted the version field.",
            safe_action="Verify the deployment exposes the stock /api/1/site/ document before dispatching.",
        )
    if unknown_version_shaped:
        raise UDataVersionError(
            version_state="ambiguous",
            message="The uData site probe response carries conflicting version evidence.",
            safe_action="Verify the deployment serves an unmodified stock /api/1/site/ document.",
        )
    if not isinstance(version, str) or not _VERSION_PATTERN.fullmatch(version):
        raise UDataVersionError(
            version_state="malformed",
            message="The uData site probe response carries a malformed version value.",
            safe_action="Verify the deployment serves the stock /api/1/site/ document before dispatching.",
        )
    site_id = payload.get("id")
    return SiteVersion(
        version=version,
        site_id=site_id if isinstance(site_id, str) and site_id else None,
    )


def require_exact_version(observed: SiteVersion, pinned_version: str) -> SiteVersion:
    """Reject any observed version that is not the exact pinned release.

    Raises:
        UDataVersionError: When the observed version differs from the pin.
    """
    if observed.version != pinned_version:
        raise UDataVersionError(
            version_state="malformed",
            message=f"The uData deployment reported {observed.version}, not the pinned {pinned_version}.",
            safe_action=(
                "Point the connector at a deployment running the exact pinned uData release; "
                "version drift requires a reviewed capability profile change."
            ),
        )
    return observed


class SiteVersionGate:
    """Anonymous-first exact-version gate caching one evidence entry per caller identity.

    The gate never resolves or attaches caller credentials: the probe request
    carries no authorization headers even when a credential is injected, and
    evidence is cached per credential identity with a bounded TTL.
    """

    def __init__(
        self,
        *,
        pinned_version: str,
        origin: str,
        transport: CatalogTransport | None,
        ttl_seconds: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        """Pin the gate to one origin, transport, and evidence TTL."""
        self._pinned_version = pinned_version
        self._origin = normalize_origin(origin)
        self._transport = transport
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._cache: dict[str, tuple[SiteVersion, float]] = {}

    def require_current(self, credentials: object | None) -> SiteVersion:
        """Return cached evidence or dispatch one anonymous site probe.

        Raises:
            UDataVersionError: When the deployment fails the exact-version gate.
            NativeCatalogError: When the probe transport or JSON decode fails.
        """
        scope = credential_scope(credentials)
        with self._lock:
            cached = self._cache.get(scope)
            if cached is not None and self._clock() - cached[1] <= self._ttl_seconds:
                return cached[0]
        observed = self._probe()
        with self._lock:
            self._cache[scope] = (observed, self._clock())
        return observed

    def invalidate(self) -> None:
        """Discard all cached site-version evidence."""
        with self._lock:
            self._cache.clear()

    def _probe(self) -> SiteVersion:
        if self._transport is None:
            raise RuntimeError("The strict site gate requires a synchronous transport.")
        request = RuntimeRequest(
            method="GET",
            url=f"{self._origin}{SITE_PATH}",
            headers={},
            body=None,
            max_response_bytes=_SITE_PROBE_MAX_BYTES,
        )
        try:
            response = self._transport.send(request)
        except Exception as exc:
            raise map_catalog_error(
                NativeCatalogError(
                    "The uData site probe transport failed.",
                    operation=SITE_OPERATION_ID,
                    platform="udata",
                )
            ) from exc
        if not 200 <= response.status_code < 300:
            raise UDataVersionError(
                version_state="missing",
                message=f"The uData site probe returned HTTP {response.status_code}.",
                safe_action="Verify the deployment origin exposes the stock /api/1/site/ document.",
            )
        import json

        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise UDataVersionError(
                version_state="malformed",
                message="The uData site probe returned an invalid JSON result.",
                safe_action="Verify the deployment serves the stock /api/1/site/ JSON document.",
            ) from exc
        return require_exact_version(parse_site_version(payload), self._pinned_version)


class AsyncSiteVersionGate(SiteVersionGate):
    """Async transport variant of the strict site gate for the async client."""

    def __init__(
        self,
        *,
        pinned_version: str,
        origin: str,
        transport: AsyncCatalogTransport,
        ttl_seconds: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        """Pin the async gate to one async transport."""
        super().__init__(
            pinned_version=pinned_version,
            origin=origin,
            transport=None,
            ttl_seconds=ttl_seconds,
            clock=clock,
        )
        self._async_transport = transport

    async def require_current_async(self, credentials: object | None) -> SiteVersion:
        """Return cached evidence or dispatch one anonymous async site probe."""
        scope = credential_scope(credentials)
        with self._lock:
            cached = self._cache.get(scope)
            if cached is not None and self._clock() - cached[1] <= self._ttl_seconds:
                return cached[0]
        observed = await self._probe_async()
        with self._lock:
            self._cache[scope] = (observed, self._clock())
        return observed

    async def _probe_async(self) -> SiteVersion:
        import json

        request = RuntimeRequest(
            method="GET",
            url=f"{self._origin}{SITE_PATH}",
            headers={},
            body=None,
            max_response_bytes=_SITE_PROBE_MAX_BYTES,
        )
        try:
            response = await self._async_transport.send(request)
        except Exception as exc:
            raise map_catalog_error(
                NativeCatalogError(
                    "The uData site probe transport failed.",
                    operation=SITE_OPERATION_ID,
                    platform="udata",
                )
            ) from exc
        if not 200 <= response.status_code < 300:
            raise UDataVersionError(
                version_state="missing",
                message=f"The uData site probe returned HTTP {response.status_code}.",
                safe_action="Verify the deployment origin exposes the stock /api/1/site/ document.",
            )
        try:
            payload = json.loads(response.body)
        except (TypeError, ValueError) as exc:
            raise UDataVersionError(
                version_state="malformed",
                message="The uData site probe returned an invalid JSON result.",
                safe_action="Verify the deployment serves the stock /api/1/site/ JSON document.",
            ) from exc
        return require_exact_version(parse_site_version(payload), self._pinned_version)
