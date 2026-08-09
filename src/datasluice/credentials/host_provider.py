"""Host-scoped credential resolver with single-flight refresh.

``HostCredentialProvider`` implements the existing
:class:`datasluice.ports.credentials.CredentialProvider` Protocol with an
UNCHANGED ``resolve(host) -> BaseAuth`` signature. Expiry, refresh,
and eviction are internal concerns the caller never sees.

Design notes
------------

1. **Protocol unchanged.** Only ``resolve(host)`` is on the port; the
   off-port ``evict(host)`` and the injected ``refresher`` are implementation
   detail (capability-checked via ``isinstance`` by ``HttpxTransport`` on
   401/403, ).

2. **Single-flight refresh.** A per-host ``threading.Lock`` plus
   double-checked expiry guarantees that when N threads hit an expired
   credential simultaneously, exactly ONE ``refresher`` invocation runs and
   every caller receives the fresh result.

3. **AUTH2-01 pluggability seam.** The ``refresher`` callable
   ``Callable[[str], tuple[BaseAuth, datetime | None]]`` is the v2 OAuth
   plug-in point. v1 passes ``refresher=None`` for the three target portals
   (static / APIKey / ``auth=`` wrap) so ``expires_at`` is ``None`` and the
   credential never refreshes.

4. **HTTP-transport-only.** ``HostCredentialProvider`` resolves
   Bearer/Basic/APIKey auth for portal HTTP requests. Object-store credentials
   (S3/GCS/Azure) flow through ``open_filesystem(uri, credentials=)`` and
   fsspec's own resolver separately — never through here.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from datasluice.exceptions import AuthenticationError
from datasluice.logging import get_logger

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth

logger = get_logger("credentials.host_provider")

Refresher = Callable[[str], "tuple[BaseAuth, datetime | None]"]

_DEFAULT_HOST_KEY = "_default"


class HostCredentialProvider:
    """Resolve a :class:`BaseAuth` per host with cached expiry and single-flight refresh.

    Args:
        refresher: Optional callable ``(host) -> (BaseAuth, expires_at | None)``
            invoked when the cached credential for a host is missing or expired.
            When ``None`` (the v1 default), resolve returns the cached auth or
            raises :class:`AuthenticationError`.
    """

    def __init__(self, refresher: Refresher | None = None) -> None:
        self._refresher = refresher
        self._cache: dict[str, tuple[BaseAuth, datetime | None]] = {}
        self._host_locks: dict[str, threading.Lock] = {}
        self._dict_lock = threading.Lock()

    def _get_host_lock(self, host: str) -> threading.Lock:
        """Return the per-host lock, creating it if necessary.

        The dict-level lock is held ONLY for the ``setdefault`` — never for the
        per-host work — so a slow refresh on one host cannot block lock
        acquisition for another (Pattern 4).
        """

        with self._dict_lock:
            return self._host_locks.setdefault(host, threading.Lock())

    @staticmethod
    def _is_expired(expires_at: datetime | None) -> bool:
        """Return whether *expires_at* has passed.

        ``None`` means the credential never expires ( zero-config
        default for static APIKey / ``auth=`` wrap). Naive datetimes are
        assumed to be UTC.
        """

        if expires_at is None:
            return False
        normalized = expires_at if expires_at.tzinfo is not None else expires_at.replace(tzinfo=UTC)
        return datetime.now(UTC) >= normalized

    def resolve(self, host: str | None = None) -> BaseAuth:
        """Return the cached :class:`BaseAuth` for *host*, refreshing if expired.

        Args:
            host: Target host. ``None`` resolves to a sentinel default key and
                is cached independently of named hosts.

        Raises:
            AuthenticationError: When no credential is cached for *host* and
                no ``refresher`` is configured.
        """

        key = host or _DEFAULT_HOST_KEY

        cached = self._cache.get(key)
        if cached is not None and not self._is_expired(cached[1]):
            return cached[0]

        if self._refresher is None:
            if cached is not None:
                return cached[0]
            raise AuthenticationError(f"No credentials for host {key!r} and no refresher configured")

        lock = self._get_host_lock(key)
        with lock:
            cached = self._cache.get(key)
            if cached is not None and not self._is_expired(cached[1]):
                return cached[0]
            auth, expires_at = self._refresher(key)
            self._cache[key] = (auth, expires_at)
            logger.debug("Refreshed credentials for host %r", key)
            return auth

    def evict(self, host: str | None = None) -> None:
        """Drop the cached credential for *host*.

        Called by :class:`HttpxTransport` on a 401/403 response to force the
        next ``resolve`` to call the refresher. Acquires the same per-host lock
        as ``resolve`` so eviction races safely against an in-flight refresh.
        """

        key = host or _DEFAULT_HOST_KEY
        lock = self._get_host_lock(key)
        with lock:
            self._cache.pop(key, None)
        logger.debug("Evicted cached credentials for host %r", key)
