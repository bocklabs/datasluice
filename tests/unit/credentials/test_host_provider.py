"""Unit tests for HostCredentialProvider.

Exercises the per-host credential resolver: CredentialProvider Protocol
conformance, per-host caching, single-flight refresh under N=10 concurrent
callers, expiry-triggered refresh, evict(host) forcing
re-resolution, and the no-refresher AuthenticationError path.

The module is resolved via ``importlib.import_module`` (rather than a static
``import``) so the RED commit can land under this repo's full-suite pre-commit
hook: until the implementation in the GREEN step ships, the whole module
skips cleanly instead of erroring at collection (same pattern as the
``test_httpx_transport.py`` RED commit in plan 03-01).
"""

from __future__ import annotations

import concurrent.futures
import importlib
import threading
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

pytest.importorskip("datasluice.credentials.host_provider")
try:
    _host_provider_module = importlib.import_module("datasluice.credentials.host_provider")
except ImportError:
    pytest.skip(
        "HostCredentialProvider implementation pending (RED -> GREEN within task 03-04)",
        allow_module_level=True,
    )
HostCredentialProvider = _host_provider_module.HostCredentialProvider

from datasluice.auth import BearerAuth, NoAuth  # noqa: E402
from datasluice.exceptions import AuthenticationError  # noqa: E402
from datasluice.ports import CredentialProvider  # noqa: E402

# --------------------------------------------------------------------------- #
# CredentialProvider Protocol conformance
# --------------------------------------------------------------------------- #


def test_host_provider_satisfies_credential_provider_protocol() -> None:
    provider = HostCredentialProvider(refresher=lambda host: (NoAuth(), None))
    assert isinstance(provider, CredentialProvider)


# --------------------------------------------------------------------------- #
# Per-host caching + refresher invocation
# --------------------------------------------------------------------------- #


def test_resolve_returns_cached_auth_per_host() -> None:
    refresher = MagicMock(side_effect=[(BearerAuth("token-a"), None), (BearerAuth("token-b"), None)])
    provider = HostCredentialProvider(refresher=refresher)

    auth_a = provider.resolve("a.com")
    auth_b = provider.resolve("b.com")

    assert isinstance(auth_a, BearerAuth)
    assert auth_a.token == "token-a"
    assert isinstance(auth_b, BearerAuth)
    assert auth_b.token == "token-b"


def test_resolve_uses_refresher_on_first_call() -> None:
    refresher = MagicMock(return_value=(BearerAuth("t"), None))
    provider = HostCredentialProvider(refresher=refresher)

    provider.resolve("host")

    assert refresher.call_count == 1


def test_resolve_caches_after_first_call() -> None:
    refresher = MagicMock(return_value=(BearerAuth("t"), None))
    provider = HostCredentialProvider(refresher=refresher)

    first = provider.resolve("host")
    second = provider.resolve("host")

    assert first is second
    assert refresher.call_count == 1


# --------------------------------------------------------------------------- #
# Single-flight refresh
# --------------------------------------------------------------------------- #


def test_single_flight_concurrent_resolve_calls_one_refresh() -> None:
    """N=10 concurrent resolves of an expired host trigger exactly ONE refresh."""

    def slow_refresher(host: str):
        time.sleep(0.1)
        return (BearerAuth("refreshed"), None)

    import time

    refresher = MagicMock(side_effect=slow_refresher)
    provider = HostCredentialProvider(refresher=refresher)
    provider._cache["expired-host"] = (BearerAuth("stale"), datetime.now(UTC) - timedelta(seconds=10))

    n_threads = 10
    barrier = threading.Barrier(n_threads)
    results: list[object] = []
    results_lock = threading.Lock()

    def resolve_once() -> None:
        barrier.wait()
        auth = provider.resolve("expired-host")
        with results_lock:
            results.append(auth)

    with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = [executor.submit(resolve_once) for _ in range(n_threads)]
        concurrent.futures.wait(futures)
        for fut in futures:
            fut.result()

    assert refresher.call_count == 1, f"expected exactly one refresh, got {refresher.call_count}"
    assert len(results) == n_threads
    first = results[0]
    for r in results:
        assert r is first, "all threads must receive the SAME refreshed auth instance"


# --------------------------------------------------------------------------- #
# Expiry handling
# --------------------------------------------------------------------------- #


def test_expiry_triggers_refresh_on_next_resolve() -> None:
    refresher = MagicMock(return_value=(BearerAuth("fresh"), datetime.now(UTC) + timedelta(hours=1)))
    provider = HostCredentialProvider(refresher=refresher)
    provider._cache["host"] = (BearerAuth("stale"), datetime.now(UTC) - timedelta(seconds=10))

    auth = provider.resolve("host")

    assert refresher.call_count == 1
    assert isinstance(auth, BearerAuth)
    assert auth.token == "fresh"
    _, stored_expiry = provider._cache["host"]
    assert stored_expiry is not None
    assert stored_expiry > datetime.now(UTC)


def test_no_expiry_never_refreshes() -> None:
    """expires_at=None never triggers refresh."""

    refresher = MagicMock(return_value=(BearerAuth("static"), None))
    provider = HostCredentialProvider(refresher=refresher)
    provider._cache["host"] = (BearerAuth("static"), None)

    auth = provider.resolve("host")

    assert refresher.call_count == 0
    assert isinstance(auth, BearerAuth)
    assert auth.token == "static"


# --------------------------------------------------------------------------- #
# Eviction
# --------------------------------------------------------------------------- #


def test_evict_forces_re_resolution() -> None:
    refresher = MagicMock(return_value=(BearerAuth("t"), None))
    provider = HostCredentialProvider(refresher=refresher)

    provider.resolve("host")
    assert refresher.call_count == 1
    provider.evict("host")
    provider.resolve("host")

    assert refresher.call_count == 2


def test_evict_during_in_flight_refresh_safe() -> None:
    """Evict mid-refresh does not deadlock; next resolve triggers a new refresh."""

    refresh_started = threading.Event()
    refresh_can_finish = threading.Event()

    def gated_refresher(host: str):
        refresh_started.set()
        refresh_can_finish.wait(timeout=5.0)
        return (BearerAuth("v1"), None)

    refresher = MagicMock(side_effect=gated_refresher)
    provider = HostCredentialProvider(refresher=refresher)

    resolve_done = threading.Event()

    def resolve_first() -> None:
        provider.resolve("fresh-host")
        resolve_done.set()

    t = threading.Thread(target=resolve_first)
    t.start()
    assert refresh_started.wait(timeout=5.0)

    # Evict a *different* host concurrently (different per-host lock, no contention).
    provider.evict("other-host")
    refresh_can_finish.set()
    assert resolve_done.wait(timeout=5.0)
    t.join()

    # Now evict the resolved host and resolve again -> a new refresh fires.
    second_refresher = MagicMock(return_value=(BearerAuth("v2"), None))
    provider._refresher = second_refresher
    provider.evict("fresh-host")
    auth = provider.resolve("fresh-host")

    assert isinstance(auth, BearerAuth)
    assert auth.token == "v2"
    assert second_refresher.call_count == 1


# --------------------------------------------------------------------------- #
# No-refresher paths
# --------------------------------------------------------------------------- #


def test_no_refresher_raises_authentication_error() -> None:
    provider = HostCredentialProvider(refresher=None)
    with pytest.raises(AuthenticationError):
        provider.resolve("never-seen")


def test_no_refresher_returns_cached_even_if_expired() -> None:
    """zero-config default: cache hit takes precedence over expiry when no refresher."""

    provider = HostCredentialProvider(refresher=None)
    cached_auth = BearerAuth("cached")
    provider._cache["host"] = (cached_auth, datetime.now(UTC) - timedelta(seconds=10))

    auth = provider.resolve("host")

    assert auth is cached_auth


# --------------------------------------------------------------------------- #
# Default host sentinel
# --------------------------------------------------------------------------- #


def test_resolve_default_host_when_none() -> None:
    """resolve() with host=None uses a sentinel key, cached independently of named hosts."""

    refresher = MagicMock(return_value=(BearerAuth("default"), None))
    provider = HostCredentialProvider(refresher=refresher)

    first = provider.resolve()
    second = provider.resolve()

    assert first is second
    assert refresher.call_count == 1


def test_refresher_failure_propagates_and_allows_retry() -> None:
    """When the refresher raises, the error propagates and a later resolve retries."""

    calls = [0]

    def flaky_refresher(host: str):
        calls[0] += 1
        if calls[0] == 1:
            raise RuntimeError("credential service down")
        return (BearerAuth("recovered"), None)

    refresher = MagicMock(side_effect=flaky_refresher)
    provider = HostCredentialProvider(refresher=refresher)

    with pytest.raises(RuntimeError, match="credential service down"):
        provider.resolve("failing-host")

    auth = provider.resolve("failing-host")
    assert cast(Any, auth).token == "recovered"
    assert refresher.call_count == 2
