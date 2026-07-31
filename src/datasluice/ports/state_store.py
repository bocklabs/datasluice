"""State store port Protocols for incremental sync state (SYNC-01).

The base :class:`StateStore` Protocol is the persistence contract every store
must satisfy. :class:`AtomicStateStore` is an *additive capability* Protocol:
stores that support compare-and-swap (CAS) writes opt in by also implementing
``read_version`` and ``conditional_put``. Stores that do not need CAS
(:class:`datasluice.sync.state_store.InMemoryStateStore`, external implementors)
remain structurally valid against :class:`StateStore` alone.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datasluice.domain import SyncState


@runtime_checkable
class StateStore(Protocol):
    """Boundary protocol for persisting incremental sync state."""

    def get(self, key: str) -> SyncState | None: ...

    def put(self, key: str, state: SyncState) -> None: ...

    def delete(self, key: str) -> None: ...


@runtime_checkable
class AtomicStateStore(Protocol):
    """Additive capability Protocol for compare-and-swap (CAS) state writes (CR-02/11).

    A :class:`StateStore` that *also* satisfies this Protocol can publish state
    transitions conditionally: a prior version is read via :meth:`read_version`
    and passed back as ``expected_prior`` to :meth:`conditional_put`, so a
    concurrent writer's intervening commit is detected (raising
    :class:`datasluice.exceptions.SyncStateConflictError`) rather than silently
    overwritten. Implementors make the compare-read and the atomic-move
    indivisible within a single process (per-key lock) and declare which fsspec
    backends provide a true atomic rename.

    This is a separate Protocol from :class:`StateStore` so stores that do not
    need CAS remain valid without it — following the established capability-
    Protocol pattern (``StreamingTransport`` / ``ConditionalTransport`` /
    ``CheckpointableResourceReader`` are all separate additive Protocols).
    """

    def read_version(self, key: str) -> bytes | None:
        """Return the raw envelope bytes for *key*, or ``None`` if absent.

        The returned bytes are opaque to the caller — they are passed verbatim
        back into :meth:`conditional_put` as ``expected_prior`` so a concurrent
        write between the read and the put is detected.
        """
        ...

    def conditional_put(self, key: str, state: SyncState, expected_prior: bytes | None) -> None:
        """Persist *state* under *key* only if the current version matches ``expected_prior``.

        Args:
            key: The sync-state key.
            state: The :class:`SyncState` to persist.
            expected_prior: Raw bytes from a prior :meth:`read_version` call,
                or ``None`` meaning "expected absent".

        Raises:
            SyncStateConflictError: if the current on-disk version does not match
                ``expected_prior`` (CAS lost a race).
        """
        ...
