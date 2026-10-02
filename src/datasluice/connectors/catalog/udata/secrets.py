"""Reveal-once boundary for newly created uData API tokens."""

from __future__ import annotations

from threading import Lock
from typing import SupportsIndex


class OneTimeUDataToken:
    """Hold plaintext only until the caller explicitly reveals it once.

    Reveal and discard are the only two ways to release the plaintext, and
    both are serialised so that exactly one caller ever receives the token
    even when several threads race on the same holder.
    """

    __slots__ = ("_lock", "_value")

    def __init__(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("A newly created uData token must be a non-empty string.")
        self._value: str | None = value
        self._lock = Lock()

    def reveal_once(self) -> str:
        """Return the token to exactly one caller and clear this holder."""
        with self._lock:
            value = self._value
            if value is None:
                raise RuntimeError("The uData token has already been revealed.")
            self._value = None
            return value

    def discard(self) -> None:
        """Drop the plaintext without revealing it, leaving ``reveal_once`` to fail."""
        with self._lock:
            self._value = None

    def _discard(self) -> None:
        """Alias of :meth:`discard` retained for callers not yet migrated."""
        self.discard()

    def __repr__(self) -> str:
        return "OneTimeUDataToken(***)"

    def __str__(self) -> str:
        return "***"

    def __reduce__(self) -> tuple[object, ...]:
        raise TypeError("One-time uData tokens cannot be serialized or copied.")

    def __reduce_ex__(self, protocol: SupportsIndex, /) -> tuple[object, ...]:
        raise TypeError("One-time uData tokens cannot be serialized or copied.")
