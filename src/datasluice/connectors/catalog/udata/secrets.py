"""Reveal-once boundary for newly created uData API tokens."""

from __future__ import annotations

from typing import SupportsIndex


class OneTimeUDataToken:
    """Hold plaintext only until the caller explicitly reveals it once."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("A newly created uData token must be a non-empty string.")
        self._value: str | None = value

    def reveal_once(self) -> str:
        """Return the token exactly once and clear this holder."""
        value = self._value
        if value is None:
            raise RuntimeError("The uData token has already been revealed.")
        self._value = None
        return value

    def _discard(self) -> None:
        self._value = None

    def __repr__(self) -> str:
        return "OneTimeUDataToken(***)"

    def __str__(self) -> str:
        return "***"

    def __reduce__(self) -> tuple[object, ...]:
        raise TypeError("One-time uData tokens cannot be serialized or copied.")

    def __reduce_ex__(self, protocol: SupportsIndex, /) -> tuple[object, ...]:
        raise TypeError("One-time uData tokens cannot be serialized or copied.")
