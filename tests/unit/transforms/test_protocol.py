"""Unit tests for the TransformStep Protocol.

Verifies the protocol is ``@runtime_checkable`` (``isinstance`` works against any
apply-bearing object and rejects plain objects) and that ``apply`` carries a
positional ``context`` parameter.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

pytest.importorskip("pyarrow")

from datasluice.transforms import TransformContext, TransformStep

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


class _PassThrough:
    """Minimal object satisfying the TransformStep Protocol."""

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        yield from batches


def test_transform_step_runtime_checkable_satisfied() -> None:
    """An object with an apply method satisfies the TransformStep Protocol."""
    assert isinstance(_PassThrough(), TransformStep) is True


def test_transform_step_runtime_checkable_rejected() -> None:
    """A plain object without apply does not satisfy the TransformStep Protocol."""
    assert isinstance(object(), TransformStep) is False


def test_apply_signature_is_positional_context() -> None:
    """apply takes (self, batches, context) — positional context."""
    import inspect

    params = list(inspect.signature(TransformStep.apply).parameters)
    assert params == ["self", "batches", "context"]
