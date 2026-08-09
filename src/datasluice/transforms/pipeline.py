"""Pipeline runner + compose factory.

The :class:`Pipeline` threads a list of :class:`~datasluice.transforms.protocol.TransformStep`
over a :class:`~datasluice.data.batch_stream.BatchStream`, building the
:class:`~datasluice.transforms.protocol.TransformContext` once at entry and
wrapping the final transformed iterator back into a NEW ``BatchStream`` whose
schema reflects the transformed batches.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from datasluice.data.batch_stream import BatchStream
from datasluice.transforms.protocol import TransformContext, TransformStep

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


class Pipeline:
    """Reusable transform pipeline.

    A :class:`Pipeline` is a first-class, reusable value: ``compose([steps])``
    returns one, and ``.run(stream)`` returns a NEW
    :class:`~datasluice.data.batch_stream.BatchStream`. The same pipeline may be
    applied to many streams.

    Attributes:
        steps: Introspectable list of :class:`TransformStep`. A defensive copy
            is taken at construction so caller mutations of the source list do
            not affect the pipeline.
    """

    steps: list[TransformStep]

    def __init__(self, steps: list[TransformStep]) -> None:
        self.steps = list(steps)

    def run(self, stream: BatchStream) -> BatchStream:
        """Build the context once, thread every step, wrap output in a NEW BatchStream.

                The :class:`TransformContext` is constructed from ``stream.schema`` at
                entry and threaded read-only through every step. Steps compose
                left-to-right: each step's ``apply`` receives the previous step's output
                iterator — no intermediate materialization. The output
                ``BatchStream.schema`` is derived by peeking the first transformed batch
                so it matches the post-transform schema, not the stale input schema
        .

                Args:
                    stream: The input :class:`BatchStream`.

                Returns:
                    A NEW :class:`BatchStream` over the transformed batches.
        """
        context = TransformContext(arrow_schema=stream.schema)
        batches: Iterable[Any] = stream.iter_batches()
        for step in self.steps:
            batches = step.apply(batches, context)
        return _build_batch_stream(batches)

    def __repr__(self) -> str:
        return f"Pipeline({self.steps!r})"


def compose(steps: list[TransformStep]) -> Pipeline:
    """Build a reusable :class:`Pipeline` from an ordered list of transforms.

    Args:
        steps: The transforms to apply, in left-to-right execution order.

    Returns:
        A :class:`Pipeline` whose ``.run`` threads the steps over any stream.
    """
    return Pipeline(steps)


def _build_batch_stream(batches: Iterator[Any]) -> BatchStream:
    """Peek the first transformed batch to derive the post-transform schema, then wrap.

    Mirrors :func:`datasluice.data.access._build_batch_stream` (lines 149–159)
    verbatim in shape. This is the fix: the output ``BatchStream.schema``
    MUST equal the first transformed batch's schema, not the input stream's
    schema, because transforms like ``SelectColumns``/``CastSchema`` change it.

    Args:
        batches: The fully-threaded transformed iterator (all steps applied).

    Returns:
        A :class:`BatchStream` over the transformed batches with a correct schema.
    """
    first_batch = next(batches, None)
    if first_batch is None:
        import pyarrow as pa

        schema = pa.schema([])
        batch_iter: Iterator[Any] = iter(())
    else:
        schema = first_batch.schema
        batch_iter = _chain(first_batch, batches)
    return BatchStream(batch_iter, schema)


def _chain(first: Any, rest: Iterator[Any]) -> Iterator[Any]:
    """Yield ``first`` then all of ``rest`` (re-inserts a peeked batch)."""
    yield first
    yield from rest
