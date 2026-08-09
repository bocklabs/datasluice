"""TransformStep Protocol + TransformContext model.

The protocol is the closed-set contract every normalization transform implements
. The context is the immutable provenance + schema snapshot threaded
read-only through every step by :class:`~datasluice.transforms.pipeline.Pipeline`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


@dataclass(frozen=True)
class TransformContext:
    """Immutable provenance + schema snapshot.

    Built once at :meth:`Pipeline.run` entry from the input stream's schema and
    threaded read-only through every step. Steps MUST NOT mutate it — the live
    post-transform schema flows only via each step's output
    :class:`~datasluice.data.batch_stream.BatchStream.schema`.

    Attributes:
        arrow_schema: The ``pa.Schema`` of the input stream (provenance snapshot,
            NOT evolving).
        source_resource_id: The ``Resource.id`` the stream was opened from, if
            known, else ``None``.
        source_url: The ``Resource.url`` the stream was opened from, if known,
            else ``None``.
        domain_schema: The advisory portal :class:`~datasluice.domain.Schema` if
            the resource carried one, else ``None``.
    """

    arrow_schema: Any
    source_resource_id: str | None = None
    source_url: str | None = None
    domain_schema: Any | None = None


@runtime_checkable
class TransformStep(Protocol):
    """Closed-set transform protocol.

    Each transform is a frozen-dataclass-configured class implementing
    :meth:`apply`. The protocol exists for internal uniformity and
    ``isinstance``-capable testing; it is NOT a third-party extension point
    (PROJECT.md Out of Scope — the transform set is closed for normalization
    only).

    Args:
        batches: The upstream ``RecordBatch`` iterable (the previous step's
            output, or the raw stream). Steps consume it lazily as a generator.
        context: The immutable :class:`TransformContext` built once at pipeline
            entry. Positional.
    """

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]: ...
