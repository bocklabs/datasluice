"""Closed-set normalization transforms.

Each transform is a frozen-dataclass-configured class implementing
:meth:`~datasluice.transforms.protocol.TransformStep.apply` as a generator
yielding transformed ``RecordBatch`` objects lazily (O(1 batch memory),
). Every hard operation delegates to pyarrow compute — never hand-rolled
. The set is CLOSED for normalization
only (PROJECT.md Out of Scope — not a plugin extension point).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from datasluice.transforms.protocol import TransformContext


@dataclass(frozen=True)
class Filter:
    """Filter rows using a pyarrow compute ``Expression``.

    Delegates per-batch to ``RecordBatch.filter`` (native Arrow, zero-copy,
    vectorized). Users build expressions with ``pyarrow.compute.field`` /
    arithmetic / boolean operators; no datasluice DSL to maintain.

    Example:
        ``Filter(pc.field("year") > 2020)``

    Attributes:
        expression: A ``pyarrow.compute.Expression`` applied to each batch.
    """

    expression: Any

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        """Yield each batch filtered by ``self.expression``."""
        for batch in batches:
            yield batch.filter(self.expression)


@dataclass(frozen=True)
class SelectColumns:
    """Project a subset (or re-ordering) of columns.

    Raises ``TransformError`` naming the missing column(s) AND the available
    ones when a requested column is absent (actionable auto-generated message,
    mirrors :class:`~datasluice.exceptions.UnsupportedQueryFieldError`). Empty
    selection is rejected at construction.

    Attributes:
        columns: Column names to keep, in output order.
    """

    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError("SelectColumns requires at least one column name")

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        """Yield batches containing only ``self.columns`` in the requested order."""
        available = set(context.arrow_schema.names)
        missing = [c for c in self.columns if c not in available]
        if missing:
            from datasluice.exceptions import TransformError

            raise TransformError(
                f"Column(s) {missing!r} not found in schema. Available columns: {', '.join(sorted(available))}."
            )
        for batch in batches:
            yield batch.select(list(self.columns))


@dataclass(frozen=True)
class RenameColumns:
    """Rename columns via an old→new mapping.

    Raises ``TransformError`` naming the missing source column(s) AND the
    available ones when a mapped source is absent. Names not in the mapping
    pass through unchanged.

    Attributes:
        mapping: ``{old_name: new_name}`` for each column to rename.
    """

    mapping: dict[str, str]

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        """Yield batches with source columns renamed to their targets."""
        available = set(context.arrow_schema.names)
        missing = [old for old in self.mapping if old not in available]
        if missing:
            from datasluice.exceptions import TransformError

            raise TransformError(
                f"Source column(s) {missing!r} not found in schema. Available columns: {', '.join(sorted(available))}."
            )
        for batch in batches:
            yield batch.rename_columns([self.mapping.get(n, n) for n in batch.schema.names])


@dataclass(frozen=True)
class CastSchema:
    """Cast each batch to a target Arrow schema, strictly.

        Uses ``Table.cast(target_schema, safe=True)``: an unsafe cast (overflow /
        truncation) raises :class:`pyarrow.ArrowInvalid`, wrapped as
        :class:`~datasluice.exceptions.TransformError`. No silent data loss
    .

        Attributes:
            target_schema: The ``pa.Schema`` every batch is cast to.
    """

    target_schema: Any

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        """Yield batches cast to ``self.target_schema`` (safe=True)."""
        import pyarrow as pa

        for batch in batches:
            table = pa.Table.from_batches([batch])
            try:
                casted = table.cast(self.target_schema, safe=True)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
                from datasluice.exceptions import TransformError

                raise TransformError(f"Unsafe cast to target schema: {exc}") from exc
            yield from casted.to_batches()


@dataclass(frozen=True)
class NormalizeTimestamps:
    """Normalize every timestamp column to a canonical type.

    Handles three branches per timestamp column:

    1. tz-naive → ``assume_timezone`` then cast (NEVER a direct naive→aware
       cast, which raises ``ArrowInvalid`` — ).
    2. tz-aware, different zone → cast to the target tz.
    3. same tz, different unit → cast to the target unit.

    Non-timestamp columns pass through unchanged. The output schema is rebuilt
    with :meth:`pyarrow.Field.with_type` so field metadata (nullable, etc.) is
    preserved while the timestamp type reflects the canonical target.

    Attributes:
        target_tz: Target IANA timezone (default ``"UTC"``).
        assume_naive_tz: Timezone assumed for tz-naive columns (default ``"UTC"``).
        target_unit: Target timestamp resolution (default ``"us"``).
    """

    target_tz: str = "UTC"
    assume_naive_tz: str = "UTC"
    target_unit: str = "us"

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        """Yield batches whose timestamp columns are the canonical target type."""
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.types as pt

        assume_timezone: Any = pc.__dict__["assume_timezone"]
        target_type = pa.timestamp(self.target_unit, tz=self.target_tz)
        for batch in batches:
            new_fields: list[Any] = []
            new_columns: list[Any] = []
            for field in batch.schema:
                col = batch.column(field.name)
                if pt.is_timestamp(field.type):
                    try:
                        if field.type.tz is None:
                            col = pc.cast(assume_timezone(col, self.assume_naive_tz), target_type, safe=True)
                        elif field.type.tz != self.target_tz:
                            col = pc.cast(col, target_type, safe=True)
                        elif field.type.unit != self.target_unit:
                            col = pc.cast(col, target_type, safe=True)
                    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
                        from datasluice.exceptions import TransformError

                        raise TransformError(
                            f"Timestamp normalization failed for {field.name!r} (possible DST fold/gap): {exc}"
                        ) from exc
                    new_fields.append(field.with_type(col.type))
                else:
                    new_fields.append(field)
                new_columns.append(col)
            yield pa.RecordBatch.from_arrays(new_columns, schema=pa.schema(new_fields))


@dataclass(frozen=True)
class Flatten:
    """Flatten struct columns into dotted-name columns.

    Each struct field ``address{city, zip}`` becomes ``address.city``,
    ``address.zip``. ``max_depth`` controls recursion depth (default one level;
    ``max_depth > 1`` recurses into nested structs). List columns are LEFT
    UNTOUCHED (documented policy — exploding lists is out of scope for the
    closed normalization set).

    ``RecordBatch`` has no ``.flatten``, so each batch is
    rebuilt via ``pa.Table`` + :func:`pyarrow.compute.struct_field`. A no-struct
    batch is yielded unchanged.

    Attributes:
        max_depth: How many struct levels to flatten (default ``1``).
        separator: Joiner between parent and child field names (default ``"."``).
    """

    max_depth: int = 1
    separator: str = "."

    def __post_init__(self) -> None:
        if self.max_depth < 1:
            raise ValueError(f"max_depth must be >= 1, got {self.max_depth}")

    def apply(self, batches: Iterable[Any], context: TransformContext) -> Iterator[Any]:
        """Yield batches whose struct fields are flattened up to ``self.max_depth``."""
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.types as pt

        struct_field: Any = pc.__dict__["struct_field"]
        for batch in batches:
            table = pa.Table.from_batches([batch])
            for _ in range(self.max_depth):
                if not any(pt.is_struct(f.type) for f in table.schema):
                    break
                new_fields: list[Any] = []
                new_columns: list[Any] = []
                for field in table.schema:
                    if pt.is_struct(field.type):
                        for child in field.type:
                            new_fields.append(pa.field(f"{field.name}{self.separator}{child.name}", child.type))
                            new_columns.append(struct_field(table.column(field.name), child.name))
                    else:
                        new_fields.append(field)
                        new_columns.append(table.column(field.name))
                table = pa.Table.from_arrays(new_columns, schema=pa.schema(new_fields))
            yield from table.to_batches()
