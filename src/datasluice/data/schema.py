"""Domain Schema → Arrow Schema mapper and batch unification helper.

The :func:`to_arrow_schema` mapper (DATA-07, D-P4-01) derives a ``pa.Schema``
from a domain :class:`~datasluice.domain.schema.Schema` for DISPLAY purposes
— advisory only, NOT enforced. Readers infer the Arrow schema from actual
data bytes (D-P4-02).

The :func:`unify_batches` helper (DATA-08, D-P4-03) concatenates a sequence
of ``RecordBatch`` objects under a unified schema by delegating entirely to
``pa.concat_tables`` with ``promote_options="permissive"``. datasluice does
NOT hand-roll an Arrow type lattice — pyarrow owns promotion (RESEARCH
Anti-Patterns). The promotion lattice is documented in the function
docstring.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datasluice.domain.schema import Schema


_TYPE_MAP: dict[str, str] = {
    "integer": "int64",
    "int": "int64",
    "number": "float64",
    "float": "float64",
    "string": "string",
    "text": "string",
    "boolean": "bool_",
    "bool": "bool_",
    "date": "date32",
    "datetime": "timestamp_us",
    "timestamp": "timestamp_us",
}


def to_arrow_schema(domain_schema: Schema) -> Any:
    """Derive a ``pa.Schema`` from a domain :class:`Schema` for display.

    Maps known portal type strings to Arrow types; unknown types default to
    ``pa.string()``. The ``nullable`` flag on each column descriptor
    propagates to the ``pa.field`` (defaults to ``True`` when absent).

    Args:
        domain_schema: The domain schema with ``columns`` (list of dicts
            with ``name``, ``type``, ``nullable`` keys).

    Returns:
        A ``pa.Schema`` derived from the portal-native column descriptors.
    """
    import pyarrow as pa

    fields = []
    for col in domain_schema.columns:
        name = str(col.get("name", ""))
        type_key = str(col.get("type", "")).lower()
        arrow_type_name = _TYPE_MAP.get(type_key, "string")
        if arrow_type_name == "int64":
            pytype: Any = pa.int64()
        elif arrow_type_name == "float64":
            pytype = pa.float64()
        elif arrow_type_name == "bool_":
            pytype = pa.bool_()
        elif arrow_type_name == "date32":
            pytype = pa.date32()
        elif arrow_type_name == "timestamp_us":
            pytype = pa.timestamp("us")
        else:
            pytype = pa.string()
        nullable = col.get("nullable", True)
        fields.append(pa.field(name, pytype, nullable=bool(nullable)))
    return pa.schema(fields)


def unify_batches(batches: Iterable[Any]) -> Any:
    """Concatenate ``RecordBatch`` objects under a unified ``pa.Schema`` (DATA-08, D-P4-03).

    Delegates ENTIRELY to ``pa.concat_tables`` with
    ``promote_options="permissive"`` — datasluice does NOT hand-roll an
    Arrow type lattice (RESEARCH Anti-Patterns explicitly forbids it).
    pyarrow's promotion semantics are the contract; this function wraps
    them with a clear error message and the project's
    :class:`SchemaUnificationError` type.

    Promotion lattice (the pyarrow ``permissive`` contract, documented for
    callers — D-P4-03):

    - **int widens to float:** a column that is ``int64`` in one batch and
      ``float64`` in another unifies to ``float64`` (values preserved).
    - **missing columns null-fill:** a column absent from one batch but
      present in another is added to the unified schema with ``null``
      values where data was absent.
    - **string + binary → binary:** a column that is ``string`` in one
      batch and ``binary`` in another unifies to ``binary`` — the lossy
      direction. ``binary`` is the safe merge because ``string`` asserts
      UTF-8 validity and ``binary`` does not, so ``binary`` can hold
      anything ``string`` can (RESEARCH Pitfall 6).
    - **tz-aware vs tz-naive timestamps: HARD FAIL.** pyarrow cannot
      reconcile a timezone-aware timestamp with a timezone-naive one even
      under ``permissive`` promotion; this function raises
      :class:`SchemaUnificationError` (the proper fix is the Phase 6
      ``NormalizeTimestamps`` transform — out of scope here).
    - **struct field mismatch: HARD FAIL.** Differing struct field sets
      or types cannot be unified; surfaces as :class:`SchemaUnificationError`.

    ``promote_options`` is set to the string ``"permissive"`` (the most
    lenient valid option in pyarrow 24.0.0). The values ``"warn"`` and
    ``"ignore"`` are INVALID in pyarrow 24.0.0 — they raise ``ValueError``
    (RESEARCH Pitfall 3 verified). datasluice never uses them.

    Args:
        batches: An iterable of ``pa.RecordBatch`` objects with potentially
            heterogeneous schemas.

    Returns:
        A ``pa.Table`` containing all rows from all batches under a unified
        schema determined by pyarrow's ``permissive`` promotion rules.

    Raises:
        SchemaUnificationError: If pyarrow cannot reconcile the batch
            schemas (tz-aware vs tz-naive timestamps, struct field
            mismatch, or any other unreconcilable type conflict).
    """
    import pyarrow as pa

    from datasluice.exceptions import SchemaUnificationError

    tables = [pa.Table.from_batches([batch]) for batch in batches]
    try:
        return pa.concat_tables(tables, promote_options="permissive")
    except (pa.ArrowTypeError, pa.ArrowInvalid) as exc:
        raise SchemaUnificationError(
            f"Cannot unify batch schemas with promote_options='permissive': {exc}. "
            "Common causes: tz-aware vs tz-naive timestamp columns; struct field mismatch. "
            "Normalize timestamps (Phase 6 NormalizeTimestamps) before unifying."
        ) from exc
