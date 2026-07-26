"""Domain Schema → Arrow Schema mapper (DATA-07, D-P4-01).

Derives a ``pa.Schema`` from a domain :class:`~datasluice.domain.schema.Schema`
for DISPLAY purposes — advisory only, NOT enforced. Readers infer the Arrow
schema from actual data bytes (D-P4-02). This mapper is the boundary where
the zero-dep domain package meets the lazy-imported pyarrow data plane.
"""

from __future__ import annotations

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
