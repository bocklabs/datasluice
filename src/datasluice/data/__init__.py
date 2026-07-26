"""Streaming data-plane package: BatchStream, byte-source adapter, schema mapper.

Re-exports are resolved lazily via PEP 562 ``__getattr__`` so that importing
``datasluice.data`` does not trigger a pyarrow import on bare installs. All
three symbols (``BatchStream``, ``IterableBytesIO``, ``to_arrow_schema``)
depend on lazy pyarrow, so the ``__getattr__`` discipline keeps the bare
import light.
"""

__all__ = [
    "BatchStream",
    "IterableBytesIO",
    "to_arrow_schema",
]


def __getattr__(name: str):  # PEP 562
    """Lazily export BatchStream, IterableBytesIO, and to_arrow_schema.

    Importing any of these eagerly would pull pyarrow at package import
    time and break bare installs. Each symbol is resolved on first
    attribute access (D-P4-01 lazy discipline).
    """
    if name == "BatchStream":
        from datasluice.data.batch_stream import BatchStream

        globals()["BatchStream"] = BatchStream
        return BatchStream
    if name == "IterableBytesIO":
        from datasluice.data._byte_source import IterableBytesIO

        globals()["IterableBytesIO"] = IterableBytesIO
        return IterableBytesIO
    if name == "to_arrow_schema":
        from datasluice.data.schema import to_arrow_schema

        globals()["to_arrow_schema"] = to_arrow_schema
        return to_arrow_schema
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
