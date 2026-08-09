"""Composable transform pipeline package.

Re-exports are resolved lazily via PEP 562 ``__getattr__`` so that importing
``datasluice.transforms`` does not trigger a pyarrow import on bare installs.
This mirrors the discipline in :mod:`datasluice.data` — every symbol here
depends on lazy pyarrow (the pipeline wraps Arrow ``RecordBatch`` iterators),
so ``__getattr__`` keeps the bare import light.
"""

__all__ = [
    "TransformStep",
    "TransformContext",
    "Pipeline",
    "compose",
    "SelectColumns",
    "RenameColumns",
    "CastSchema",
    "NormalizeTimestamps",
    "Filter",
    "Flatten",
]


def __getattr__(name: str):  # PEP 562
    """Lazily export transform symbols (mirrors datasluice.data.__getattr__).

    Importing any of these eagerly would pull pyarrow at package import time
    and break bare installs. Each symbol is resolved on first attribute access.
    """
    if name == "TransformStep":
        from datasluice.transforms.protocol import TransformStep

        globals()["TransformStep"] = TransformStep
        return TransformStep
    if name == "TransformContext":
        from datasluice.transforms.protocol import TransformContext

        globals()["TransformContext"] = TransformContext
        return TransformContext
    if name == "Pipeline":
        from datasluice.transforms.pipeline import Pipeline

        globals()["Pipeline"] = Pipeline
        return Pipeline
    if name == "compose":
        from datasluice.transforms.pipeline import compose

        globals()["compose"] = compose
        return compose
    if name in {
        "SelectColumns",
        "RenameColumns",
        "CastSchema",
        "NormalizeTimestamps",
        "Filter",
        "Flatten",
    }:
        from datasluice.transforms import steps as _steps

        symbol = getattr(_steps, name)
        globals()[name] = symbol
        return symbol
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
