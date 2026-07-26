"""Streaming data-plane package: BatchStream, byte-source adapter, schema mapper.

Re-exports are resolved lazily via PEP 562 ``__getattr__`` so that importing
``datasluice.data`` does not trigger a pyarrow import on bare installs. The
full ``__getattr__`` implementation lands in Task 2 alongside the concrete
classes.
"""
