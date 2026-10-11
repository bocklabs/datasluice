from __future__ import annotations

from typing import TYPE_CHECKING, Self

from datasluice.domain import HttpDownload, Resource

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    import pyarrow as pa


class Opened:
    def __init__(self, batches: list[pa.RecordBatch]) -> None:
        self._batches = batches
        self.closed = False
        self.yielded_rows = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.close()

    def iter_batches(self) -> Iterator[pa.RecordBatch]:
        for batch in self._batches:
            self.yielded_rows += batch.num_rows
            yield batch

    def close(self) -> None:
        self.closed = True


class Facade:
    def __init__(self, opened: Opened, *, resource: Resource | None = None) -> None:
        self._opened = opened
        url = "https://data.example.test/observations.csv"
        self._resource = resource or Resource(id="observations", url=url, format="CSV", access=HttpDownload(url=url))
        self.resolved: list[object] = []
        self.opened: list[Resource] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        return None

    def resolve(self, locator: object) -> Resource:
        self.resolved.append(locator)
        return self._resource

    def open(self, resource: Resource) -> Opened:
        self.opened.append(resource)
        return self._opened
