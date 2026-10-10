import contextlib
import os
import resource
import sys
from typing import Any, cast

import pyarrow as pa

import datasluice.cli.open as open_command
from datasluice.application import DataSluice
from datasluice.domain import HttpDownload, Resource

_ROWS = 75_000
_ROW_WIDTH = 500
_PAYLOAD_SIZE = 2048
_URL = "https://data.example.test/large.csv"


class Opened:
    """Stand-in for the opened result of a streamed export."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def iter_batches(self):
        payload = "x" * _PAYLOAD_SIZE
        for start in range(0, _ROWS, _ROW_WIDTH):
            yield pa.RecordBatch.from_pylist(
                [{"id": value, "payload": payload} for value in range(start, start + _ROW_WIDTH)]
            )


class Facade:
    """Locator that resolves one synthetic resource without touching the network."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def resolve(self, locator):
        return Resource(id="large", url=_URL, format="CSV", access=HttpDownload(url=_URL))

    def open(self, resource):
        return Opened()


def _open_data_sluice() -> DataSluice:
    return cast("DataSluice", Facade())


def main() -> int:
    cast("Any", open_command).open_data_sluice = _open_data_sluice
    with open(os.devnull, "w") as sink, contextlib.redirect_stdout(sink):
        open_command.open(_URL, all_rows=True, output="jsonl")
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss_kb = peak_rss // 1024 if sys.platform == "darwin" else peak_rss
    print(f"peak_rss_kb={peak_rss_kb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
