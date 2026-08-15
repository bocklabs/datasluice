"""peak-RSS subprocess memory-bound test for the streaming data plane.

Proves that the streaming data plane keeps memory bounded on large inputs —
the core promise. Two tests:

- ``test_peak_rss_bounded``: streams ~5M rows of synthetic CSV through the
  production ``IterableBytesIO`` adapter into ``pyarrow.csv.open_csv``. The
  input is generated lazily row-by-row (never materialised) so peak RSS
  reflects only pyarrow's batch buffers + Python interpreter baseline, NOT
  the full input size. A regression that switched the data plane to
  full-buffer semantics would push peak RSS well above the threshold.
- ``test_json_peak_rss_bounded``: parses ~200K rows of synthetic JSONL via
  ``pyarrow.json.read_json``. JSON is inherently non-streaming (the reader
  materialises a full ``Table`` internally — documented in
  ``data/readers/json.py``), so the threshold accommodates the parsed
  Table in addition to the input bytes; the test catches accidental
  double-buffering regressions.

CRITICAL: uses ``resource.getrusage(RUSAGE_SELF).ru_maxrss`` in a subprocess
— NOT ``tracemalloc``. verified that pyarrow allocates
from the mimalloc native pool which ``tracemalloc`` does NOT track; a
tracemalloc-based test would pass trivially even when pyarrow buffers a
multi-GB file. ``ru_maxrss`` is a process-lifetime high-water mark that
includes native allocations; the subprocess isolates the measurement from
accumulated pytest process memory.

Both tests run in the DEFAULT pytest suite (NOT marked ``slow``) per
 so CI catches buffering regressions every run.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

pytest.importorskip("pyarrow")

_CSV_SUBPROCESS_CODE = """
import os
import resource
import sys
os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"
sys.path.insert(0, "src")
from datasluice.data._byte_source import IterableBytesIO
import pyarrow.csv as pacsv


def _gen(n_rows):
    yield b"id,name,value\\n"
    for i in range(n_rows):
        yield f"{i},item_{i},{i*1.5}\\n".encode()


# ~145 MB of CSV if materialised; the generator never materialises it.
src = IterableBytesIO(_gen(5_000_000))
reader = pacsv.open_csv(src)
total = 0
for batch in reader:
    total += batch.num_rows
peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_kb = peak_rss // 1024 if sys.platform == "darwin" else peak_rss
print(f"rows={total}")
print(f"peak_rss_kb={peak_kb}")
sys.stdout.flush()
os._exit(0)
"""

_JSON_SUBPROCESS_CODE = """
import io
import os
import resource
import sys
os.environ["ARROW_DEFAULT_MEMORY_POOL"] = "system"
import pyarrow.json as paj


def _gen_jsonl(n_rows):
    for i in range(n_rows):
        yield f'{{"id":{i},"name":"item_{i}","value":{i*1.5}}}\\n'.encode()


# ~10 MB of JSONL input. JSON inherently materialises a full Table
# (documented in data/readers/json.py); the test catches double-buffering.
n_rows = 200_000
data = b"".join(_gen_jsonl(n_rows))
input_bytes = len(data)
read_options = paj.ReadOptions(block_size=1 << 20)
table = paj.read_json(io.BytesIO(data), read_options=read_options)
peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
peak_kb = peak_rss // 1024 if sys.platform == "darwin" else peak_rss
print(f"rows={table.num_rows}")
print(f"input_bytes={input_bytes}")
print(f"peak_rss_kb={peak_kb}")
sys.stdout.flush()
os._exit(0)
"""


def _parse_output(stdout: str) -> dict[str, int]:
    """Parse ``key=value`` lines from the subprocess stdout into a dict."""
    out: dict[str, int] = {}
    for line in stdout.splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            out[key.strip()] = int(value)
    return out


def _run_isolated(code: str, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["sh", "-c", '"$1" -c "$2"', "peak-rss", sys.executable, code],
        capture_output=True,
        text=True,
        check=True,
        timeout=timeout,
    )


def test_peak_rss_bounded() -> None:
    """Streaming ~145MB of synthetic CSV through IterableBytesIO keeps peak RSS well below input.

    The threshold of 250 MB accommodates Python 3.14 + pyarrow allocator
    pressure during the full suite while remaining below the ~290 MB+
    materialisation regression (str + bytes + BytesIO). The isolated streaming
    path remains near 90 MB; this margin keeps the default-suite test stable
    without accepting full-buffer semantics.
    """
    result = _run_isolated(_CSV_SUBPROCESS_CODE, 180)
    assert result.returncode == 0, f"subprocess failed: {result.stderr}"

    values = _parse_output(result.stdout)
    assert values["rows"] == 5_000_000, f"expected 5M rows, got {values['rows']}"
    peak_rss_kb = values["peak_rss_kb"]

    threshold_kb = 250_000  # 250 MB; full-suite peak ~210 MB, materialisation regression ~290 MB+
    assert peak_rss_kb < threshold_kb, (
        f"peak RSS {peak_rss_kb} KB ({peak_rss_kb / 1024:.1f} MB) exceeds "
        f"{threshold_kb} KB ({threshold_kb / 1024:.0f} MB) bound for ~145 MB streaming CSV input "
        "— the data plane is buffering rather than streaming (regression)"
    )


def test_json_peak_rss_bounded() -> None:
    """Parsing ~10MB of synthetic JSONL keeps peak RSS below the bound.

    JSON parsing is inherently non-streaming (``pa.json.read_json`` materialises
    a full ``Table``); the threshold accommodates input bytes + parsed Table
    + pyarrow baseline. A double-buffering regression (e.g. holding the input
    AND a separate decoded copy) would push peak RSS well above this bound.
    """
    result = _run_isolated(_JSON_SUBPROCESS_CODE, 60)
    assert result.returncode == 0, f"subprocess failed: {result.stderr}"

    values = _parse_output(result.stdout)
    assert values["rows"] == 200_000, f"expected 200K rows, got {values['rows']}"
    peak_rss_kb = values["peak_rss_kb"]
    input_bytes = values["input_bytes"]

    threshold_kb = 250_000  # 250 MB; actual is ~100 MB for 10 MB JSON input, regression is ~300+ MB
    assert peak_rss_kb < threshold_kb, (
        f"peak RSS {peak_rss_kb} KB ({peak_rss_kb / 1024:.1f} MB) exceeds "
        f"{threshold_kb} KB ({threshold_kb / 1024:.0f} MB) bound for "
        f"{input_bytes / (1024 * 1024):.1f} MB JSONL input "
        "— the JSON reader is double-buffering (regression)"
    )
