# Testing Patterns

**Analysis Date:** 2026-07-26

## Test Framework

**Runner:**
- `pytest` (declared in `pyproject.toml` `[dependency-groups].test`).
- Config: `pyproject.toml` `[tool.pytest.ini_options]`:
  ```toml
  testpaths = ["tests"]
  pythonpath = ["src", "."]
  ```

**Assertion Library:** bare `assert` statements (pytest assertion rewriting). No `unittest.TestCase` subclasses anywhere.

**Coverage:** `coverage.py` with branch + parallel mode (`pyproject.toml` `[tool.coverage.run]`):
```toml
[tool.coverage.run]
branch = true
parallel = true
source = ["src/", "tests/"]

[tool.coverage.report]
show_missing = true
skip_covered = true
fail_under = 50
exclude_also = [
    "if TYPE_CHECKING:", "if typing.TYPE_CHECKING:",
    "@overload", "@typing.overload",
    "class .*\\bProtocol\\):",
    "@(abc\\.)?abstractmethod",
    "raise NotImplementedError", "\\.\\.\\.",
]
```

**Run Commands:**
```bash
uv run pytest                                # Run all tests
uv run pytest tests/unit/domain/test_models.py   # Single file
uv run pytest -k "test_bearer"               # By name pattern
just test -x                                 # Via justfile (passes ARGS), -x stops at first failure
just pdb                                     # Drop into debugger on failure (--pdb --maxfail=10)
uv run --all-extras ty check .               # Type check (required for pre-commit)
just qa                                      # Full pipeline: ruff format → ruff check → ty check → pytest
```

**Multi-version coverage** (`justfile` / `Makefile` `coverage` target): runs pytest under Python 3.12, 3.13, 3.14 separately with `coverage run`, then `coverage combine` + `coverage report` + `coverage html`. CI mirrors this (`.github/workflows/ci.yml` `test` + `coverage` jobs upload/combine `.coverage.*` artifacts across the matrix).

## Test File Organization

**Location:** separate `tests/` tree, mirroring the source layout:
```
tests/
├── conftest.py                 # Shared fixtures (currently: fixtures_dir)
├── __init__.py                 # Empty package marker
├── helpers/
│   ├── __init__.py
│   └── http_server.py          # Scriptable local ThreadingHTTPServer
├── fixtures/                   # Data fixtures (per-portal, currently .gitkeep placeholders)
│   ├── ckan/.gitkeep
│   ├── datagouv/.gitkeep
│   └── socrata/.gitkeep
├── integration/                # Live/integration tests (currently .gitkeep placeholders)
│   ├── ckan/.gitkeep
│   ├── datagouv/.gitkeep
│   └── socrata/.gitkeep
└── unit/                       # All active tests live here
    ├── auth/test_auth.py
    ├── cli/test_download.py
    ├── connectors/test_ckan_mapper.py
    ├── credentials/test_host_provider.py
    ├── discovery/test_discovery.py
    ├── domain/
    │   ├── test_models.py
    │   ├── test_credentials.py
    │   ├── test_new_models.py
    │   └── test_purity.py
    ├── formats/test_formats.py
    ├── integrations/
    │   ├── test_integrations.py
    │   └── test_duckdb_injection.py
    ├── io/
    │   ├── test_io.py
    │   ├── test_cache.py
    │   ├── test_content_cache.py
    │   ├── test_storage.py
    │   ├── test_filesystem.py
    │   └── test_fsspec_storage.py
    ├── ports/
    │   ├── test_protocols.py
    │   ├── test_transport_conformance.py
    │   ├── test_transport_protocol.py
    │   └── test_capability_probing.py
    ├── runtime/
    │   ├── test_session.py
    │   ├── test_session_injection.py
    │   ├── test_plugin_manager.py
    │   └── test_no_global_state.py
    ├── transport/
    │   ├── test_http_client.py
    │   ├── test_httpx_transport.py
    │   ├── test_retry.py
    │   ├── test_redirect.py
    │   └── test_transport.py
    ├── test_package.py         # Public-API smoke tests
    ├── test_no_dead_settings.py
    └── test_redacting_filter.py
```

**Naming:** `test_<module_or_feature>.py` under a directory matching the source package. **34 test files** total, all under `tests/unit/`.

**Structure:** tests are **flat functions**, not classes. Every test function is `def test_<thing>(...) -> None:` with an explicit `-> None` return annotation and a `from __future__ import annotations` header, mirroring source conventions.

## Test Structure

**Suite Organization — bare functions with arrange/act/assert:**

```python
# tests/unit/domain/test_models.py
"""Unit tests for domain models."""

from __future__ import annotations

from datasluice.domain import Dataset, License, Organization, Query, Resource, SearchResult


def test_license_defaults() -> None:
    license_ = License(id="CC-BY-4.0")
    assert license_.id == "CC-BY-4.0"
    assert license_.title is None
    assert license_.url is None
```

**Per-file module docstring** is mandatory — one line describing the scope, often referencing the plan/decision ID (e.g. `"""Unit tests for HostCredentialProvider (INFRA-04)."""`).

**Section banners** with `# ----` rules separate logical groups within larger test files (see `tests/unit/credentials/test_host_provider.py`, `tests/unit/test_redacting_filter.py`).

**No setup/teardown fixtures per test.** State is built inline or via small module-local helpers (prefixed `_`).

## Parametrized Tests

Use `@pytest.mark.parametrize` with **tuple-form argument lists** for multi-input cases. Example from `tests/unit/cli/test_download.py`:

```python
@pytest.mark.parametrize(
    ("formats", "fmt", "expected_formats"),
    [
        (["CSV", "JSON", "XLSX"], "CSV", ["CSV"]),
        (["csv", "JSON", "XLSX"], "csv", ["csv"]),
        (["CSV", "csv", "CSV"], "CSV", ["CSV", "csv", "CSV"]),
        (["CSV", "JSON", "XLSX"], None, ["CSV", "JSON", "XLSX"]),
    ],
)
def test_download_format_filtering(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    formats: list[str | None],
    fmt: str | None,
    expected_formats: list[str | None],
) -> None:
    ...
```

Other parametrized cases: security payloads in `tests/unit/integrations/test_duckdb_injection.py` (`RESOURCE_URL_INJECTIONS`, `BAD_TABLE_NAMES`, valid table names), optional-dep import checks in `tests/unit/integrations/test_integrations.py`.

## Mocking

**Framework:** `unittest.mock` (`patch`, `MagicMock`). Used **sparingly** — only for isolating external side effects (time, network entry points, refresher callables). Prefer fakes and the local HTTP server over mocks.

**Patterns:**

Patch a specific symbol's reference at its use site (`time.sleep` in the retry module):
```python
# tests/unit/transport/test_retry.py
from unittest.mock import patch

with patch("datasluice.transport.retry.time.sleep", side_effect=fake_sleep):
    result = with_retry(func, RetryPolicy(max_attempts=2, base_delay=0.01, max_delay=5.0))
assert sleeps == [5.0]
```

`MagicMock` as a callable double recording call counts and return values (credential refresher):
```python
# tests/unit/credentials/test_host_provider.py
refresher = MagicMock(side_effect=[(BearerAuth("token-a"), None), (BearerAuth("token-b"), None)])
provider = HostCredentialProvider(refresher=refresher)
...
assert refresher.call_count == 1
```

`MagicMock(spec=...)` to constrain the double to a Protocol surface:
```python
# tests/unit/transport/test_httpx_transport.py
provider = MagicMock(spec=host_provider.HostCredentialProvider)
```

Patch `fsspec` URL dispatch for filesystem-abstraction tests:
```python
# tests/unit/io/test_filesystem.py
with patch("fsspec.core.url_to_fs") as mocked:
    mocked.return_value = (MagicMock(), "path")
```

**What to Mock:**
- `time.sleep` (deterministic backoff verification).
- External refresher callables (count invocations, return canned tuples).
- `fsspec.core.url_to_fs`, `entry_points` (entry-point discovery isolation).

**What NOT to Mock:**
- **The HTTP layer** — instead spin up the real-socket test server (`tests/helpers/http_server.py`). See next section.
- **Domain models / mappers** — construct real instances and assert on their values.
- **The CLI** — invoke via `typer.testing.CliRunner` against the real `app`, patching only the composition root (`DataSluiceSession`).

## Real-Socket HTTP Test Helper

The codebase deliberately avoids mocking HTTP. `tests/helpers/http_server.py` provides a **scriptable `ThreadingHTTPServer`** on an ephemeral port:

```python
# tests/helpers/http_server.py
@dataclass
class MockResponse:
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b"OK"

def start_test_server(
    responses: dict[str, MockResponse | list[MockResponse]] | None = None,
) -> tuple[_CapturingServer, str]:
    ...
```

Usage (from `tests/unit/transport/test_http_client.py`):
```python
from tests.helpers.http_server import MockResponse, start_test_server

def test_http_client_429_raises_rate_limit_error() -> None:
    server, base = start_test_server({"/busy": MockResponse(status=429, headers={"Retry-After": "2"}, body=b"slow")})
    try:
        client = HttpClient(retry_policy=_fast_policy())
        with pytest.raises(RateLimitError) as exc_info:
            client.request(f"{base}/busy")
        assert exc_info.value.retry_after == 2.0
    finally:
        server.shutdown()
        server.server_close()
```

**Conventions:**
- A `list[MockResponse]` value is consumed sequentially per request (for retry-then-succeed flows).
- The server records `captured` request-header dicts (lower-cased keys) — assert on them for credential-leak tests (`tests/unit/transport/test_redirect.py`).
- **Always** shut down in a `finally`: `server.shutdown(); server.server_close()` (threads are daemon, but explicit cleanup avoids port churn).
- The multi-server pattern (server A → server B redirect) is used for cross-host credential-stripping tests.

## CLI Testing

Use `typer.testing.CliRunner` against the real `app`, injecting a fake session via `monkeypatch.setattr`:

```python
# tests/unit/cli/test_download.py
from typer.testing import CliRunner
from datasluice.cli.app import app

runner = CliRunner()

def _patch_client(monkeypatch: pytest.MonkeyPatch, dataset: Dataset) -> _RecordingDownloader:
    downloader = _RecordingDownloader()
    class FakeConnector: ...
    class FakeDataSluiceSession:
        def portal(self, url: str) -> FakeConnector: ...
    monkeypatch.setattr(datasluice, "DataSluiceSession", FakeDataSluiceSession)
    return downloader

result = runner.invoke(app, ["download", "--portal", "https://example.com", "dataset-1", "--dest", str(tmp_path)])
assert result.exit_code == 0
```

Assert on `result.exit_code`, `result.output`, and the recording double's captured state.

## Pytest Built-in Fixtures Used

| Fixture | Usage |
|---------|-------|
| `tmp_path: Path` | All filesystem-touching tests (cache, storage, formats, duckdb CSV, content-cache concurrency). The dominant fixture. |
| `monkeypatch: pytest.MonkeyPatch` | Env vars (`monkeypatch.setenv("DATASLUICE_NO_REDACT", "1")`), attribute patches (`monkeypatch.setattr(datasluice, "DataSluiceSession", ...)`), `entry_points` swap. |

`capsys` / `caplog` are **not** currently used — output is tested via `CliRunner.result.output`, and logging via direct filter inspection.

## Shared Fixtures (conftest.py)

`tests/conftest.py` is minimal:
```python
@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"
```
Most test data is constructed inline rather than loaded from files (the `tests/fixtures/` dirs hold `.gitkeep` placeholders awaiting portal payload fixtures).

## Optional-Dependency Test Guards

Two complementary patterns let tests run under the repo's full-suite pre-commit hook even when an optional dep or a not-yet-implemented module is absent:

**1. `pytest.importorskip` / module-level `pytest.skip`:**
```python
# tests/unit/integrations/test_duckdb_injection.py
pytest.importorskip("duckdb")
import duckdb  # noqa: E402
```
```python
# tests/unit/credentials/test_host_provider.py
pytest.importorskip("datasluice.credentials.host_provider")
try:
    _host_provider_module = importlib.import_module("datasluice.credentials.host_provider")
except ImportError:
    pytest.skip("HostCredentialProvider implementation pending (RED -> GREEN within task 03-04)", allow_module_level=True)
```

**2. `importlib` + `hasattr` RED-commit resolution** (`tests/unit/test_redacting_filter.py`):
```python
_logging_module = importlib.import_module("datasluice.logging")
if not hasattr(_logging_module, "RedactingFilter"):
    pytest.skip("RedactingFilter implementation pending (RED -> GREEN within task 03-04)", allow_module_level=True)
RedactingFilter = _logging_module.RedactingFilter
```
After the `importorskip`/`skip` gate, subsequent imports use `# noqa: E402` since they follow a non-import statement.

## Concurrency / Thread-Safety Tests

`HostCredentialProvider` single-flight behaviour is verified with real threads (no mock of the threading primitives):

```python
# tests/unit/credentials/test_host_provider.py
n_threads = 10
barrier = threading.Barrier(n_threads)
with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = [executor.submit(resolve_once) for _ in range(n_threads)]
    concurrent.futures.wait(futures)
assert refresher.call_count == 1, f"expected exactly one refresh, got {refresher.call_count}"
```

The content-cache atomicity tests (`tests/unit/io/test_content_cache.py`, ~299 lines) use ThreadPoolExecutor to exercise concurrent `put`/`get` for distinct keys, same-key write races, torn-read prevention, and crash-mid-two-phase rollback.

`threading.Event` gates (`refresh_started` / `refresh_can_finish`) coordinate eviction-during-refresh scenarios (`test_evict_during_in_flight_refresh_safe`).

## Meta / Source-Scanning Tests

The suite includes **behavioural guards that read source files at test time** to enforce architectural invariants:

- `tests/unit/test_no_dead_settings.py` — scans every `.py` under `src/datasluice/` for `DATASLUICE_[A-Z_]+` env-var references (allow-listing only `DATASLUICE_NO_REDACT`); asserts `Settings` / `load_settings` are no longer importable.
- `tests/unit/runtime/test_no_global_state.py` — scans `src/datasluice/connectors/` for `registry = AdapterRegistry`, `registry.register(`, and `AdapterRegistry()` substrings to forbid module-level singleton reintroduction.
- `tests/unit/domain/test_purity.py` — deletes optional modules from `sys.modules`, imports `datasluice.domain`, asserts none of `(pyarrow, pandas, polars, dlt, duckdb, openpyxl, airflow)` re-entered `sys.modules`.
- `tests/unit/test_package.py` — public-API smoke: `__version__` shape, exported symbols present, exception `issubclass` hierarchy.

**Pattern for new architectural rules:** add a focused scanning test that fails loudly if the invariant is violated, rather than relying on review.

## Fixtures and Factories

Inline factory helpers (module-local, `_`-prefixed) are preferred over fixture functions:

```python
# tests/unit/cli/test_download.py
def _make_dataset(formats: list[str | None]) -> Dataset:
    resources = [Resource(id=f"res-{i}", name=f"resource-{i}", url=..., format=fmt) for i, fmt in enumerate(formats)]
    return Dataset(id="dataset-1", title="Test Dataset", resources=resources)
```
```python
# tests/unit/transport/test_http_client.py
def _fast_policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=1, base_delay=0.01)
```
```python
# tests/unit/test_redacting_filter.py
def _make_record(**attrs) -> logging.LogRecord: ...
```

Recording doubles capture arguments for later assertion:
```python
class _RecordingDownloader:
    def __init__(self) -> None:
        self.received: list[Resource] | None = None
    def download_many(self, resources, dest): ...
```

## Coverage

**Requirements:** `fail_under = 50` (`pyproject.toml` `[tool.coverage.report]`). Branch coverage is on (`branch = true`).

**Excluded from coverage** (`exclude_also`): `TYPE_CHECKING` blocks, `@overload`, `Protocol` class bodies, `@abstractmethod`, bare `raise NotImplementedError`, and `...` (stub/ellipsis) lines — these are structurally unreachable or interface-only.

**View Coverage:**
```bash
just coverage      # full multi-version run + combine + report + html
uv run coverage report   # after a coverage run
uv run coverage html     # writes htmlcov/
```

## Test Types

**Unit Tests (the entire active suite):** `tests/unit/**/*.py`. Pure-function and class-instantiation tests dominate; I/O uses `tmp_path`. Network behaviour uses the real-socket helper server. No mocking of the system under test — only of adjacent collaborators (time, entry_points, refreshers).

**Integration Tests:** `tests/integration/{ckan,datagouv,socrata}/` directories exist but currently hold only `.gitkeep` placeholders — live-portal integration tests are not yet implemented. The real-socket transport tests in `tests/unit/transport/` are labelled "Integration tests" in their module docstrings (exercising redirect/retry over real sockets) but live under `unit/`.

**E2E Tests:** Not used. The closest is the CI **smoke-test** job (`.github/workflows/ci.yml`) which installs the built wheel in a fresh venv and imports `datasluice`.

## Common Patterns

**Exception testing:**
```python
with pytest.raises(RetryableHTTPError) as exc_info:
    client.request(f"{base}/err")
assert exc_info.value.status_code == 503
```
Always assert on the raised exception's carried attributes (`status_code`, `retry_after`, `expected`/`actual`), not just its type.

**Assertion that an exception is NOT a subtype** (negative classification):
```python
with pytest.raises(PortalError) as exc_info:
    client.request(f"{base}/missing")
assert not isinstance(exc_info.value, RetryableHTTPError)
```

**Protocol structural conformance:**
```python
from datasluice.ports import Transport
assert isinstance(HttpClient(), Transport)  # runtime_checkable Protocol
```

**No async tests.** The suite is entirely synchronous; `pytest-asyncio` is not a dependency.

---

*Testing analysis: 2026-07-26*
