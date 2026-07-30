# Testing Patterns

**Analysis Date:** 2026-07-30

## Test Framework

**Runner:**
- pytest (declared in the `test` dependency group, `pyproject.toml`).
- Coverage via `coverage` with branch + parallel mode.

**Config:** `pyproject.toml`:
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src", "."]

[tool.coverage.run]
branch = true
parallel = true
source = ["src/", "tests/"]

[tool.coverage.report]
show_missing = true
skip_covered = true
fail_under = 50
exclude_also = [
    "if TYPE_CHECKING:",
    "@overload",
    "class .*\\bProtocol\\):",
    "@(abc\\.)?abstractmethod",
    "raise NotImplementedError",
    "\\.\\.\\.",
]
```

**Assertion Library:** bare `assert` (no `unittest.TestCase`). Tests are plain functions.

**Run Commands:**
```bash
uv run pytest                              # all tests
uv run pytest tests/unit/domain            # one directory
uv run pytest tests/unit/transport/test_http_client.py  # one file
uv run pytest -k rate_limit                # by keyword
uv run pytest -q                           # quiet (used by pre-commit hook)
just qa   # or  make qa                    # format → lint → typecheck → test
```

## Test File Organization

**Location:** separate `tests/` tree mirroring `src/` layout (not co-located). Top-level split:
```
tests/
├── conftest.py                 # package-wide fixtures
├── helpers/                    # shared test utilities (http_server.py)
├── fixtures/                   # hand-authored JSON portal responses
│   ├── ckan/   *.json
│   ├── datagouv/  *.json
│   └── socrata/   *.json
├── unit/                       # the bulk of the suite (~87 test files)
│   ├── auth/  cli/  connectors/  contracts/  credentials/  data/
│   ├── discovery/  domain/  formats/  integrations/  io/  ports/
│   ├── runtime/  sync/  transforms/  transport/
│   └── *.py                    # cross-cutting unit tests
└── integration/
    ├── ckan/  datagouv/  socrata/   # portal-specific integration dirs
```

**Naming:**
- Files: `test_<subject>.py`.
- Functions: `test_<scenario>`. Descriptive, no leading `Test` on functions.
- `conftest.py` per subtree for shared fixtures (e.g. `tests/unit/contracts/conftest.py`, `tests/unit/sync/conftest.py`, `tests/unit/data/conftest.py`).

## Test Structure

**Suite Organization — plain functions with module docstrings:**
```python
"""Integration tests for HttpClient hardening: credential_scope, error mapping, get_json."""
from __future__ import annotations
import pytest
from datasluice.transport import HttpClient
from tests.helpers.http_server import MockResponse, start_test_server


def _fast_policy() -> RetryPolicy:
    return RetryPolicy(max_attempts=1, base_delay=0.01)


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
(`tests/unit/transport/test_http_client.py:59-68`.)

**Patterns:**
- **No classes.** Every test is a module-level `def test_...`.
- **Helper functions** prefixed `_` for builders/factories (`_fast_policy`, `_load_fixtures`, `_json_body`, `_ckan_responses`).
- **Real HTTP over localhost sockets** is the default for transport/connector tests — see the "No transport mocking" rule below.

## Mocking & HTTP

**The project deliberately avoids HTTP mocking libraries (no `respx`, `responses`, `httpx_mock`).** The AGENTS.md / docstrings state the rule explicitly: "no transport mocking, no network egress" (D-P5-12). Instead, tests spin up a **real scriptable local HTTP server**.

**Primary test double: `tests/helpers/http_server.py`**
- `start_test_server(responses)` → `(server, base_url)`. `ThreadingHTTPServer` on an ephemeral port.
- `MockResponse(status=200, headers=..., body=b"...", chunk_size=...)` configures a canned reply.
- Path map: `dict[str, MockResponse | list[MockResponse]]`. A **list** is consumed sequentially per request (queue), enabling retry/redirect/pagination scenarios.
- The server **records** received requests on `server.captured` (header dicts) and `server.captured_paths` (raw request targets) for assertions.
- Supports `Transfer-Encoding: chunked` (`chunk_size`) and conditional-GET (304) handling for SYNC/ETag tests.

**Lifecycle (mandatory):** start → exercise → `server.shutdown(); server.server_close()` in `finally`:
```python
server, base = start_test_server({"/err": MockResponse(status=503, body=b"oops")})
try:
    ...
finally:
    server.shutdown()
    server.server_close()
```

**`unittest.mock` usage (sparingly):** only for non-HTTP seams, e.g. `MagicMock(spec=HostCredentialProvider)` to fake a credential provider (`tests/unit/transport/test_httpx_transport.py:223`) or `patch` filesystem functions (`tests/unit/io/test_filesystem.py`). Prefer the real-socket server for anything HTTP-shaped.

**`monkeypatch`:** used for attribute/env swaps, e.g. patching `datasluice.DataSluiceSession` in integration tests (`tests/unit/integrations/test_dlt.py:48`).

## Fixtures and Factories

**Package-level** (`tests/conftest.py`):
```python
@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"
```

**Subtree-level:** portal-server fixtures in `tests/unit/contracts/conftest.py` yield `(server, base_url, fixture_set)`:
```python
@pytest.fixture
def ckan_server() -> Iterator[tuple[_CapturingServer, str, dict[str, Any]]]:
    fixture_set = _load_fixtures(_FIXTURES_ROOT / "ckan", ["package_search", "package_show", "organization_show"])
    fixture_set["dataset_id"] = fixture_set["package_show"]["result"]["id"]
    server, base = start_test_server(_ckan_responses(fixture_set))
    try:
        yield server, base, fixture_set
    finally:
        server.shutdown()
        server.server_close()
```
(`tests/unit/contracts/conftest.py:112-122`.) Each portal has a `_<portal>_responses(fixture_set)` builder that scripts the call sequence the conformance suite expects.

**Test data = hand-authored JSON** under `tests/fixtures/<portal>/` — small, deterministic, no VCR cassettes, no recorded-live captures, no credentials. Loaded via `datasluice.contracts.fixtures.load_fixture` / `load_fixture_set` (`src/datasluice/contracts/fixtures.py`).

**Conformance `fixture_set` contract:** MUST include a `"dataset_id"` entry naming a dataset `get_dataset` can fetch. The server scripts the suite's documented fixed call order (see `src/datasluice/contracts/checks.py` module docstring).

## Contract / Conformance Tests

The centerpiece QA mechanism is `datasluice.contracts.run_contract_suite` (`src/datasluice/contracts/checks.py`), an **8-check matrix** every catalog connector must pass:

| # | Check | Guard |
|---|-------|-------|
| 1 | Publishes `capabilities: ClassVar[CatalogCapabilities]` | D-P5-23 |
| 2 | `isinstance(connector, SearchableCatalog)` (structural) | D-08 |
| 3 | `get_dataset` returns `Dataset` with a list `resources` | QUAL-02 |
| 4 | `search` returns populated `SearchResult` (`total >= len(datasets)`) | QUAL-02 |
| 5 | Dataset IDs stable across repeated `get_dataset` calls | QUAL-02 |
| 6 | Pagination: two consecutive pages share no dataset IDs | QUAL-02 |
| 7 | Unsupported filter field raises `UnsupportedQueryFieldError` pre-flight | QUAL-02, ARCH-08 |
| 8 | At least one resource per dataset carries an access descriptor | QUAL-02, D-P5-02 |

**How it runs:**
```python
@pytest.mark.parametrize(
    ("portal_name", "factory"),
    [
        pytest.param("ckan", create_ckan_connector, id="ckan"),
        pytest.param("datagouv", create_datagouv_connector, id="datagouv"),
        pytest.param("socrata", create_socrata_connector, id="socrata"),
    ],
)
def test_builtin_connector_conforms(portal_name, factory, request) -> None:
    _server, base, fixture_set = request.getfixturevalue(f"{portal_name}_server")
    run_contract_suite(factory, fixture_set, base_url=base, transport=HttpClient())
```
(`tests/unit/contracts/test_builtin_conformance.py`.) Runs in the **default** suite — no marker, no opt-in.

**API-stability tests:** `tests/unit/contracts/test_contract_api.py` asserts `run_contract_suite` is importable from the package root, re-exported from `checks`, and has the locked signature `(connector_factory, fixture_set, *, base_url, transport)`. The signature is a one-way-locked public contract (D-P5-11) — changing it breaks CI.

**Third-party on-ramp:** the suite is public; external connector authors parametrize it against their own fixtures.

## Common Patterns

**Parametrization** (heavily used):
```python
@pytest.mark.parametrize("bad_name", ["x; DROP", "bad name", "1lead", "", "'); DROP TABLE x;--"])
def test_invalid_table_name_rejected(bad_name: str) -> None: ...
```
(`tests/unit/integrations/test_duckdb_injection.py`.) Use `pytest.param(..., id=...)` for readable IDs (conformance matrix).

**Exception testing** — `pytest.raises` with `exc_info` attribute assertions:
```python
with pytest.raises(RetryableHTTPError) as exc_info:
    client.request(f"{base}/err")
assert exc_info.value.status_code == 503
```
Assert hierarchy directly where relevant: `assert issubclass(UnsupportedQueryFieldError, DataSluiceError)` (`tests/unit/connectors/test_ckan_reject_policy.py:38`).

**Optional-dependency gating** — skip when an extra is unavailable:
```python
@pytest.mark.skipif(...)
```
(`tests/unit/data/test_batch_stream.py:160`.) And module-level `pytest.skip("...", allow_module_level=True)` after a `try/except ImportError` import guard for TDD RED-phase tests (`tests/unit/connectors/test_ckan_reject_policy.py:15-24`).

**Async testing:** Not used — the codebase is synchronous (threading only inside the test HTTP server).

## Test Types

**Unit tests** (`tests/unit/`): the bulk. Pure-function mapper tests with inline dict fixtures (`tests/unit/connectors/test_ckan_mapper.py`), port/Protocol assertions, error-hierarchy checks, and signature-introspection stability tests. No network.

**Integration tests** (`tests/integration/<portal>/` and the contract suite): exercise a real connector against the local scriptable HTTP server over real sockets. Distinguished from "unit" by touching the full transport→adapter→mapper stack, but still **no network egress** — everything is localhost.

**E2E tests:** Not present. The contract suite over real sockets is the closest equivalent for connectors.

## Coverage

**Requirement:** `fail_under = 50` (`pyproject.toml` `[tool.coverage.report]`). CI enforces it.

**Coverage exclusions** (not counted against you): `if TYPE_CHECKING:` blocks, `@overload`, `Protocol` classes, `@abstractmethod`, bare `raise NotImplementedError`, and `...` (Protocol bodies / stubs).

**Report:** `show_missing = true`, `skip_covered = true`, branch coverage on.

**Run coverage:**
```bash
uv run coverage run -m pytest && uv run coverage report   # branch report with missing lines
```

## Cross-Cutting Test Invariants

Special guard tests live at `tests/unit/`:
- `test_package.py` — public package import/surface checks.
- `test_no_dead_settings.py` — guards against orphaned/dead config.
- `test_custom_adapter_removed.py` — locks the removal of the old `CustomAdapter` extension path (now entry-points only).
- `test_sync_purity.py` — asserts sync-side-effect purity invariants.
- `test_redacting_filter.py` — verifies `RedactingFilter` secret redaction (independent of `logging.py`).

When adding a removed/locked behavior, add a guard test of this shape to prevent regression.

---

*Testing analysis: 2026-07-30*
