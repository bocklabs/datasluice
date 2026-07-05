# Testing Patterns

**Analysis Date:** 2026-07-05

## Test Framework

**Runner:**
- pytest (latest, via `uv run pytest`).
- Config: `[tool.pytest.ini_options]` in `pyproject.toml` — `testpaths = ["tests"]`.

**Assertion Library:**
- Plain `assert` statements (pytest built-ins). No `unittest.TestCase` subclasses anywhere.

**Coverage:**
- `coverage` package, configured via `[tool.coverage.run]` and `[tool.coverage.report]` in `pyproject.toml`.
- Branch coverage enabled (`branch = true`), parallel execution (`parallel = true`), sources `["src/", "tests/"]`.
- Threshold: **`fail_under = 50`** (CI fails below this).
- `show_missing = true`, `skip_covered = true`.

**Run Commands:**
```bash
uv run pytest                                # Run all tests
uv run pytest tests/unit/domain/test_models.py  # Single file
uv run pytest -k test_retry                  # By name match
uv run pytest --pdb --maxfail=10             # Drop into debugger on failure (just pdb)
just test                                    # Justfile alias (passes extra args)
just qa                                      # Full pipeline: format → lint → typecheck → test
just coverage                                # Coverage across Python 3.12, 3.13, 3.14
uv run pre-commit run --all-files            # Pre-commit (runs ty + pytest hooks too)
```

**Coverage exclusions** (`[tool.coverage.report] exclude_also`): `if TYPE_CHECKING:` blocks, `@overload`, `Protocol` classes, `@abstractmethod`, `raise NotImplementedError`, and `...` stub bodies.

## Test File Organization

**Location:** Separate `tests/` directory mirroring the source tree.

```
tests/
├── conftest.py                          # Shared fixtures (fixtures_dir)
├── unit/
│   ├── test_package.py                  # Top-level package smoke tests
│   ├── adapters/
│   │   ├── test_ckan_mapper.py
│   │   └── test_registry.py
│   ├── auth/
│   │   └── test_auth.py
│   ├── discovery/
│   │   └── test_discovery.py
│   ├── domain/
│   │   └── test_models.py
│   ├── formats/
│   │   └── test_formats.py
│   ├── integrations/
│   │   └── test_integrations.py
│   ├── io/
│   │   └── test_io.py
│   └── transport/
│       └── test_transport.py
├── integration/                         # Placeholder dirs (.gitkeep only)
│   ├── ckan/.gitkeep
│   ├── socrata/.gitkeep
│   └── datagouv/.gitkeep
└── fixtures/                            # Test data fixtures (.gitkeep placeholders)
    ├── ckan/.gitkeep
    ├── socrata/.gitkeep
    └── datagouv/.gitkeep
```

**Naming:**
- Test files: `test_<module_or_concern>.py`. One test file per source module (or per concern when modules are small — e.g. `test_io.py` covers checksums + cache + local + storage).
- Test functions: `test_<behaviour>()` — describe the behaviour, not the implementation.
- One concern per test, no shared mutable state between tests.

**Co-location:** Tests are NOT co-located with source. They live under `tests/unit/` mirroring `src/datasluice/` package structure.

## Test Structure

**Suite Organization:** Function-based — **no test classes**. Each test is a standalone `def test_xxx() -> None:` function. This is the universal pattern across the codebase.

```python
# src mirror: tests/unit/domain/test_models.py
"""Unit tests for domain models."""

from __future__ import annotations

from datasluice.domain import Dataset, License, Organization, Query, Resource, SearchResult


def test_resource_normalize_format() -> None:
    assert Resource.normalize_format("text/csv") == "CSV"
    assert Resource.normalize_format("application/json") == "JSON"
    assert Resource.normalize_format("csv") == "CSV"
    assert Resource.normalize_format(None) is None


def test_dataset_defaults() -> None:
    dataset = Dataset(id="ds-1")
    assert dataset.id == "ds-1"
    assert dataset.resources == []
    assert dataset.tags == []
```

**Patterns observed:**
- **Module docstring + future import + imports** at the top of every test file (mirrors source convention).
- **All test functions return `-> None`** (the single intentional exception is `test_csv_reader`, marked `# type: ignore[no-untyped-def]`).
- **One assertion concept per test** — multiple `assert` lines are fine when testing facets of one behaviour (see `test_resource_normalize_format`).
- **Descriptive variable names**: `license_`, `dataset`, `resource`, `raw` (for raw portal JSON).
- **"defaults" tests** assert the dataclass field defaults (e.g. `test_license_defaults`, `test_query_defaults`).

**Setup/Teardown:**
- No `setUp`/`tearDown` — use pytest fixtures instead.
- Filesystem tests use the built-in `tmp_path` fixture (pytest auto-cleans).
- Registry mutation tests clean up after themselves (see `test_registry_register_and_unregister` below).

## Mocking

**Framework:** **None.** The codebase does **not** use `unittest.mock`, `pytest-mock`, or any mocking library.

Tests use one of three strategies instead:

**1. Real implementations (preferred):**

```python
# tests/unit/transport/test_transport.py
def test_with_retry_succeeds() -> None:
    calls = [0]

    def func() -> str:
        calls[0] += 1
        return "ok"

    result = with_retry(func, RetryPolicy(max_attempts=3))
    assert result == "ok"
    assert calls[0] == 1
```

**2. Inline test doubles (closures / simple fakes):**

```python
# tests/unit/transport/test_transport.py — fake fetch function
def test_paginate() -> None:
    def fetch_page(page: int, size: int) -> tuple[list[int], bool]:
        if page > 3:
            return [], False
        return list(range((page - 1) * size, page * size)), page < 3

    pages = list(paginate(fetch_page, page_size=10, max_pages=5))
    assert len(pages) == 3
```

**3. Inline stub subclasses** for abstract bases:

```python
# tests/unit/adapters/test_registry.py
def test_registry_register_and_unregister() -> None:
    class Dummy(BaseAdapter):
        portal_type = "dummy"

        def search(self, query=None): ...
        def get_dataset(self, dataset_id): ...
        def list_resources(self, dataset_id): ...
        def get_organization(self, organization_id): ...

    registry.register("dummy", Dummy)
    assert registry.has("dummy")
    registry.unregister("dummy")
    assert not registry.has("dummy")
```

**What to Mock:**
- Nothing via a mocking library. Provide fakes/stubs for collaborators that perform I/O (HTTP, filesystem) if needed.

**What NOT to Mock:**
- Domain models, mappers, pure functions, registry — test against the real implementation.
- Filesystem — use `tmp_path` instead.

> **Note:** Because there is currently no HTTP mocking, the `HttpClient` live-request path is not directly unit-tested. Adapter behavior tests focus on the pure mapper functions (`test_ckan_mapper.py`), which is the testable seam by design.

## Fixtures and Factories

**Shared fixtures** live in `tests/conftest.py`:

```python
# tests/conftest.py
"""Shared pytest fixtures and configuration for DataSluice tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"
```

**Test data pattern:** Inline dict literals representing portal-native JSON. No external factory library, no shared JSON files yet (the `tests/fixtures/<portal>/` dirs are `.gitkeep` placeholders for future fixture files).

```python
# tests/unit/adapters/test_ckan_mapper.py
def test_map_dataset_basic() -> None:
    raw = {
        "id": "ds-1",
        "name": "test-dataset",
        "title": "Test Dataset",
        "notes": "A test dataset.",
        "resources": [{"id": "r1", "url": "https://example.com/r.csv", "format": "csv"}],
        "organization": {"id": "org-1", "name": "my-org"},
        "tags": [{"name": "climate"}, {"name": "weather"}],
    }
    dataset = map_dataset(raw)
    assert dataset.title == "Test Dataset"
    assert dataset.resources[0].format == "CSV"
```

**Filesystem fixtures:** Use the built-in `tmp_path` (a `pathlib.Path`) — no custom fixture needed.

```python
# tests/unit/io/test_io.py
def test_compute_sha256(tmp_path: Path) -> None:
    f = tmp_path / "test.txt"
    f.write_text("hello")
    h = compute_sha256(f)
    assert isinstance(h, str)
    assert len(h) == 64
```

**Location:**
- Shared cross-module fixtures: `tests/conftest.py`.
- Test-specific data: inline in the test function.
- Future portal payload files: `tests/fixtures/<portal>/` (currently empty).

## Coverage

**Requirements:** Minimum **50%** enforced in CI via `fail_under = 50`.

**Configuration** (`pyproject.toml`):
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
    "if TYPE_CHECKING:",
    "@overload",
    "class .*\\bProtocol\\):",
    "@(abc\\.)?abstractmethod",
    "raise NotImplementedError",
    "\\.\\.\\.",
]
```

**View Coverage:**
```bash
just coverage   # Runs pytest under coverage across Python 3.12/3.13/3.14, combines, reports + HTML
# or manually:
uv run coverage run -m pytest && uv run coverage report
uv run coverage html    # → htmlcov/index.html
```

Coverage runs in `parallel` mode and is combined across Python versions — `just coverage` runs 3.12, 3.13, and 3.14 then `coverage combine`.

## Test Types

**Unit Tests:**
- Location: `tests/unit/`.
- Scope: Single module/function/class in isolation. No network, no real portal calls.
- Approach: Exercise pure functions (mappers, normalisers, retry logic, registry), dataclass defaults, and auth strategy `apply()` methods.
- All current tests are unit tests. This is the only test type with real content today.

**Integration Tests:**
- Location: `tests/integration/<portal>/` (currently `.gitkeep` placeholders only).
- Scope: Adapter ↔ live portal round-trips (CKAN, Socrata, data.gouv).
- Approach: Not yet implemented. The directory structure is staged for future live-portal smoke tests.

**E2E Tests:**
- Not used. The CLI commands (`datasluice search`, `datasluice download`) are thin wrappers around `DataSluice` and are not separately end-to-end tested.

**Smoke / Import Tests:**
- `tests/unit/integrations/test_integrations.py` verifies that integration modules import cleanly without their optional deps (parametrised over `pandas`, `polars`, `dlt`, `airflow`, `duckdb`) — guards the lazy-import contract.
- `tests/unit/test_package.py` verifies the public API surface (`__version__`, exports, exception hierarchy).

## Common Patterns

**Async Testing:**
- Not applicable — the codebase is fully synchronous. No `asyncio`, no `pytest-asyncio`.

**Error Testing:**

```python
import pytest
from datasluice.exceptions import AdapterNotFoundError, ChecksumMismatchError

def test_registry_get_unknown_raises() -> None:
    with pytest.raises(AdapterNotFoundError):
        registry.get("nonexistent")

def test_verify_checksum_mismatch(tmp_path: Path) -> None:
    f = tmp_path / "test.txt"
    f.write_text("hello")
    with pytest.raises(ChecksumMismatchError):
        verify_checksum(f, "0" * 64)
```

Pattern: `with pytest.raises(ExpectedError):` block around the call. Do not assert on exception messages unless the message is part of the contract.

**Parametrised Testing:**

```python
# tests/unit/integrations/test_integrations.py
@pytest.mark.parametrize("module_name", ["pandas", "polars", "dlt", "airflow", "duckdb"])
def test_integration_modules_importable(module_name: str) -> None:
    module = importlib.import_module(f"datasluice.integrations.{module_name}")
    assert module is not None
```

Use `@pytest.mark.parametrize` when the same assertion logic applies to multiple inputs. Keep the param list short and self-documenting.

**Iteration/Container Testing:**

```python
# tests/unit/domain/test_models.py
def test_search_result_iteration() -> None:
    result = SearchResult(datasets=[Dataset(id="a"), Dataset(id="b")], total=2)
    assert len(result) == 2
    ids = [d.id for d in result]
    assert ids == ["a", "b"]
```

Test both `__len__` and `__iter__` protocol methods together when a class implements them.

## Pre-commit & CI

**Pre-commit** (`.pre-commit-config.yaml`) runs local hooks for `ty check` and `pytest -q` alongside ruff — so commits are gated on a green test suite. Always invoke as `uv run pre-commit` (not bare `pre-commit`) because hooks call `uv run`.

**CI** (`.github/workflows/ci.yml`):
- `type-check` job: `uv run --all-extras ty check .` — new optional deps must be added to CI install.
- Test matrix: Python 3.12, 3.13, 3.14.
- Coverage gate: `fail_under = 50`.

---

*Testing analysis: 2026-07-05*
