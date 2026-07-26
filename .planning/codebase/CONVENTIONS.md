# Coding Conventions

**Analysis Date:** 2026-07-26

## Language & Target

- **Python 3.12+** (`requires-python = ">= 3.12"` in `pyproject.toml`). CI matrix runs 3.12, 3.13, 3.14.
- Use modern syntax: `str | None` unions (not `Optional[str]`), `list[...]` (not `List[...]`), walrus, `match`. The `UP` (pyupgrade) ruff rule enforces this.
- Use **PEP 695 type params** — `def func[T](...)` / `class Generic[T]` — not `TypeVar`. Do not import from `typing` what `collections.abc` provides.

## Code Style

**Formatting:** Ruff format (line length **120**, configured in `pyproject.toml` `[tool.ruff]`).

**Linting:** Ruff check with selects `E, W, F, I, B, UP` (`pyproject.toml` `[tool.ruff.lint]`). Run with auto-fix:
```bash
uv run ruff format .
uv run ruff check . --fix
```

**Type checking:** `ty` (Astral), all rules as errors by default. `--all-extras` is **required** so optional lazy deps resolve:
```bash
uv run --all-extras ty check .
```

**Pre-commit** (`.pre-commit-config.yaml`) runs: trailing-whitespace, end-of-file-fixer, check-yaml/toml, ruff `--fix`, ruff-format, `ty check` (local hook), `pytest -q` (local hook). Always invoke as `uv run pre-commit`, never bare `pre-commit`.

**Editor config** (`.editorconfig`): UTF-8, LF line endings, 4-space indent for Python, trim trailing whitespace, final newline.

## Module Header Pattern

Every `.py` file begins with, in this order:

1. Module docstring (Google-style, one-line summary first).
2. `from __future__ import annotations` (universal — enables deferred evaluation).
3. stdlib imports.
4. third-party imports.
5. local (`datasluice...`) imports.

Example from `src/datasluice/transport/http_client.py`:
```python
"""HTTP client with retry, rate-limiting, and authentication support."""

from __future__ import annotations

import email.utils
import json as json_module
...
from datasluice.exceptions import PortalError, RateLimitError, RetryableHTTPError
```

## Naming Patterns

**Files:** `snake_case.py` — e.g. `http_client.py`, `host_provider.py`, `user_agent.py`.

**Modules / packages:** short snake_case — `transport/`, `credentials/`, `formats/`, `runtime/`.

**Classes:** `PascalCase`. Abstract bases prefixed `Base` (`BaseAdapter`, `BaseAuth`, `BaseFormatReader`). Protocols suffixed `Port` or descriptive (`Transport`, `CachePort`, `CatalogPort`, `CredentialProvider`).

**Functions / methods:** `snake_case`. Private/internal prefixed `_` (`_parse_retry_after`, `_truncate_body`, `_do_request`, `_action`).

**Variables:** `snake_case`.

**Constants:** `UPPER_SNAKE`, module-level, often `_`-prefixed when private (`DEFAULT_TIMEOUT`, `DEFAULT_PAGE_SIZE`, `SENSITIVE_HEADERS`, `_FORMAT_ALIASES`, `_logger_name`).

**ClassVar attributes:** declare with `typing.ClassVar` — `portal_type: ClassVar[str] = "ckan"` (`src/datasluice/connectors/base.py`), `format_name: ClassVar[str]` (`src/datasluice/formats/base.py`).

## Import Organization

**Order** (enforced by ruff `I`/isort):
1. Standard library.
2. Third-party (`typer`, `rich`, `pytest`).
3. First-party (`datasluice...`, `tests...`).

**Path / pythonpath:** `pythonpath = ["src", "."]` (`pyproject.toml` `[tool.pytest.ini_options]`). Import source as `from datasluice.domain import Dataset`, never `from src.datasluice...`. Tests import helpers as `from tests.helpers.http_server import ...`.

**Lazy imports:** heavy optional dependencies (pyarrow, openpyxl, pandas, polars, dlt, duckdb, fsspec, airflow) are imported **inside functions**, not at module top-level. This keeps the package importable without optional extras. Example from `src/datasluice/formats/parquet.py`:
```python
def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise FormatError("Parquet support requires 'pyarrow'...") from exc
```
Also used for runtime wiring in `src/datasluice/runtime/session.py` (`from datasluice.discovery import detect_portal_type` inside `portal()`) and `src/datasluice/connectors/base.py` (`from datasluice.transport import HttpClient` inside the lazy `transport` property).

**`TYPE_CHECKING` guards:** import-only type references go under `if TYPE_CHECKING:` to avoid runtime import cost / cycles. Example from `src/datasluice/connectors/base.py`:
```python
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.ports import Transport
```

**Barrel `__init__.py` files:** every package exposes its public surface via an explicit, alphabetically-sorted `__all__` list. See `src/datasluice/__init__.py`, `src/datasluice/domain/__init__.py`, `src/datasluice/auth/__init__.py`.

## Comments

**No explanatory comments** in code unless explicitly requested (per `AGENTS.md`). The only inline comments present are **linter directives**:
- `# noqa: E402` — imports after a non-import line (used in RED-commit test files and post-`importorskip` imports).
- `# noqa: F401` — intentional re-export of unused name.
- `# type: ignore[...]` / `# ty: ignore[...]` — suppress type-checker diagnostics where dynamic behaviour is intentional (`src/datasluice/transport/__init__.py:31`, `src/datasluice/transport/retry.py:73`, `src/datasluice/integrations/airflow.py:56`, `src/datasluice/integrations/pandas.py:34`).

All other intent is conveyed through docstrings.

## Docstrings

**Google style**, first line is a concise summary. Use `Args:`, `Returns:`, `Raises:`, `Attributes:`, `Example:` sections as applicable. RST cross-references with `` :class:`...` `` / `` :func:`...` `` / `` :mod:`...` `` appear in API-facing modules (consumed by mkdocstrings). Examples:

- Class with attributes — `src/datasluice/domain/dataset.py` (`Dataset`).
- Function with Args/Returns/Raises — `src/datasluice/runtime/session.py` (`DataSluiceSession.portal`).
- Class with Args + Example — `src/datasluice/runtime/session.py` (`DataSluiceSession`).

## Error Handling

**Exception hierarchy** (`src/datasluice/exceptions.py`) — single root `DataSluiceError(Exception)`:

```
DataSluiceError
├── PortalError              (portal unreachable / non-2xx)
│   ├── RateLimitError        (429, carries retry_after)
│   ├── RetryableHTTPError    (5xx, carries status_code)
│   └── NotFoundError
├── AdapterError
│   └── AdapterNotFoundError
├── PortalDetectionError
├── AuthenticationError
├── DownloadError
│   └── ChecksumMismatchError (carries expected/actual)
├── FormatError
└── ConfigError
```

**Patterns:**
- **Raise, don't return None, for failure.** Each domain error type maps to a distinct caller-recoverable condition.
- **Wrap third-party exceptions** with `from exc` to preserve the chain: `raise FormatError(...) from exc` (`src/datasluice/formats/parquet.py:25`).
- **Carry diagnostic context** on the exception instance: `RateLimitError(message, retry_after=...)`, `RetryableHTTPError(message, status_code=...)`, `ChecksumMismatchError(message, expected=..., actual=...)`.
- **Map HTTP status → exception** centrally in `src/datasluice/transport/http_client.py` `_do_request`: 429 → `RateLimitError`, ≥500 → `RetryableHTTPError`, else → `PortalError`; `URLError` → `PortalError`.
- **Retry classification** via `RetryPolicy.retry_on` tuple in `src/datasluice/transport/retry.py`; only listed exception types are retried (full-jitter backoff).

## Logging

**Framework:** Python stdlib `logging` only — no third-party logging library.

**Patterns:**
- Obtain a logger via the factory `from datasluice.logging import get_logger`, then `logger = get_logger("transport.http")` / `get_logger("session")` — the name is appended to the package root `"datasluice"`.
- Configure via `configure_logging(level, format_string=None, **kwargs)` (`src/datasluice/logging.py`) which attaches a `StreamHandler` with a `RedactingFilter`.
- **Secret redaction** is automatic: `RedactingFilter(logging.Filter)` walks `record.__dict__` and `record.args` dicts, replacing values whose lower-cased key is in `_SENSITIVE_KEYS` (`authorization`, `cookie`, `x-api-key`, `api_key`, `token`, `secret`, `password`, …) with `"***"`. Targeted by **key name**, never by value-pattern heuristics, so legitimate base64 / open-data payloads pass through unchanged. Env var `DATASLUICE_NO_REDACT=1` is the debug escape hatch.
- `SENSITIVE_HEADERS` frozenset is the single source of truth, lifted into `src/datasluice/logging.py` so the redacting filter and the redirect handler share it without a circular import.
- Log levels: `logger.debug(...)` for wiring/lifecycle (`"transport= injected; ... scalars ignored"`), `"Resolved connector %s for %s"`. Avoid `print()` — the CLI uses `rich.console.Console` for user-facing output, not logging.

## Function Design

**Constructors:** keyword-only (`*` after `self`) with `None` defaults and `or`-fallback to a default instance. Example from `src/datasluice/transport/http_client.py`:
```python
def __init__(
    self,
    *,
    auth: BaseAuth | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    retry_policy: RetryPolicy | None = None,
    ...
) -> None:
    self.auth = auth or NoAuth()
    self.retry_policy = retry_policy or RetryPolicy()
```
This enables zero-config construction while allowing dependency injection (D-02 / D-P3-05).

**Return types:** explicit. Public API returns domain models, `bytes`, `dict[str, Any]`, or `list[...]` — never bare `Any` on typed surfaces.

**Mutability:** domain models are `@dataclass(frozen=True)` (immutable value objects — `Dataset`, `Resource`, `License`, `Query`). Builders/services (`HttpClient`, `PluginManager`, `HostCredentialProvider`) are mutable classes with private `_`-prefixed state.

## Typer / CLI

**Entry point:** `datasluice.cli.app:app` (`pyproject.toml` `[project.scripts]`). A single `typer.Typer(name="datasluice", no_args_is_help=True)` with a `@app.callback()` global (`--version`/`-V`) and commands registered via `app.command(name=...)(fn)` (`src/datasluice/cli/app.py`).

**Option declaration — preferred pattern (`Annotated`):**
```python
from typing import Annotated
import typer

def download(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    dest: Annotated[Path, typer.Option("--dest", "-o", help="...")] = Path("."),
) -> None:
```
This satisfies the flake8-bugbear **B008** rule (no function call in argument default). Used in `src/datasluice/cli/download.py`. **Always use this `Annotated` form for new CLI commands.**

> Note: `src/datasluice/cli/app.py` (`main`) and `src/datasluice/cli/search.py` still use the legacy `param: str = typer.Option(...)` form, which violates B008. These are pre-existing inconsistencies — new code must use `Annotated`.

**Output:** `rich.console.Console` for all user-facing output (`Table`, styled spans like `[green]...[/green]`). Console instantiated at module scope: `console = Console()`.

## Module Design

**Exports:** explicit `__all__` list in every package `__init__.py`. Re-export only what is part of the stable public API.

**Lazy attribute access:** `src/datasluice/transport/__init__.py` uses a module-level `__getattr__` to import `HttpClient` / `HttpxTransport` lazily, deferring the (optional `httpx`) import until first attribute access.

**Dependency-injected composition root:** `DataSluiceSession` (`src/datasluice/runtime/session.py`) is the single facade. It wires `PluginManager`, transport, auth, cache, and storage via explicit kwargs — no env-var-driven `Settings` system (removed: CORR-04 / D-14 / D-10; enforced by `tests/unit/test_no_dead_settings.py`). The connector registry is always an injected `PluginManager` instance, never a module-level singleton (enforced by `tests/unit/runtime/test_no_global_state.py`).

## Secret Safety (cross-cutting)

- `__repr__` on auth classes **must redact** secret material. Enforced by `tests/unit/auth/test_auth.py` (`test_bearer_auth_repr_redacts_token`, etc.): assert the secret string is absent and `"***"` is present. `src/datasluice/auth/bearer.py`:
  ```python
  def __repr__(self) -> str:
      return f"<BearerAuth scheme={self.scheme!r} token=***>"
  ```
- `DataSluiceSession.__repr__` must not leak secrets — enforced by `tests/unit/runtime/test_session.py::test_repr_has_no_secrets`.
- Cross-host redirects strip credentials by default (`src/datasluice/transport/redirect.py` `CredentialAwareRedirectHandler`); `CredentialScope` opt-in retains them on allow-listed hosts.

---

*Convention analysis: 2026-07-26*
