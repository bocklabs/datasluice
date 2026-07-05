# Coding Conventions

**Analysis Date:** 2026-07-05

## Toolchain & Enforcement

**Package manager:** `uv` exclusively — never call `pip` directly.

**Formatter/Linter:** Ruff (`ruff v0.13.2` via pre-commit). All checks enforced in pre-commit (`.pre-commit-config.yaml`) and `just qa` / `make qa`.

**Type checker:** `ty` (Astral). Run as `uv run --all-extras ty check .` — `--all-extras` is mandatory so optional lazy-imported deps resolve.

**Python target:** 3.12+ (CI matrix: 3.12, 3.13, 3.14).

## Code Style

**Formatting:**
- Ruff format (`uv run ruff format .`).
- Line length: **120** (`[tool.ruff] line-length = 120`).

**Linting rules** (`[tool.ruff.lint] select`): `E` (pycodestyle errors), `W` (warnings), `F` (Pyflakes), `I` (isort), `B` (flake8-bugbear), `UP` (pyupgrade).

**Pre-commit hygiene hooks** (`.pre-commit-config.yaml`): `trailing-whitespace`, `end-of-file-fixer`, `check-yaml`, `check-toml`, `check-added-large-files` (max 2048 KB), `check-merge-conflict`, `check-case-conflict`, `debug-statements`, `mixed-line-ending` (force `lf`), `check-ast`, `check-docstring-first`.

## Naming Patterns

**Files:**
- Modules are `snake_case.py`: `http_client.py`, `rate_limit.py`, `user_agent.py`.
- One primary class/concern per module (e.g. `RateLimiter` in `rate_limit.py`).
- Test files mirror source: `test_<module>.py` (e.g. `src/.../io/cache.py` → `tests/unit/io/test_io.py`).

**Classes:**
- `PascalCase`: `HttpClient`, `CKANAdapter`, `APIKeyAuth`, `BaseFormatReader`.
- Exception classes suffixed with `Error`: `PortalError`, `DownloadError`, `ChecksumMismatchError`.

**Functions/methods:**
- `snake_case`: `map_dataset`, `compute_sha256`, `build_user_agent`.
- Private/internal prefixed with underscore: `_action`, `_do_request`, `_to_text`, `_build_transport`.
- Mapping functions in adapters are module-level: `map_dataset`, `map_resource`, `map_organization`, `map_license`.

**Variables:**
- `snake_case`: `base_url`, `retry_policy`, `cache_key`.
- Private instance attrs prefixed underscore: `self._transport`, `self._downloader`, `self._last_request`.
- Module-level singletons lowercase: `registry`, `logger`, `console`.

**Constants:**
- `UPPER_SNAKE_CASE`: `_FORMAT_ALIASES`, `READERS`, `DEFAULT_TIMEOUT`, `PATH_FINGERPRINTS`, `HTML_FINGERPRINTS`.
- Module-private constants prefixed `_` when not re-exported.

**Type parameters (PEP 695):**
- Use the new syntax `def with_retry[T](...)` — NOT `TypeVar`. Example: `src/datasluice/transport/retry.py:34`.

## Module Header Pattern

Every source module starts with these two lines, in this order:

```python
"""One-line module docstring (Google style)."""

from __future__ import annotations
```

The `from __future__ import annotations` enables PEP 604 unions (`str | None`) and defers annotation evaluation. **Always present** — see `src/datasluice/exceptions.py`, `src/datasluice/client.py`, every `domain/*.py`.

## Import Organization

Ruff isort (`I`) enforces ordering. Observed order:

1. `from __future__ import annotations` (always first).
2. Standard library (`os`, `time`, `threading`, `pathlib`, `abc`, `csv`, `io`, `json`, `logging`, `urllib.*`, `collections.abc`, `dataclasses`, `typing`).
3. Third-party (`typer`, `rich`).
4. First-party (`datasluice.*`).

Within first-party, subpackages use absolute imports: `from datasluice.domain import Dataset`.

**Path Aliases:** None. Imports are absolute (`from datasluice.adapters.base import BaseAdapter`), except `factory.py` uses one relative import (`from .registry import registry`).

**Lazy imports (critical pattern):** Heavy/optional dependencies (pyarrow, openpyxl, pandas, polars, dlt, duckdb) are imported **inside functions**, never at module top-level. This keeps optional extras truly optional.

```python
# CORRECT — src/datasluice/formats/parquet.py:21-27
def read(self, source: str | Path | bytes) -> list[dict[str, Any]]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise FormatError(
            "Parquet support requires 'pyarrow'. Install with: pip install datasluice[parquet]"
        ) from exc
```

Same pattern in `src/datasluice/formats/xlsx.py`, `src/datasluice/integrations/pandas.py`, `src/datasluice/integrations/dlt.py`, `src/datasluice/integrations/duckdb.py`.

**TYPE_CHECKING guards:** Imports used only for type hints go under `if TYPE_CHECKING:` to avoid circular imports and runtime cost.

```python
# src/datasluice/adapters/base.py:6-12
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from datasluice.auth import BaseAuth
    from datasluice.transport import HttpClient
```

Used consistently in `domain/dataset.py`, `domain/resource.py`, `domain/result.py`, `adapters/base.py`, `adapters/registry.py`, `adapters/factory.py`, `io/downloader.py`, `transport/http_client.py`, `integrations/pandas.py`.

## Type Annotations

- **Fully typed** — public functions, methods, and class attributes are annotated.
- **Unions:** PEP 604 `X | None` syntax (not `Optional[X]`).
- **Collections:** `list[X]`, `dict[str, Any]`, `tuple[A, B]` (not `List`/`Dict`/`Tuple`).
- **Callables:** `collections.abc.Callable` (see `transport/retry.py:6`).
- **Iterators:** `collections.abc.Iterator` (see `domain/result.py:5`).
- **Test functions:** Annotate return as `-> None`. The one exception (`test_csv_reader` in `tests/unit/formats/test_formats.py:11`) carries an inline `# type: ignore[no-untyped-def]`.
- **ClassVars:** Portal type constants use `ClassVar[str]` on `BaseAdapter` (`adapters/base.py:26`).

## Dataclasses

Domain models and configuration use `@dataclass`:

- **Immutable models:** `@dataclass(frozen=True)` for value objects: `Dataset`, `Resource`, `License`, `Organization`, `Query`, `RetryPolicy`. See `src/datasluice/domain/*.py`.
- **Mutable config/containers:** plain `@dataclass` for `Settings` (`config/settings.py`), `SearchResult` (`domain/result.py`), `RateLimiter` (`transport/rate_limit.py`).
- **Mutable defaults** via `field(default_factory=...)`: `field(default_factory=list)`, `field(default_factory=dict)`.
- **Initialised state** in `__post_init__` (e.g. `RateLimiter.__post_init__` validates and sets `_lock`).

## Error Handling

**Strategy:** A single rooted exception hierarchy in `src/datasluice/exceptions.py`. All public APIs raise `DataSluiceError` subclasses — never leak stdlib exceptions across module boundaries for domain-level failures.

```
DataSluiceError
├── PortalError
│   ├── RateLimitError
│   └── NotFoundError
├── AdapterError
│   └── AdapterNotFoundError
├── PortalDetectionError
├── AuthenticationError
├── DownloadError
│   └── ChecksumMismatchError
├── FormatError
└── ConfigError
```

**Chaining rules (enforced by convention):**
- Re-raising from a caught exception: **always** `raise NewError(...) from exc` to preserve the traceback. Examples: `http_client.py:97`, `formats/json.py:49`, `io/local.py:38`.
- Re-raising after catching to suppress context (e.g. dict lookup miss): `raise ... from None`. Examples: `config/settings.py:27`, `adapters/registry.py:46`.

**Rich exceptions:** Some exceptions carry structured data via custom `__init__`:
- `RateLimitError(message, retry_after=...)` — `exceptions.py:33`.
- `ChecksumMismatchError(message, expected=..., actual=...)` — `exceptions.py:49`.

**Stdlib exceptions** (`ValueError`, `KeyError`, `ImportError`, `NotImplementedError`, `OSError`) are used only for:
- Programming/precondition errors inside a single module (e.g. `rate_limit.py:22`, `client.py:118`).
- Registry/factory lookup misses surfaced as `KeyError` (`formats/__init__.py:30`).
- Optional-dependency absence raised as `ImportError`/`FormatError` with an install hint.
- Abstract stubs in `adapters/custom/adapter.py` raise bare `NotImplementedError`.

**Docstring `Raises:` section:** Public methods that raise document every exception. See `adapters/factory.py:34-36`, `io/downloader.py:59-61`, `transport/http_client.py:62-64`.

## Logging

**Framework:** Python stdlib `logging`, wrapped by `src/datasluice/logging.py`.

**Entry points:**
- `get_logger(name)` → returns `logging.getLogger(f"datasluice.{name}")`.
- `configure_logging(level, format_string=...)` → attaches a `StreamHandler` once (guarded by `if not logger.handlers`).

**Module-level logger pattern** — every module that logs declares a module-level logger:

```python
from datasluice.logging import get_logger

logger = get_logger("client")          # src/datasluice/client.py:23
logger = get_logger("transport.http")  # src/datasluice/transport/http_client.py:22
logger = get_logger("transport.retry") # src/datasluice/transport/retry.py:12
logger = get_logger("io.downloader")   # src/datasluice/io/downloader.py:18
```

**Format:** `%(asctime)s [%(name)s] %(levelname)s: %(message)s`.

**Convention:** Use %-style lazy interpolation (never f-strings in log calls):

```python
logger.debug("Initialised DataSluice for %s (%s)", portal_url, self.adapter.portal_type)
logger.warning("Attempt %d/%d failed: %s — retrying in %.1fs", attempt, policy.max_attempts, exc, sleep)
logger.error("Failed to download %s: %s", resource.id, exc)
```

Level default `INFO`, configurable via `DATASLUICE_LOG_LEVEL` env var.

## Comments

**No inline/block comments in code unless explicitly requested.** This is a hard rule (see `AGENTS.md`). The codebase contains effectively zero `# explanation` comments.

The only acceptable comment forms:
- `# type: ignore[...]` — targeted ty suppressions (`transport/retry.py:69`, `tests/unit/formats/test_formats.py:11`).
- `# type: ignore` — broad suppression for dynamic returns (`integrations/pandas.py:34`).
- Docstring-driven documentation only.

## Docstrings

**Style:** Google format. First line is a one-line imperative summary.

Every **public** module, class, and function has a docstring. Structure:

```python
def create_adapter(base_url: str, *, portal_type: str | None = None, auth: BaseAuth | None = None) -> BaseAdapter:
    """Instantiate an adapter for the given *base_url*.

    If *portal_type* is ``None`` the portal type is auto-detected using
    :mod:`datasluice.discovery`.

    Args:
        base_url: Root URL of the portal.
        portal_type: Explicit portal type (e.g. ``"ckan"``).

    Returns:
        An adapter instance ready for use.

    Raises:
        PortalDetectionError: If auto-detection fails.
    """
```

Conventions:
- Module docstring: one line, e.g. `"""CKAN adapter implementation."""`.
- Class docstrings: summary + optional `Attributes:` / `Args:`.
- Methods: `Args:`, `Returns:`, `Raises:` sections as applicable.
- Reference names in prose with asterisks: `*base_url*`, `*dataset_id*`.
- Reference modules/types with double-backticks: `` ``"ckan"`` ``, `:class:`Dataset``, `:mod:`datasluice.discovery``.

## Function Design

**Size:** Small and focused. Most functions < 30 lines. The largest (`HttpClient.request`, `Downloader.download`) stay under ~45 lines by delegating to helpers.

**Keyword-only arguments:** Use `*` separator for optional/configuration params on constructors and methods:

```python
def __init__(self, base_url: str, *, auth: BaseAuth | None = None, transport: HttpClient | None = None) -> None:
def request(self, url: str, *, method: str = "GET", params: ..., headers: ..., body: ...) -> bytes:
def download(self, resource, dest=None, *, filename=None, verify_hash=None, hash_algorithm="sha256") -> Path:
```

**`**kwargs` pass-through:** Used where downstream APIs accept arbitrary options (`DataSluice.search(**kwargs)`, `HttpClient.get_text(**kwargs)`).

**Return values:** Always typed. List-returning methods declare `list[X]`; dict-returning declare `dict[str, Any]`.

**Lazy property initialisation:** Heavy collaborators built on first access via a `@property`:

```python
@property
def transport(self) -> HttpClient:
    if self._transport is None:
        from datasluice.transport import HttpClient
        self._transport = HttpClient(auth=self.auth)
    return self._transport
```

See `src/datasluice/client.py:78` (`downloader`) and `src/datasluice/adapters/base.py:39` (`transport`).

## CLI (Typer) Conventions

**Entry point:** `src/datasluice/cli/app.py` — a `typer.Typer(name="datasluice", no_args_is_help=True)` instance.

**Command argument pattern:** Use `Annotated[T, typer.Option(...)]` / `Annotated[T, typer.Argument(...)]`, **not** `param: T = typer.Option(...)`. The `B008` rule (flake8-bugbear) rejects function calls in argument defaults.

```python
# CORRECT — src/datasluice/cli/download.py:14-19
def download(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    dest: Annotated[Path, typer.Option("--dest", "-o", help="Destination directory")] = Path("."),
) -> None:
```

**Output:** `rich.console.Console` for all CLI output. Tables via `rich.table.Table` with style tags (`[cyan]`, `[dim]`, `[green]`, `[bold]`). See `src/datasluice/cli/search.py`.

**Command registration:** Via `app.command(name=...)(func)` in `app.py`, keeping subcommand modules importable and testable independently.

**Lazy imports in CLI:** Heavy imports (`from datasluice import DataSluice`) happen inside the command function body, not at module top, to keep CLI startup fast.

## Module Design

**Barrel files:** Each subpackage exposes its public API via `__init__.py` with an explicit `__all__` list. Examples: `domain/__init__.py`, `transport/__init__.py`, `io/__init__.py`, `auth/__init__.py`, `formats/__init__.py`, `adapters/__init__.py`, top-level `datasluice/__init__.py`.

**`__all__`** is alphabetical-ish and lists every re-exported name.

**Side-effect registration:** Importing `datasluice.adapters` triggers built-in adapter registration into the module-level `registry` singleton (`src/datasluice/adapters/__init__.py:18-21`). This is intentional and documented.

**Module-level singletons:** `registry` (`adapters/registry.py:61`), `READERS` dict (`formats/__init__.py:10`), `console` (CLI modules), `logger` (every module that logs).

**Version separation:** `__version__` lives in `src/datasluice/_version.py` (NOT `__init__.py`) to break a circular import with `transport/user_agent.py`. Do not move it.

**Adapter subpackage layout:** Each portal adapter follows a fixed four-file layout:
- `adapter.py` — `XxxAdapter(BaseAdapter)` implementation.
- `mapper.py` — pure functions translating portal JSON → domain models.
- `pagination.py` — pagination helpers.
- `errors.py` — portal-specific error mapping.

Mappers are **pure functions** (no I/O), making them trivially unit-testable. See `src/datasluice/adapters/ckan/mapper.py`.

**`__repr__` methods:** Key classes implement a concise repr for debugging:

```python
def __repr__(self) -> str:
    return f"<{type(self).__name__}({self.portal_type!r}, {self.base_url!r})>"
```

See `src/datasluice/client.py:132`, `src/datasluice/adapters/base.py:64`.

## Concurrency

`RateLimiter` (`src/datasluice/transport/rate_limit.py`) is the only thread-aware component — it uses `threading.Lock` for thread-safe rate limiting. The rest of the library is synchronous and single-threaded.

---

*Convention analysis: 2026-07-05*
