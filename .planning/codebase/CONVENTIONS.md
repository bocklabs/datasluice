# Coding Conventions

**Analysis Date:** 2026-07-30

## Language & Toolchain Baseline

- **Target Python:** `>=3.12` (declared in `pyproject.toml`). CI matrix runs 3.12, 3.13, 3.14.
- **Formatter/Linter:** Ruff (config in `pyproject.toml` under `[tool.ruff]`). Line length **120**.
- **Type checker:** `ty` (Astral), all rules as errors by default (`[tool.ty]`). Run with `uv run --all-extras ty check .`.
- **Type hints are mandatory.** The package ships `src/datasluice/py.typed`; the classifier `Typing :: Typed` is set.
- **Package manager:** `uv` exclusively — never call `pip` directly. Full install: `uv sync --all-extras`.

## Naming Patterns

**Modules/Files:**
- `snake_case.py` — e.g. `state_store.py`, `batch_stream.py`, `resource_reader.py`.
- One concept per module; the module name matches the primary class/concept (`dataset.py` → `Dataset`).

**Classes:**
- `PascalCase`. Domain models are nouns: `Dataset`, `Resource`, `SearchResult`, `SyncState`.
- Exceptions end in `Error`: `DataSluiceError`, `DownloadError`, `FormatError`, `PortalError`.
- Adapters: `<Portal>Adapter` (e.g. `CKANAdapter`, `DatagouvAdapter`, `SocrataAdapter`) in `connectors/<portal>/adapter.py`.

**Functions:**
- `snake_case`. Mappers are prefixed `map_`: `map_dataset`, `map_resource`, `map_organization` (`src/datasluice/connectors/ckan/mapper.py`).
- Internal helpers prefixed `_`: `_resolve_access`, `_reject_unsupported_fields`, `_check_publishs_catalog_capabilities`.
- Factory functions named `create_<portal>_connector` and registered as entry-points (`pyproject.toml` → `[project.entry-points."datasluice.connectors"]`).

**Variables/constants:**
- Module-level constants are `UPPER_SNAKE`: `_FORMAT_ALIASES`, `_QUERY_DECLARATION_ORDER`, `_CKAN_SUPPORTED_QUERY_FIELDS`, `SENSITIVE_HEADERS`.
- Private constants may keep a leading `_`.

**Type parameters:**
- **PEP 695 syntax is the standard.** Use `def func[T](...)`, never `TypeVar`. Confirmed in `src/datasluice/transport/pagination.py:22` (`def paginate[T]`) and `src/datasluice/transport/retry.py:40` (`def with_retry[T]`). No `TypeVar` usage exists in `src/`.

## Code Style

**Formatting (Ruff):**
- Line length 120.
- `from __future__ import annotations` appears at the top of essentially every module (PEP 563 deferred annotations). Keep it.
- Double quotes for strings.

**Linting (Ruff `select`):**
```
E   # pycodestyle errors
W   # pycodestyle warnings
F   # Pyflakes
I   # isort
B   # flake8-bugbear
UP  # pyupgrade
```
- B008 (no function calls in argument defaults) is enforced — see the Typer pattern below.
- `# noqa: <code>` is used sparingly and annotated with a reason, e.g. `# noqa: F401 — lazy import gate` (`src/datasluice/integrations/polars.py:35`), `# noqa: E402` for imports after a `pytest.skip` guard (`tests/unit/connectors/test_ckan_reject_policy.py:26`).

**Modern type syntax (pyupgrade / UP):**
- Use `X | None` not `Optional[X]`; `list[T]` not `List[T]`; `dict[str, Any]` not `Dict`. Example: `id: str`, `title: str | None = None` (`src/datasluice/domain/dataset.py:36-37`).
- Use `from collections.abc import Callable, Iterator, Mapping` — not `typing.Callable`.

## Import Organization

**Order (Ruff isort):**
1. `from __future__ import annotations`
2. Standard library (`import json`, `from pathlib import Path`)
3. Third-party (`import typer`, `from rich.console import Console`)
4. First-party (`from datasluice.domain import Dataset`)

**Path Aliases:**
- None. Imports are absolute and fully-qualified: `from datasluice.connectors.ckan.mapper import map_dataset`.
- pytest config sets `pythonpath = ["src", "."]` (`pyproject.toml`), so tests import `from tests.helpers.http_server import ...` and `from datasluice... import ...`.

**TYPE_CHECKING guards:**
- Import type-only dependencies under `if TYPE_CHECKING:` to avoid circular imports and runtime cost. Pattern in `src/datasluice/connectors/base.py:10-12`, `src/datasluice/domain/dataset.py:8-11`, `src/datasluice/ports/catalog.py:7-8`:
```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from datasluice.domain import Organization, Query, SearchResult
```

## Error Handling

**Strategy:** A single rooted exception hierarchy. Every public failure raises a subclass of `DataSluiceError`. Callers can catch the broad base or a specific branch.

**Hierarchy** (defined in `src/datasluice/exceptions.py`):
```
DataSluiceError (base)
├── PortalError                       # portal returned error / unreachable
│   ├── RateLimitError                # HTTP 429; carries .retry_after
│   ├── RetryableHTTPError            # HTTP 5xx; carries .status_code
│   └── NotFoundError                 # missing dataset/resource
├── AdapterError                      # adapter cannot fulfil request
│   └── AdapterNotFoundError          # no adapter registered for portal type
├── PortalDetectionError              # detection failed; carries .detection_result
├── AuthenticationError               # missing/invalid credentials
├── DownloadError                     # resource download failed
│   ├── ChecksumMismatchError         # carries .expected/.actual
│   └── UnsupportedAccessError        # no reader for an access Kind
├── FormatError                       # parse failure
│   ├── DecompressionError            # decompression failure
│   └── TransformError                # transform step failure
├── ConfigError                       # invalid/incomplete config
├── StateStoreError                   # durable sync-state I/O failure
│   └── SyncStateConflictError        # optimistic-CAS race lost
├── StreamClosedError                 # use-after-close on a BatchStream
├── SchemaUnificationError            # pyarrow schema promotion failure
└── UnsupportedQueryFieldError        # pre-flight: Query filter not supported
```

**Patterns:**
- **Rich exception constructors with keyword-only params.** When an exception carries structured data, give it explicit attributes and a `__init__`. See `RateLimitError(message, retry_after=None)`, `ChecksumMismatchError(message, expected=None, actual=None)`, and the fully kw-only `UnsupportedQueryFieldError(*, field, supported_fields, portal_name)` (`src/datasluice/exceptions.py:169`).
- **Docstrings explain *why* each subclass hangs where it does.** E.g. `UnsupportedQueryFieldError` is a direct child of `DataSluiceError`, not under `PortalError`, because the reject policy fires pre-flight before any portal contact (`src/datasluice/exceptions.py:154-167`).
- **Catch and re-raise with context** for optional-dependency gating, converting `ImportError` into an actionable `ImportError` with install instructions:
```python
try:
    import polars as pl  # noqa: F401 — lazy import gate
except ImportError as exc:
    raise ImportError("to_polars requires 'polars'. Install with: pip install datasluice[polars]") from exc
```
(`src/datasluice/integrations/polars.py:34-37`.)
- **Always chain:** `raise ... from exc` when wrapping.
- **Fail loud, never silently swallow** corrupt state — see `StateStoreError` docstring: "staleness is worse than a loud failure" (`src/datasluice/exceptions.py:106-114`).

## Lazy Imports (Heavy Optional Deps)

Heavy third-party deps are imported **inside functions, not at module top-level**, so the core package imports cheaply without optional extras. Keep this invariant.

**Where it applies:** pyarrow, openpyxl, pandas, polars, dlt, duckdb, fsspec, zstandard.

**Two patterns observed:**

1. **Lazy import gate inside a function** (`src/datasluice/integrations/`, `src/datasluice/transforms/`, `src/datasluice/sync/materialize.py`):
```python
def to_polars(stream: BatchStream) -> Any:
    try:
        import polars as pl  # noqa: F401 — lazy import gate
    except ImportError as exc:
        raise ImportError("to_polars requires 'polars'. Install with: ...") from exc
    from datasluice.integrations.arrow import to_arrow
    return pl.from_arrow(to_arrow(stream))
```

2. **Lazy property for transport** (`src/datasluice/connectors/base.py:47-54`) — `HttpClient` is only constructed when `.transport` is first accessed.

`--all-extras` is required for `ty check` and pre-commit because `ty` resolves these lazy imports.

## Docstrings

**Style:** Google format. Every public module, class, and function has a docstring.

**Structure:**
- **Module docstring:** first line is an imperative summary; may add paragraphs of design rationale referencing decision IDs (e.g. `D-P5-14`, `ARCH-08`, `QUAL-02`). See `src/datasluice/contracts/checks.py:1-36`.
- **Class:** summary line + `Attributes:` section listing each field with type and meaning. Example: `src/datasluice/domain/dataset.py:16-34`.
- **Function:** summary + optional `Args:` / `Returns:` / `Raises:` sections.

```python
def run_contract_suite(
    connector_factory: Callable[[ConnectorContext], BaseAdapter],
    fixture_set: Mapping[str, Any],
    *,
    base_url: str,
    transport: Transport,
) -> None:
    """Run all 8 conformance checks (D-P5-14) against a fixture-served connector.

    Args:
        connector_factory: A ``create_*_connector(ctx)`` callable ...
        fixture_set: Parsed fixture payloads keyed by fixture name. ...

    Raises:
        AssertionError: On the first failing conformance check. ...
    """
```
(`src/datasluice/contracts/checks.py:52-83`.)

**Comments in code:** none unless explicitly requested (per `AGENTS.md`). Rationale lives in docstrings, not inline comments.

## Module Design

**Exports:**
- Every package has an `__init__.py` that re-exports its public surface and defines `__all__`. See `src/datasluice/domain/__init__.py`, `src/datasluice/ports/__init__.py`, `src/datasluice/contracts/__init__.py`.
- `__all__` is **sorted alphabetically** (`src/datasluice/domain/__init__.py:17-36`).

**Domain models are `@dataclass(frozen=True)`:**
- Immutable value objects in `src/datasluice/domain/`. Mutable defaults use `field(default_factory=...)`.
- Optional fields default to `None`; collections default to empty via `field(default_factory=list)` / `dict`.

**Ports are `@runtime_checkable` Protocols:**
- Boundary interfaces in `src/datasluice/ports/`. Decorated `@runtime_checkable` so the runtime/contracts probe capabilities with `isinstance` (`src/datasluice/ports/catalog.py:11`, `src/datasluice/ports/transport.py:18`).
- Capability protocols are narrow and separate (e.g. `Transport`, `StreamingTransport`, `ConditionalTransport`) so a backend advertises only what it implements.
- **Important gotama:** do NOT declare capability methods on `BaseAdapter` — only on the Protocol. Declaring `get_organization` on the base would make `isinstance` short-circuit and every adapter falsely satisfy `OrganizationCatalog` (python/typing#800). See `src/datasluice/connectors/base.py:20-28`.

## Adapter Pattern

Each portal connector lives in `src/datasluice/connectors/<portal>/` with a fixed file layout:

| File | Responsibility |
|------|----------------|
| `adapter.py` | `<Portal>Adapter(BaseAdapter)` — orchestrates transport calls, maps responses, applies the reject gate |
| `mapper.py` | Pure `map_*` functions translating portal-native JSON → `datasluice.domain` models |
| `pagination.py` | Portal-specific pagination logic (`<Portal>Page`) |
| `errors.py` | Portal-specific exception subclasses (e.g. `connectors/ckan/errors.py`) |
| `factory.py` | `create_<portal>_connector(ctx)` entry-point target |

**Adapter conventions** (from `src/datasluice/connectors/ckan/adapter.py:28-63`):
- Declare a `portal_type: ClassVar[str]` (e.g. `"ckan"`).
- Publish a `capabilities: ClassVar[CatalogCapabilities]` enumerating `supported_query_fields`.
- Every `search()` starts with the pre-flight reject gate: `_reject_unsupported_fields(query, self.capabilities.supported_query_fields, "<portal>")` (from `src/datasluice/connectors/_reject.py`). No transport call before the gate.
- Mappers are pure functions in `mapper.py` — no transport access, easy to unit-test with dicts.

## Typer / CLI Conventions

- **Use `Annotated[T, typer.Option(...)]`** (preferred form), NOT function-call defaults, because B008 rejects calls in defaults:
```python
def download(
    portal: Annotated[str, typer.Option("--portal", "-p", help="Portal base URL")],
    dataset_id: Annotated[str, typer.Argument(help="Dataset ID")],
    dest: Annotated[Path, typer.Option("--dest", "-o", help="Destination directory")] = Path("."),
    fmt: Annotated[str | None, typer.Option("--format", "-f", help="Filter resources by format")] = None,
) -> None:
```
(`src/datasluice/cli/download.py:14-19`.)

  > Note: `src/datasluice/cli/search.py` still uses the older `param: str = typer.Option(...)` form. The `Annotated` form in `download.py` is the target pattern — follow it for new commands.

- CLI modules import heavy/session objects **lazily inside the function body** (`from datasluice import DataSluiceSession` inside `download`/`search`) to keep CLI startup fast.
- Output uses `rich` (`Console`, `Table`) with inline markup like `[bold]`, `[green]`, `[yellow]`, `[dim]`.
- Exit with `raise typer.Exit(1)` on error conditions (not `sys.exit`).
- Commands are registered on the Typer app: `app.command(name="download")(download)` (`src/datasluice/cli/app.py:39-42`).

## Logging

**Framework:** stdlib `logging` via a thin wrapper in `src/datasluice/logging.py`.

- Get a logger with `get_logger("subsystem")` → returns `logging.getLogger("datasluice.subsystem")`.
- **Secret redaction is mandatory.** `RedactingFilter` walks log records and replaces values whose lowercased key is in `SENSITIVE_KEYS` with `"***"`. `SENSITIVE_HEADERS = frozenset({"authorization", "cookie", "x-api-key", "x-auth-token"})` is the single source of truth.
- Redaction targets **known keys only, never value-pattern heuristics** — legitimate base64/open-data payloads pass through unchanged (see `RedactingFilter` docstring, `src/datasluice/logging.py:45-67`).
- `DATASLUICE_NO_REDACT=1` env var disables redaction (test/debug escape hatch).

## Cross-Cutting Patterns

**Design-decision IDs:** Code and docstrings reference decision/requirement IDs (`D-P5-14`, `ARCH-08`, `QUAL-02`, `INFRA-06`, `CONN-01`). Preserve these when editing the relevant code; they trace to the planning docs.

**Public API stability:** Public contract surfaces have signature-stability tests (e.g. `tests/unit/contracts/test_contract_api.py` asserts `run_contract_suite`'s param kinds). Do not change the public signature of locked functions without updating these tests.

**No comments in code** unless explicitly requested. Design intent goes in docstrings.

---

*Convention analysis: 2026-07-30*
