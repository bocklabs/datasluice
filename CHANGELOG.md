# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking Changes

This milestone is a clean break: removed surfaces get no shims, aliases, or deprecation wrappers.

- **Ports re-exports removed.** The `datasluice.ports` exports `Transport`, `StreamingTransport`,
  `ConditionalTransport`, `ConditionalFetchResult`, and `CredentialProvider` are gone together with
  the deleted `ports.transport` / `ports.credentials` modules. Use their replacements under
  `datasluice.runtime.transport` (`CatalogTransport`, `AsyncCatalogTransport`, `RuntimeRequest`,
  `RuntimeResponse`, …) and `datasluice.runtime.credentials`. These are renamed and reshaped
  contracts, not drop-in aliases.
- **`DataSluiceSession` legacy kwargs removed.** The constructor no longer accepts `auth`,
  `page_size`, `timeout`, `retries`, `rate_limit`, or `credential_provider`; passing them raises
  `TypeError`. Configure transports, budgets, TLS policy, breakers, and credential resolvers
  explicitly through the current runtime parameters.
- **`detect()` takes injected probe engines.** `datasluice.discovery.detect()` no longer builds its
  probe engines implicitly; callers pass the probe engines to run.
- **`DEFAULT_RATE_LIMIT` deleted.** Proactive client-side rate limiting was removed along with the
  constant in `datasluice.config`; reactive Retry-After / 429 handling remains in the runtime layer.

## [0.2.3](https://github.com/bocklabs/datasluice/compare/datasluice-v0.2.2...datasluice-v0.2.3) (2026-08-10)


### Bug Fixes

* **provider:** correct project URLs ([#52](https://github.com/bocklabs/datasluice/issues/52)) ([1e07d7b](https://github.com/bocklabs/datasluice/commit/1e07d7b5fa1b7429a55dd2d4f62b567cfdab89f4))

## [0.2.2](https://github.com/bocklabs/datasluice/compare/datasluice-v0.2.1...datasluice-v0.2.2) (2026-08-10)


### Bug Fixes

* Add build distribution workflow and update publish workflow ([#47](https://github.com/bocklabs/datasluice/issues/47)) ([07009dc](https://github.com/bocklabs/datasluice/commit/07009dc54f1fd36f5c47a7e0355e2e0f503bf76b))

## [0.2.1](https://github.com/bocklabs/datasluice/compare/datasluice-v0.2.0...datasluice-v0.2.1) (2026-08-09)


### Bug Fixes

* update datasluice dependency version constraints in pyproject.toml ([#42](https://github.com/bocklabs/datasluice/issues/42)) ([8fcc951](https://github.com/bocklabs/datasluice/commit/8fcc9511a187e4299bd1d5eb8d44a7201945597e))
* update provider version retrieval and remove hardcoded version ([#44](https://github.com/bocklabs/datasluice/issues/44)) ([cd8ee21](https://github.com/bocklabs/datasluice/commit/cd8ee21fcf8259385c78f830a4e1a850ddbf40fc))

## [0.2.0](https://github.com/bocklabs/datasluice/compare/datasluice-v0.1.0...datasluice-v0.2.0) (2026-08-09)


### Features

* onboard gsd and complete refactor ([#25](https://github.com/bocklabs/datasluice/issues/25)) ([5d878cb](https://github.com/bocklabs/datasluice/commit/5d878cb2912196950157fc34ff21ada5911b1de0))


### Bug Fixes

* enhance release workflow by adding GitHub app token generation ([#39](https://github.com/bocklabs/datasluice/issues/39)) ([fcaea5b](https://github.com/bocklabs/datasluice/commit/fcaea5bd4642b8881aedfb9298b179df5d51d419))
* update documentation workflow and enhance renovate configuration ([#37](https://github.com/bocklabs/datasluice/issues/37)) ([4bd5bfc](https://github.com/bocklabs/datasluice/commit/4bd5bfc4b6e7f026165229b9bdfc4e60f76e61fb))
* update documentation workflow to include extra dependencies ([#35](https://github.com/bocklabs/datasluice/issues/35)) ([e33cb3d](https://github.com/bocklabs/datasluice/commit/e33cb3dd659db22e1ca963994a242c326368e8dd))

## [0.1.0] - 2026-06-21

One Python interface for open-data discovery, extraction, format normalization, and pipeline integration.

This release reserves the PyPI package name and establishes the project scaffold.

### Added

- `src/datasluice/` package with CLI (Typer + Rich), py.typed marker
- Tests with pytest, coverage across Python 3.12/3.13/3.14
- CI via GitHub Actions: lint (Ruff), type check (ty), test matrix, coverage reporting
- Security scanning: CodeQL analysis, Dependabot, zizmor workflow audit
- Docs site with Zensical + mkdocstrings, auto-deployed to GitHub Pages
- Trusted publishing to PyPI with OIDC and build provenance attestation
- `justfile` and `Makefile` with dev commands: qa, test, type-check, docs-serve, release
- Issue templates, PR template, contributing guide, code of conduct, security policy
- MIT license, .editorconfig, .gitignore

[Unreleased]: https://github.com/bocklabs/datasluice/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/bocklabs/datasluice/releases/tag/v0.1.0
