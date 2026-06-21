# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/nitish-raj/datasluice/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nitish-raj/datasluice/releases/tag/v0.1.0
