# Contributing to DataSluice

Contributions are welcome, and they are greatly appreciated! Every little bit helps, and credit will always be given.

---

## Conventional Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Every commit message **must** start with a type prefix so [Release Please](https://github.com/googleapis/release-please) can generate versions and changelogs automatically.

```
<type>(<optional scope>): <description>

[optional body]

[optional footer(s)]
```

### Supported types

| Type       | Bump      | Section in changelog   |
|------------|-----------|------------------------|
| `feat`     | minor     | Features               |
| `fix`      | patch     | Bug Fixes              |
| `perf`     | patch     | Performance            |
| `revert`   | *reverts* | Reverts                |
| `docs`     | none      | Documentation          |
| `refactor` | none      | Code Refactoring       |
| `test`     | none      | Tests                  |
| `build`    | none      | Build System           |
| `ci`       | none      | Continuous Integration |
| `chore`    | none      | Miscellaneous Chores   |

> **Breaking change:** add `!` after the type/scope (e.g. `feat!: drop Python 3.11`) or a `BREAKING CHANGE:` footer. This triggers a **major** version bump.

### Examples

```
feat: add Socrata resource format detection
fix(ckan): handle paginated package_search results
docs: document optional dependency extras
ci: add wheel smoke test to CI
```

---

## Branching Strategy

| Branch            | Purpose                                              | Lifetime |
|-------------------|------------------------------------------------------|----------|
| `main`            | Production branch — always releasable                | Permanent|
| `feat/*`          | New features                                         | Temporary|
| `fix/*`           | Bug fixes                                            | Temporary|
| `docs/*`          | Documentation improvements                           | Temporary|
| `refactor/*`      | Code refactoring (no behavior change)                | Temporary|
| `chore/*`         | Tooling, deps, maintenance                           | Temporary|
| `test/*`          | Test additions/improvements                          | Temporary|

**All pull requests target `main`.** There are no `develop`, `staging`, or `release` branches — release state is represented by Git tags created by Release Please.

---

## Development Setup

1. **Fork & clone:**

   ```bash
   git clone git@github.com:your-username/datasluice.git
   cd datasluice
   ```

2. **Install dependencies** (including all optional extras for type checking and dev):

   ```bash
   uv sync --all-extras
   ```

3. **Install pre-commit hooks:**

   ```bash
   uv run pre-commit install
   ```

4. **Install `just`** (task runner — optional but recommended):

   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to .venv/bin
   ```

   If `just` is missing, `make` works as a zero-dependency fallback (same targets).

---

## Quality Checks

| Task | Command |
|------|---------|
| Full QA (format → lint → typecheck → test) | `just qa` or `make qa` |
| Format only | `uv run ruff format .` |
| Lint only | `uv run ruff check . --fix` |
| Type check | `uv run --all-extras ty check .` |
| Tests | `uv run pytest` |
| Build distribution | `uv build` |
| Validate distribution | `uvx twine check dist/*` |

CI runs the full pipeline on every pull request and push to `main`, plus a wheel build, twine check, and smoke-test install — so **`pre-commit run --all-files` passing locally is a good proxy for CI**.

---

## Pull Request Guidelines

- Use a **Conventional Commit** message (the squash-merge commit becomes the changelog entry).
- Include tests for new or changed behavior.
- Keep the diff focused on a single concern.
- Ensure `pre-commit run --all-files` passes.
- Update docstrings and docs where relevant.

---

## Release Process (Release Please)

Releases are **fully automated** — there is no manual version bumping or tagging.

1. **Conventional commits** land on `main` via pull requests.
2. **Release Please** maintains a running **"release PR"** that accumulates changes, bumps the version in `pyproject.toml`, and updates `CHANGELOG.md`.
3. **Merge the release PR** when you're ready to ship. Release Please then:
   - creates a Git tag (`vX.Y.Z`),
   - creates a **GitHub Release** with the generated changelog.
4. The GitHub Release (`published` event) **triggers `publish.yml`**, which:
   - builds the distribution and validates it (`twine check`),
   - **auto-publishes to [TestPyPI](https://test.pypi.org/project/datasluice/)** (`test-pypi` environment),
   - **pauses** — the `pypi` environment has required reviewers, so a maintainer must **approve** the deployment,
   - on approval, **publishes to [PyPI](https://pypi.org/project/datasluice/)**.

> **No manual tagging.** The version source of truth is `pyproject.toml`, bumped by Release Please. The `.release-please-manifest.json` file tracks the last released version.

---

## Code of Conduct

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating you agree to abide by its terms.
