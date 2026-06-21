# DataSluice

![PyPI version](https://img.shields.io/pypi/v/datasluice.svg)

One Python interface for open-data discovery, extraction, format normalization, and pipeline integration

* [GitHub](https://github.com/nitish-raj/datasluice/) | [PyPI](https://pypi.org/project/datasluice/) | [Documentation](https://nitish-raj.github.io/datasluice/)
* Created by [Nitish Raj](https://rajnitish.com/) | GitHub [@nitish-raj](https://github.com/nitish-raj) | PyPI [@nitish-raj](https://pypi.org/user/nitish-raj/)
* MIT License

## Features

* TODO

## Documentation

Documentation is built with [Zensical](https://zensical.org/) and deployed to GitHub Pages.

* **Live site:** https://nitish-raj.github.io/datasluice/
* **Preview locally:** `just docs-serve` (serves at http://localhost:8000)
* **Build:** `just docs-build`

API documentation is auto-generated from docstrings using [mkdocstrings](https://mkdocstrings.github.io/).

Docs deploy automatically on push to `main` via GitHub Actions. To enable this, go to your repo's Settings > Pages and set the source to **GitHub Actions**.

## Development

To set up for local development:

```bash
# Clone your fork
git clone git@github.com:your_username/datasluice.git
cd datasluice

# Install in editable mode with live updates
uv tool install --editable .
```

This installs the CLI globally but with live updates - any changes you make to the source code are immediately available when you run `datasluice`.

Run tests:

```bash
uv run pytest
```

Run quality checks (format, lint, type check, test):

```bash
just qa
```

## Author

DataSluice was created in 2026 by Nitish Raj.

Built with [Cookiecutter](https://github.com/cookiecutter/cookiecutter) and the [audreyfeldroy/cookiecutter-pypackage](https://github.com/audreyfeldroy/cookiecutter-pypackage) project template.
