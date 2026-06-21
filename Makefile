.PHONY: help install build test test-all coverage qa type-check format lint clean clean-build clean-pyc clean-test docs-serve docs-build release publish pre-commit pre-commit-install

help:
	@echo "DataSluice - available make targets:"
	@grep -E '^[a-zA-Z_-]+:.*?' Makefile | sed 's/\.PHONY://' | awk 'BEGIN {FS = ":.*?"}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' | sed 's/\\$$//' | sed 's/#//'

install:
	uv sync

pre-commit-install:
	uv run pre-commit install

pre-commit:
	uv run pre-commit run --all-files

build:
	rm -rf build dist
	uv build

test:
	uv run pytest

test-all:
	uv run --python=3.12 pytest
	uv run --python=3.13 pytest
	uv run --python=3.14 pytest

coverage:
	uv run --python=3.12 coverage run -m pytest
	uv run --python=3.13 coverage run -m pytest
	uv run --python=3.14 coverage run -m pytest
	uv run --python=3.14 coverage combine
	uv run --python=3.14 coverage report
	uv run --python=3.14 coverage html

format:
	uv run ruff format .

lint:
	uv run ruff check . --fix
	uv run ruff check --select I --fix .

type-check:
	uv run ty check --output-format=concise .

qa: format lint type-check test

docs-serve:
	-lsof -ti :8000 | xargs kill
	uv run --group docs zensical serve

docs-build:
	uv run --group docs zensical build --clean

release:
	uv run scripts/release.py

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/ dist/ .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -f .coverage .coverage.*
	rm -fr htmlcov/ .pytest_cache

publish:
	uv build
	uv publish
