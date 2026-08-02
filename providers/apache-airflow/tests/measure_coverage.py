"""Measure provider branch coverage in the candidate venv, isolated to datasluice.

Builds on the candidate ``run_candidate.py`` harness: the provider wheel
installs ``airflow.providers.datasluice`` under site-packages, so coverage is
scoped to that installed package at runtime. Running pytest under an in-process
``Coverage`` here avoids the module-replacement breakage that a dotted
``--source`` name triggers in the Airflow provider import path. The reported
total, ``precision=2`` rounding, and the ``fail-under`` exit code are taken
straight from coverage 7.14+ (D-25/QUAL-11).
"""

from __future__ import annotations

import contextlib
import io
import os
import sys

FAIL_UNDER = 80.00


def _command(argv: list[str]) -> list[str]:
    command: list[str] = []
    after_dd = False
    for token in argv:
        if token == "--":
            after_dd = True
            continue
        if after_dd:
            command.append(token)
    if command[:2] == ["-m", "pytest"]:
        command = command[2:]
    return command


def _provider_dir() -> str:
    import importlib.util

    spec = importlib.util.find_spec("airflow.providers.datasluice")
    if spec is None or spec.submodule_search_locations is None:
        raise RuntimeError("airflow.providers.datasluice is not installed in this environment")
    return next(iter(spec.submodule_search_locations))


def _reported_total(cov: object) -> float:
    from coverage import Coverage

    cov_obj: Coverage = cov
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        cov_obj.report(show_missing=False, skip_covered=False)
    output = buf.getvalue()
    total_line = next((line for line in output.splitlines() if line.strip().startswith("TOTAL")), "")
    if not total_line:
        return 0.0
    return float(total_line.split()[-1].rstrip("%"))


def main(argv: list[str] | None = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]
    command = _command(argv)

    provider_dir = _provider_dir()
    data_file = os.path.join(os.getcwd(), ".coverage.provider")

    from coverage import Coverage

    cov = Coverage(config_file=False, branch=True, source=[provider_dir], data_file=data_file)
    cov.set_option("report:precision", 2)

    cov.start()
    exit_code = 0
    try:
        exit_code = _run_pytest(command)
    finally:
        cov.stop()
        cov.save()

    if exit_code:
        return exit_code

    total = _reported_total(cov)
    print(f"Provider branch coverage total: {total:.2f}% (threshold {FAIL_UNDER:.2f}%)")
    return 0 if total >= FAIL_UNDER else 2


def _run_pytest(command: list[str]) -> int:
    if not command:
        return 0
    import pytest

    return int(pytest.main(command))


if __name__ == "__main__":
    sys.exit(main())
