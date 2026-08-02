"""Provider smoke convention module.

Imports the provider public namespace, validates metadata, and executes the
sample DAG import against the installed candidate. Run from the provider
package root: ``python tests/smoke.py``.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> int:
    from airflow.providers.datasluice.get_provider_info import get_provider_info
    from airflow.providers.datasluice.hooks.datasluice import DataSluiceHook
    from airflow.providers.datasluice.operators.materialize import DataSluiceMaterializeOperator
    from airflow.providers.datasluice.operators.search import DataSluiceSearchOperator

    info = get_provider_info()
    assert info["package-name"] == "apache-airflow-providers-datasluice", info

    assert DataSluiceHook is not None
    assert DataSluiceMaterializeOperator is not None
    assert DataSluiceSearchOperator is not None

    dag_path = Path(__file__).resolve().parent / "dags" / "example_datasluice.py"
    runpy.run_path(str(dag_path), run_name="__smoke__")
    return 0


if __name__ == "__main__":
    sys.exit(main())
