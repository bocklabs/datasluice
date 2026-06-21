# Apache Airflow Integration

Use DataSluice inside [Apache Airflow](https://airflow.apache.org/) DAGs.

```python
from datetime import datetime

from airflow import DAG
from datasluice.integrations.airflow import DataSluiceOperator

with DAG("opendata_sync", start_date=datetime(2026, 1, 1), schedule="@daily") as dag:
    fetch = DataSluiceOperator(
        task_id="fetch_climate_data",
        portal="https://catalog.data.gov",
        query="climate change",
        dest_dir="/tmp/climate_data",
    )
```
