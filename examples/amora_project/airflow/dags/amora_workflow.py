from datetime import datetime
from amora.airflow.dags import default


dag = default(
    dag_id="amora-workflow",
    start_date=datetime(2023, 4, 1, 4, 20),
    schedule_interval="@hourly",
    with_dash=True,
    with_feature_store=True,
)