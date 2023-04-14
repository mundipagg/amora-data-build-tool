from datetime import datetime, timedelta

from amora.airflow.dags import default


def test_default_dag():
    dag = default(
        dag_id="amora",
        start_date=datetime.today(),
        schedule_interval=timedelta(hours=1),
    )
    tasks = dag.topological_sort()

    assert dag.dag_id == "amora"
    assert dag.schedule_interval == timedelta(hours=1)
    assert [t.task_id for t in tasks] == [
        "dag_root",
        "compile",
        "materialize",
        "models__list",
        "test",
    ]


def test_default_dag_with_dash():
    dag = default(
        dag_id="amora",
        start_date=datetime.today(),
        schedule_interval=timedelta(hours=1),
        with_dash=True,
    )
    tasks = dag.topological_sort()

    assert dag.dag_id == "amora"
    assert dag.schedule_interval == timedelta(hours=1)
    assert [t.task_id for t in tasks] == [
        "dag_root",
        "compile",
        "materialize",
        "models__list",
        "test",
        "dash__inspect",
    ]


def test_default_dag_with_feature_store():
    dag = default(
        dag_id="amora",
        start_date=datetime.today(),
        schedule_interval=timedelta(hours=1),
        with_feature_store=True,
    )
    tasks = dag.topological_sort()

    assert dag.dag_id == "amora"
    assert dag.schedule_interval == timedelta(hours=1)
    assert [t.task_id for t in tasks] == [
        "dag_root",
        "compile",
        "materialize",
        "models__list",
        "test",
        "feature_store__plan",
        "feature_store__apply",
        "feature_store__materialize",
    ]