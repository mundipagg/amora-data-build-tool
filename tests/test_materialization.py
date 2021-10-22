from datetime import datetime
from unittest.mock import patch, MagicMock
from uuid import uuid4

import pytest
from amora.compilation import clean_compiled_files
from amora.materialization import Task, DependencyDAG, materialize
from amora.models import AmoraModel, ModelConfig, PartitionConfig, MaterializationTypes
from config import settings
from google.cloud.bigquery import QueryJobConfig

from tests.models.heart_agg import HeartRateAgg
from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps


def setup_function(module):
    clean_compiled_files()


def test_it_creates_a_task_from_a_target_file_path():
    target_path = HeartRate.target_path(model_file_path=HeartRate.model_file_path())
    with open(target_path, "w") as fp:
        fp.write("SELECT 1")

    task = Task.for_target(target_path)

    assert isinstance(task, Task)
    assert task.sql_stmt == "SELECT 1"
    assert task.target_file_path == target_path
    assert task.model.__table__ == HeartRate.__table__


def test_dependency_dags_is_iterable_and_topologicaly_sorted():
    tasks = []

    for model in [HeartRateAgg, Steps, HeartRate]:
        target_path = model.target_path(model_file_path=model.model_file_path())
        with open(target_path, "w") as fp:
            fp.write("SELECT 1")
        tasks.append(Task.for_target(target_path))

    dag = DependencyDAG.from_tasks(tasks)
    topologicaly_sorted_tasks = list(dag)

    assert topologicaly_sorted_tasks[0] == "Health"
    assert topologicaly_sorted_tasks[-1] == "HeartRateAgg"


@patch("amora.materialization.Client")
def test_materialize_as_view(Client: MagicMock):
    class ViewModel(AmoraModel):
        x: int
        y: int

        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.view,
            partition_by=PartitionConfig(
                field="creationDate", data_type="TIMESTAMP", granularity="day"
            ),
            cluster_by="sourceName",
            tags=["daily"],
        )

    result = materialize(sql="SELECT 1", model=ViewModel)

    client = Client.return_value
    assert client.create_table.call_count == 1
    assert client.create_table.return_value == result


@patch("amora.materialization.Client")
@patch("amora.materialization.QueryJobConfig")
def test_materialize_as_table(QueryJobConfig: MagicMock, Client: MagicMock):
    table_name = uuid4().hex
    table_id = f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.{table_name}"

    class TableModel(AmoraModel):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.table,
            partition_by=PartitionConfig(
                field="created_at", data_type="TIMESTAMP", granularity="day"
            ),
            cluster_by=["x", "y"],
            tags=["daily"],
        )

        x: int
        y: int
        created_at: datetime

    result = materialize(sql="SELECT 1", model=TableModel)

    client = Client.return_value
    query_job = client.query.return_value

    assert query_job.result.return_value == result
    client.query.assert_called_once_with(
        "SELECT 1",
        job_config=QueryJobConfig(destination=table_id),
    )


@patch("amora.materialization.Client")
def test_materialize_as_ephemeral(Client: MagicMock):
    table_name = uuid4().hex
    table_id = f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.{table_name}"

    class EphemeralModel(AmoraModel):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.ephemeral,
        )

        x: int
        y: int
        created_at: datetime

    assert materialize(sql="SELECT 1", model=EphemeralModel) is None
    assert not Client.called
