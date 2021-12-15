from datetime import datetime
from unittest.mock import patch, MagicMock
from uuid import uuid4

from amora.utils import clean_compiled_files
from amora.materialization import Task, DependencyDAG, materialize
from amora.models import (
    AmoraModel,
    ModelConfig,
    PartitionConfig,
    MaterializationTypes,
    Field,
)
from amora.config import settings
from amora.providers.bigquery import Client, QueryJobConfig, Table

from tests.models.heart_agg import HeartRateAgg
from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps


def setup_function(module):
    clean_compiled_files()


def test_it_creates_a_task_from_a_target_file_path():
    target_path = HeartRate.target_path(
        model_file_path=HeartRate.model_file_path()
    )
    target_path.write_text("SELECT 1")
    task = Task.for_target(target_path)

    assert isinstance(task, Task)
    assert task.sql_stmt == "SELECT 1"
    assert task.target_file_path == target_path
    assert task.model.__table__ == HeartRate.__table__


def test_dependency_dags_is_iterable_and_topologicaly_sorted():
    tasks = []

    for model in [HeartRateAgg, Steps, HeartRate]:
        target_path = model.target_path(model_file_path=model.model_file_path())
        target_path.write_text("SELECT 1")

        tasks.append(Task.for_target(target_path))

    dag = DependencyDAG.from_tasks(tasks)
    topologicaly_sorted_tasks = list(dag)

    assert (
        topologicaly_sorted_tasks[0] == "amora-test-project.testschema.health"
    )
    assert (
        topologicaly_sorted_tasks[-1]
        == "amora-test-project.testschema.heart_rate_agg"
    )


@patch("amora.materialization.Client", spec=Client)
def test_materialize_as_view(Client: MagicMock):
    class ViewModel(AmoraModel, table=True):
        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)

        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.view,
            partition_by=PartitionConfig(
                field="creationDate", data_type="TIMESTAMP", granularity="day"
            ),
            cluster_by=["sourceName"],
            labels={"freshness": "daily"},
        )

    result = materialize(sql="SELECT 1", model=ViewModel)

    client = Client.return_value
    assert client.create_table.call_count == 1
    assert client.create_table.return_value == result

    view: Table = client.create_table.call_args_list[0][0][0]
    assert view.description == ViewModel.__model_config__.description
    assert view.clustering_fields == ViewModel.__model_config__.cluster_by
    assert view.labels == ViewModel.__model_config__.labels


@patch("amora.materialization.Client", spec=Client)
@patch("amora.materialization.QueryJobConfig", spec=QueryJobConfig)
def test_materialize_as_table(QueryJobConfig: MagicMock, Client: MagicMock):
    table_name = uuid4().hex

    class TableModel(AmoraModel, table=True):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.table,
            partition_by=PartitionConfig(
                field="created_at", data_type="TIMESTAMP", granularity="day"
            ),
            cluster_by=["x", "y"],
            labels={"freshness": "daily"},
            description=uuid4().hex,
        )

        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)
        created_at: datetime = Field(primary_key=True)

    result = materialize(sql="SELECT 1", model=TableModel)

    client = Client.return_value

    client.get_table.assert_called_once_with(TableModel.unique_name)

    client.query.assert_called_once_with(
        "SELECT 1",
        job_config=QueryJobConfig(
            destination=TableModel.unique_name,
            write_disposition="WRITE_TRUNCATE",
        ),
    )

    table: Table = client.get_table.return_value
    client.update_table.assert_called_once_with(
        table,
        ["description", "labels", "clustering_fields"],
    )

    assert table.description == TableModel.__model_config__.description
    assert table.clustering_fields == TableModel.__model_config__.cluster_by
    assert table.labels == TableModel.__model_config__.labels


@patch("amora.materialization.Client", spec=Client)
def test_materialize_as_ephemeral(Client: MagicMock):
    table_name = uuid4().hex
    table_id = (
        f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.{table_name}"
    )

    class EphemeralModel(AmoraModel, table=True):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.ephemeral,
        )

        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)
        created_at: datetime = Field(primary_key=True)

    assert materialize(sql="SELECT 1", model=EphemeralModel) is None
    assert not Client.called
