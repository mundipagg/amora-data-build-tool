from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    Table,
    TimePartitioning,
    TimePartitioningType,
)

from amora.config import settings
from amora.dag import DependencyDAG
from amora.materialization import Task, materialize
from amora.models import (
    AmoraModel,
    Field,
    Label,
    MaterializationTypes,
    ModelConfig,
    PartitionConfig,
)
from amora.providers.bigquery import Client, QueryJobConfig, Table
from amora.utils import clean_compiled_files

from tests.models.heart_agg import HeartRateAgg
from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps


def setup_function(module):
    clean_compiled_files()


def test_it_creates_a_task_from_a_target_file_path():
    target_path = HeartRate.target_path(model_file_path=HeartRate.model_file_path())
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
        topologicaly_sorted_tasks[0]
        == f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.health"
    )
    assert (
        topologicaly_sorted_tasks[-1]
        == f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.heart_rate_agg"
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
            labels={Label("freshness", "daily")},
        )

    result = materialize(sql="SELECT 1", model=ViewModel)

    client = Client.return_value

    client.delete_table.assert_called_once_with(
        ViewModel.unique_name(), not_found_ok=True
    )

    assert client.create_table.call_count == 1
    assert client.create_table.return_value == result

    view: Table = client.create_table.call_args_list[0][0][0]
    assert view.description == ViewModel.__model_config__.description
    assert view.labels == {"freshness": "daily"}


@patch("amora.materialization.Client", spec=Client)
@patch("amora.materialization.QueryJobConfig", spec=QueryJobConfig)
def test_materialize_as_table_with_clustering_and_time_partitioning(
    QueryJobConfig: MagicMock, Client: MagicMock
):
    table_name = uuid4().hex

    query_job_config = MagicMock()
    QueryJobConfig.return_value = query_job_config

    class TableModel(AmoraModel, table=True):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.table,
            partition_by=PartitionConfig(
                field="created_at", data_type="TIMESTAMP", granularity="day"
            ),
            cluster_by=["x", "y"],
            labels={Label("freshness", "daily")},
            description=uuid4().hex,
        )

        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)
        created_at: datetime = Field(primary_key=True)

    materialize(sql="SELECT 1", model=TableModel)

    client = Client.return_value

    client.get_table.assert_called_once_with(TableModel.unique_name())

    QueryJobConfig.assert_called_once_with(
        destination=TableModel.unique_name(),
        write_disposition="WRITE_TRUNCATE",
    )

    assert query_job_config.time_partitioning == TimePartitioning(
        field=TableModel.__model_config__.partition_by.field,
        type_=TimePartitioningType.DAY,
    )

    assert query_job_config.clustering_fields == TableModel.__model_config__.cluster_by

    client.query.assert_called_once_with("SELECT 1", job_config=query_job_config)

    table: Table = client.get_table.return_value
    client.delete_table.assert_called_once_with(
        TableModel.unique_name(), not_found_ok=True
    )
    client.update_table.assert_called_once_with(
        table,
        ["description", "labels"],
    )

    assert table.description == TableModel.__model_config__.description
    assert table.labels == {"freshness": "daily"}


@patch("amora.materialization.Client", spec=Client)
@patch("amora.materialization.QueryJobConfig", spec=QueryJobConfig)
def test_materialize_as_table_without_clustering_configuration(
    QueryJobConfig: MagicMock, Client: MagicMock
):
    table_name = uuid4().hex

    query_job_config = MagicMock()
    QueryJobConfig.return_value = query_job_config

    class TableModel(AmoraModel, table=True):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.table,
            partition_by=PartitionConfig(
                field="created_at", data_type="TIMESTAMP", granularity="day"
            ),
            labels={Label("freshness", "daily")},
            description=uuid4().hex,
        )

        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)
        created_at: datetime = Field(primary_key=True)

    materialize(sql="SELECT 1", model=TableModel)

    client = Client.return_value

    client.get_table.assert_called_once_with(TableModel.unique_name())

    QueryJobConfig.assert_called_once_with(
        destination=TableModel.unique_name(),
        write_disposition="WRITE_TRUNCATE",
    )

    assert query_job_config.time_partitioning == TimePartitioning(
        field=TableModel.__model_config__.partition_by.field,
        type_=TimePartitioningType.DAY,
    )

    assert query_job_config.clustering_fields == None

    table: Table = client.get_table.return_value
    client.update_table.assert_called_once_with(
        table,
        ["description", "labels"],
    )

    assert table.description == TableModel.__model_config__.description
    assert table.labels == {"freshness": "daily"}


@patch("amora.materialization.Client", spec=Client)
@patch("amora.materialization.QueryJobConfig", spec=QueryJobConfig)
def test_materialize_as_table_without_partitioning_configuration():
    table_name = uuid4().hex

    query_job_config = MagicMock()
    QueryJobConfig.return_value = query_job_config

    class TableModel(AmoraModel, table=True):
        __tablename__ = table_name
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.table,
            labels={Label("freshness", "daily")},
            description=uuid4().hex,
        )

        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)
        created_at: datetime = Field(primary_key=True)

    materialize(sql="SELECT 1", model=TableModel)

    client = Client.return_value

    client.get_table.assert_called_once_with(TableModel.unique_name())

    QueryJobConfig.assert_called_once_with(
        destination=TableModel.unique_name(),
        write_disposition="WRITE_TRUNCATE",
    )

    assert query_job_config.time_partitioning is None

    assert query_job_config.clustering_fields == TableModel.__model_config__.cluster_by


@patch("amora.materialization.Client", spec=Client)
def test_materialize_as_ephemeral(Client: MagicMock):
    table_name = uuid4().hex

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
