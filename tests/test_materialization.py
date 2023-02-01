from datetime import datetime
from unittest.mock import ANY, MagicMock, call, patch
from uuid import uuid4

import pytest
from google.cloud.bigquery import Client
from sqlalchemy import TIMESTAMP, DateTime, Integer

from amora.config import settings
from amora.dag import DependencyDAG
from amora.materialization import Task, materialize
from amora.models import (AmoraModel, Field, Label, MaterializationTypes,
                          ModelConfig, PartitionConfig)
from amora.providers.bigquery import schema_for_model
from amora.utils import clean_compiled_files
from tests.models.heart_agg import HeartRateAgg
from tests.models.heart_rate import HeartRate
from tests.models.steps import Steps


class ViewModel(AmoraModel):
    x: int = Field(Integer, primary_key=True)
    y: int = Field(Integer, primary_key=True)

    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        partition_by=PartitionConfig(
            field="creationDate", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by=["sourceName"],
        labels={Label("freshness", "daily")},
    )


class TableModelByDay(AmoraModel):
    __tablename__override__ = uuid4().hex
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="created_at", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by=["x", "y"],
        labels={Label("freshness", "daily")},
        description=uuid4().hex,
    )

    x: int = Field(Integer, primary_key=True)
    y: int = Field(Integer, primary_key=True)
    created_at: datetime = Field(DateTime, primary_key=True)


class TableModelByrange(AmoraModel):
    __tablename__override__ = uuid4().hex
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="x",
            data_type="int",
            range={
                "start": 1,
                "end": 10,
            },
        ),
        cluster_by=["x", "y"],
        labels={Label("freshness", "daily")},
        description=uuid4().hex,
    )

    x: int = Field(Integer, primary_key=True)
    y: int = Field(Integer, primary_key=True)
    created_at: datetime = Field(DateTime, primary_key=True)


def setup_function(module):
    clean_compiled_files()


def test_it_creates_a_task_from_a_target_file_path():
    target_path = HeartRate.target_path()
    target_path.write_text("SELECT 1")
    task = Task.for_target(target_path)

    assert isinstance(task, Task)
    assert task.sql_stmt == "SELECT 1"
    assert task.target_file_path == target_path
    assert task.model.__table__ == HeartRate.__table__


def test_dependency_dags_is_iterable_and_topologicaly_sorted():
    tasks = []

    for model in [HeartRateAgg, Steps, HeartRate]:
        target_path = model.target_path()
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
def test_materialize_deletes_table(Client: MagicMock):
    client = Client.return_value
    materialize(
        sql="SELECT 1",
        model_name=TableModelByDay.unique_name(),
        config=TableModelByDay.__model_config__,
    )

    assert client.delete_table.call_count == 1
    assert client.delete_table.call_args_list == [
        call(TableModelByDay.unique_name(), not_found_ok=True)
    ]


@patch("amora.materialization.Client", spec=Client)
def test_materialize_deletes_view(Client: MagicMock):
    client = Client.return_value
    materialize(
        sql="SELECT 1",
        model_name=ViewModel.unique_name(),
        config=ViewModel.__model_config__,
    )

    assert client.delete_table.call_count == 1
    assert client.delete_table.call_args_list == [
        call(ViewModel.unique_name(), not_found_ok=True)
    ]


@patch("amora.materialization.Client", spec=Client)
def test_materialize_creates_view(Client: MagicMock):
    client = Client.return_value

    materialize(
        sql="SELECT 1",
        model_name=ViewModel.unique_name(),
        config=ViewModel.__model_config__,
    )
    assert client.create_table.call_count == 1

    view = client.create_table.call_args.args[0]
    assert view.view_query == "SELECT 1"
    assert view.description == ViewModel.__model_config__.description
    assert view.labels == {"freshness": "daily"}


@patch("amora.materialization.Client", spec=Client)
def test_materialize_creates_table(Client: MagicMock):
    client = Client.return_value

    materialize(
        sql="SELECT 1",
        model_name=TableModelByDay.unique_name(),
        config=TableModelByDay.__model_config__,
    )

    assert client.query.call_count == 1
    assert client.query.call_args_list == [call("SELECT 1", job_config=ANY)]


@patch("amora.materialization.Client", spec=Client)
def test_materialize_partition_table_by_range(Client: MagicMock):
    client = Client.return_value

    materialize(
        sql="SELECT 1",
        model_name=TableModelByrange.unique_name(),
        config=TableModelByrange.__model_config__,
    )

    table = client.create_table.call_args.args[0]
    partition_config = table.range_partitioning

    assert partition_config.field == "x"
    assert partition_config.range_.start == 1
    assert partition_config.range_.end == 10


@patch("amora.materialization.Client", spec=Client)
def test_materialize_partition_table_by_time(Client: MagicMock):
    client = Client.return_value

    materialize(
        sql="SELECT 1",
        model_name=TableModelByDay.unique_name(),
        config=TableModelByDay.__model_config__,
    )

    table = client.create_table.call_args.args[0]
    partition_config = table.time_partitioning

    assert partition_config.field == "created_at"
    assert partition_config.type_ == "DAY"


@patch("amora.materialization.Client", spec=Client)
def test_materialize_cluster_table(Client: MagicMock):
    client = Client.return_value

    materialize(
        sql="SELECT 1",
        model_name=TableModelByDay.unique_name(),
        config=TableModelByDay.__model_config__,
    )

    table = client.create_table.call_args.args[0]
    clustering_fields = table.clustering_fields

    assert clustering_fields == ["x", "y"]


@patch("amora.materialization.Client", spec=Client)
def test_materialize_update_table_metadata(Client: MagicMock):
    client = Client.return_value

    materialize(
        sql="SELECT 1",
        model_name=TableModelByDay.unique_name(),
        config=TableModelByDay.__model_config__,
    )

    assert client.create_table.call_count == 1
    table = client.create_table.call_args.args[0]

    assert table.description == TableModelByDay.__model_config__.description
    assert table.labels == TableModelByDay.__model_config__.labels_dict
    assert table.schema == schema_for_model(TableModelByDay)


def test_materialize_invalid_materialization():
    class InvalidModel(AmoraModel):
        x: int = Field(primary_key=True)
        y: int = Field(primary_key=True)

        __model_config__ = ModelConfig(materialized="invalid")  # type:ignore

    with pytest.raises(ValueError):
        materialize(
            sql="SELECT 1",
            model_name=InvalidModel.unique_name(),
            config=InvalidModel.__model_config__,
        )


@patch("amora.materialization.Client", spec=Client)
def test_materialize_as_ephemeral(Client: MagicMock):
    class EphemeralModel(AmoraModel):
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.ephemeral,
        )

        x: int = Field(Integer, primary_key=True)
        y: int = Field(Integer, primary_key=True)
        created_at: datetime = Field(TIMESTAMP, primary_key=True)

    assert (
        materialize(
            sql="SELECT 1",
            model_name=EphemeralModel.unique_name(),
            config=EphemeralModel.__model_config__,
        )
        is None
    )
    assert not Client.called
