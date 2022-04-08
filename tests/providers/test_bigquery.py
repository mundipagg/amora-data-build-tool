from datetime import datetime, date, time
from typing import Optional

import pytest
from google.api_core.exceptions import NotFound
from google.cloud.bigquery.schema import SchemaField
from sqlalchemy import TIMESTAMP, Column
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.selectable import CTE

from amora.compilation import compile_statement
from amora.models import AmoraModel, Field
from amora.providers.bigquery import (
    cte_from_rows,
    estimated_query_cost_in_usd,
    estimated_storage_cost_in_usd,
    dry_run,
    DryRunResult,
    get_fully_qualified_id,
    get_schema_for_model,
    get_schema_for_source,
)
from amora.config import settings
from amora.types import Compilable
from tests.models.health import Health
from tests.models.heart_rate import HeartRate
from tests.models.heart_rate_over_100 import HeartRateOver100


def test_cte_from_rows_with_single_row():
    cte = cte_from_rows([{"x": 666, "y": 666}])

    assert isinstance(cte, CTE)
    assert cte.c.keys() == ["x", "y"]
    assert compile_statement(cte)


def test_cte_from_rows_with_multiple_rows():
    cte = cte_from_rows([{"x": n, "y": n} for n in range(5)])

    assert isinstance(cte, CTE)
    assert cte.c.keys() == ["x", "y"]
    assert compile_statement(cte)


def test_cte_from_rows_with_distinguished_schema_rows():
    cte = cte_from_rows([{"x": 1, "y": 1}, {"x": 2, "y": 2, "z": 2}])
    assert isinstance(cte, CTE)

    with pytest.raises(CompileError):
        compile_statement(cte)


ONE_TERABYTE = 1 * 1024**4
ONE_GIGABYTE = 1 * 1024**3


@pytest.mark.parametrize(
    "total_bytes, expected_cost",
    [
        (0, 0),
        (
            ONE_TERABYTE,
            settings.GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD,
        ),
        (
            ONE_TERABYTE * 10,
            settings.GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD * 10,
        ),
        (
            ONE_TERABYTE / 2,
            settings.GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD / 2,
        ),
    ],
)
def test_estimated_query_cost_in_usd(total_bytes: int, expected_cost: float):
    assert estimated_query_cost_in_usd(total_bytes) == expected_cost


@pytest.mark.parametrize(
    "total_bytes, expected_cost",
    [
        (0, 0),
        (
            ONE_GIGABYTE,
            settings.GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD,
        ),
        (
            ONE_GIGABYTE * 10,
            settings.GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD * 10,
        ),
        (
            ONE_GIGABYTE / 2,
            settings.GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD / 2,
        ),
    ],
)
def test_estimated_storage_cost_in_usd(total_bytes: int, expected_cost: float):
    assert estimated_storage_cost_in_usd(total_bytes) == expected_cost


def test_dry_run_on_sourceless_table_model():
    result = dry_run(Health)
    assert isinstance(result, DryRunResult)
    assert result.referenced_tables == [get_fully_qualified_id(Health)]


def test_dry_run_on_sourceless_view_model():
    result = dry_run(HeartRateOver100)
    assert isinstance(result, DryRunResult)
    assert result.referenced_tables == [get_fully_qualified_id(HeartRate)]


def test_dry_run_on_invalid_model():
    class Model(AmoraModel, table=True):
        x: int
        y: int
        id: int = Field(primary_key=True)

    with pytest.raises(NotFound):
        dry_run(Model)


def test_dry_run_on_model_with_source():
    result = dry_run(HeartRate)

    assert isinstance(result, DryRunResult)
    assert result.referenced_tables == [
        get_fully_qualified_id(dep) for dep in HeartRate.__depends_on__
    ]


def test_get_schema_for_model():
    class ModelB(AmoraModel, table=True):
        a_boolean: bool
        a_date: date
        a_datetime: datetime
        a_float: float
        a_string: str
        a_time: time
        a_timestamp: datetime = Field(sa_column=Column(TIMESTAMP))
        an_int: int = Field(primary_key=True)

    schema = get_schema_for_model(ModelB)

    assert schema == [
        SchemaField(name="a_timestamp", field_type="TIMESTAMP"),
        SchemaField(name="a_boolean", field_type="BOOLEAN"),
        SchemaField(name="a_date", field_type="DATE"),
        SchemaField(name="a_datetime", field_type="DATETIME"),
        SchemaField(name="a_float", field_type="FLOAT"),
        SchemaField(name="a_string", field_type="STRING"),
        SchemaField(name="a_time", field_type="TIME"),
        SchemaField(name="an_int", field_type="INTEGER"),
    ]


def test_get_schema_for_source():
    class Model(AmoraModel, table=True):
        a_boolean: bool
        a_float: float
        a_string: str = Field(primary_key=True)

        @classmethod
        def source(cls) -> Optional[Compilable]:
            return cte_from_rows(
                [
                    {"a_boolean": True, "a_float": 7.1, "a_string": "Amora"},
                    {"a_boolean": False, "a_float": 1.7, "a_string": "Aroma"},
                ]
            )

    schema = get_schema_for_source(Model)

    assert schema == [
        SchemaField(name="a_boolean", field_type="BOOLEAN"),
        SchemaField(name="a_float", field_type="FLOAT"),
        SchemaField(name="a_string", field_type="STRING"),
    ]


def test_get_schema_for_source_on_sourceless_model():
    class Model(AmoraModel, table=True):
        a_boolean: bool
        a_float: float
        a_string: str = Field(primary_key=True)

    assert get_schema_for_source(Model) is None
