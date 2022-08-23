import string
from datetime import date, datetime, time
from typing import List, Optional

import pandas as pd
import pytest
from google.api_core.exceptions import NotFound
from google.cloud.bigquery.schema import SchemaField
from sqlalchemy import ARRAY, TIMESTAMP, Column, Integer, String, select
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.selectable import CTE
from sqlalchemy_bigquery import ARRAY, STRUCT
from sqlalchemy_bigquery.base import BQArray

from amora.compilation import compile_statement
from amora.config import settings
from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig
from amora.protocols import Compilable
from amora.providers.bigquery import (
    DryRunResult,
    array,
    column_for_schema_field,
    cte_from_rows,
    dry_run,
    estimated_query_cost_in_usd,
    estimated_storage_cost_in_usd,
    get_fully_qualified_id,
    run,
    sample,
    schema_for_model,
    schema_for_model_source,
    struct_for_model,
    zip_arrays,
)

from tests.models.health import Health
from tests.models.heart_rate import HeartRate
from tests.models.heart_rate_over_100 import HeartRateOver100
from tests.models.step_count_by_source import StepCountBySource


def test_cte_from_rows_with_single_row():
    cte = cte_from_rows([{"x": 666, "y": 666}])

    assert isinstance(cte, CTE)
    assert cte.c.keys() == ["x", "y"]
    assert compile_statement(cte)
    assert run(cte)


def test_cte_from_rows_with_multiple_rows():
    cte = cte_from_rows([{"x": n, "y": n} for n in range(5)])

    assert isinstance(cte, CTE)
    assert cte.c.keys() == ["x", "y"]
    assert compile_statement(cte)
    assert run(cte)


def test_cte_from_rows_with_distinguished_schema_rows():
    cte = cte_from_rows([{"x": 1, "y": 1}, {"x": 2, "y": 2, "z": 2}])
    assert isinstance(cte, CTE)

    with pytest.raises(CompileError):
        compile_statement(cte)


def test_cte_from_rows_with_repeated_fields():
    cte = cte_from_rows([{"x": array(range(10))}, {"x": array(range(10, 20))}])

    assert isinstance(cte, CTE)
    assert compile_statement(cte)
    assert run(cte)


def test_cte_from_rows_with_struct_fields():
    class Node(AmoraModel):
        id: str

    cte = cte_from_rows(
        [{"node": Node(id="a")}, {"node": Node(id="b")}, {"node": Node(id="c")}]
    )

    assert isinstance(cte, CTE)
    assert compile_statement(cte)
    assert run(cte)


@pytest.mark.skip(
    "Compilation incorrect. Described on issue: https://github.com/mundipagg/amora-data-build-tool/issues/155"
)
def test_cte_from_rows_with_record_repeated_fields():
    class Node(AmoraModel):
        id: str

    class Edge(AmoraModel):
        from_node: Node
        to_node: Node

    cte = cte_from_rows(
        [
            {
                "id": 1,
                "nodes": array([Node(id="a"), Node(id="b"), Node(id="c")]),
                "edges": array(
                    [
                        Edge(from_node=Node(id="a"), to_node=Node(id="b")),
                        Edge(from_node=Node(id="b"), to_node=Node(id="c")),
                    ]
                ),
            },
        ]
    )

    assert isinstance(cte, CTE)
    assert compile_statement(cte)
    assert run(cte)


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


def test_dry_run_on_unmaterialized_model():
    class ModelA(AmoraModel, table=True):
        __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

        id: int = Field(primary_key=True)

    class ModelB(AmoraModel, table=True):
        __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

        id: int = Field(primary_key=True)

        @classmethod
        def source(cls):
            return select(ModelA.id)

    assert dry_run(ModelB) is None


def test_dry_run_on_model_with_source():
    result = dry_run(HeartRate)

    assert isinstance(result, DryRunResult)
    assert result.referenced_tables == [
        get_fully_qualified_id(dep) for dep in HeartRate.__depends_on__
    ]


def test_schema_for_model():
    class Node(AmoraModel):
        id: int
        label: str

    class ModelB(AmoraModel, table=True):
        a_boolean: bool
        a_date: date
        a_datetime: datetime
        a_float: float
        a_string: str
        a_time: time
        a_timestamp: datetime = Field(sa_column=Column(TIMESTAMP))
        an_int: int = Field(primary_key=True)
        an_int_array: List[int] = Field(sa_column=Column(ARRAY(Integer)))
        a_str_array: List[str] = Field(sa_column=Column(ARRAY(String)))
        a_struct_array: List[dict] = Field(
            sa_column=Column(ARRAY(STRUCT(key=Integer, value=String)))
        )
        a_struct: Node = Field(sa_column=Column(struct_for_model(Node)))

    schema = schema_for_model(ModelB)

    assert schema == [
        SchemaField(name="a_timestamp", field_type="TIMESTAMP", mode="NULLABLE"),
        SchemaField(name="an_int_array", field_type="INTEGER", mode="REPEATED"),
        SchemaField(name="a_str_array", field_type="STRING", mode="REPEATED"),
        SchemaField(
            name="a_struct_array",
            field_type="RECORD",
            mode="REPEATED",
            fields=(
                SchemaField("key", "INTEGER"),
                SchemaField("value", "STRING"),
            ),
        ),
        SchemaField(
            name="a_struct",
            field_type="RECORD",
            fields=(
                SchemaField("id", "INTEGER"),
                SchemaField("label", "STRING"),
            ),
        ),
        SchemaField(name="a_boolean", field_type="BOOLEAN", mode="NULLABLE"),
        SchemaField(name="a_date", field_type="DATE", mode="NULLABLE"),
        SchemaField(name="a_datetime", field_type="DATETIME", mode="NULLABLE"),
        SchemaField(name="a_float", field_type="FLOAT", mode="NULLABLE"),
        SchemaField(name="a_string", field_type="STRING", mode="NULLABLE"),
        SchemaField(name="a_time", field_type="TIME", mode="NULLABLE"),
        SchemaField(name="an_int", field_type="INTEGER", mode="NULLABLE"),
    ]


def test_schema_for_source():
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

    schema = schema_for_model_source(Model)

    assert schema == [
        SchemaField(name="a_boolean", field_type="BOOLEAN"),
        SchemaField(name="a_float", field_type="FLOAT"),
        SchemaField(name="a_string", field_type="STRING"),
    ]


def test_schema_for_source_on_sourceless_model():
    class Model(AmoraModel, table=True):
        a_boolean: bool
        a_float: float
        a_string: str = Field(primary_key=True)

    assert schema_for_model_source(Model) is None


def test_column_for_schema_field_on_struct_field():
    struct_field = SchemaField(
        name="point",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField("id", "STRING", "NULLABLE", None, (), None),
            SchemaField("x", "INTEGER", "NULLABLE", None, (), None),
            SchemaField("y", "INTEGER", "NULLABLE", None, (), None),
        ),
    )
    column = column_for_schema_field(struct_field)
    assert column.type.get_col_spec() == "STRUCT<id STRING, x INT64, y INT64>"


def test_columns_for_schema_field_on_repeated_struct_field():
    repeated_struct_field = SchemaField(
        name="points",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField("id", "STRING", "NULLABLE", None, (), None),
            SchemaField("x", "INTEGER", "NULLABLE", None, (), None),
            SchemaField("y", "INTEGER", "NULLABLE", None, (), None),
        ),
    )
    column = column_for_schema_field(repeated_struct_field)
    assert repr(column.type) == "ARRAY(STRUCT(id=String(), x=Integer(), y=Integer()))"


def test_columns_for_schema_field_on_repeated_struct_field_with_repeated_fields():
    complex_struct_field = SchemaField(
        name="graph",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField(
                name="nodes",
                field_type="RECORD",
                mode="REPEATED",
                fields=(
                    SchemaField(name="id", field_type="STRING"),
                    SchemaField(name="label", field_type="STRING"),
                ),
            ),
            SchemaField(
                name="edges",
                field_type="RECORD",
                mode="REPEATED",
                fields=(
                    SchemaField(
                        name="from_node",
                        field_type="RECORD",
                        fields=(
                            SchemaField(name="id", field_type="STRING"),
                            SchemaField(name="label", field_type="STRING"),
                        ),
                    ),
                    SchemaField(
                        name="to_node",
                        field_type="RECORD",
                        fields=(
                            SchemaField(name="id", field_type="STRING"),
                            SchemaField(name="label", field_type="STRING"),
                        ),
                    ),
                ),
            ),
        ),
    )
    column = column_for_schema_field(complex_struct_field)
    assert (
        repr(column.type)
        == "ARRAY(STRUCT(nodes=ARRAY(STRUCT(id=String(), label=String())), edges=ARRAY(STRUCT(from_node=STRUCT(id=String(), label=String()), to_node=STRUCT(id=String(), label=String())))))"
    )


def test_simple_struct():
    class Point(AmoraModel):
        x: float
        y: float

    point_struct = struct_for_model(Point)

    assert isinstance(point_struct, STRUCT)
    assert point_struct.get_col_spec() == "STRUCT<x FLOAT64, y FLOAT64>"


def test_nested_struct():
    class Node(AmoraModel):
        id: str
        label: str

    class Edge(AmoraModel):
        from_node: Node
        to_node: Node

    edge_struct = struct_for_model(Edge)

    assert isinstance(edge_struct, STRUCT)
    assert (
        edge_struct.get_col_spec()
        == "STRUCT<from_node STRUCT<id STRING, label STRING>, to_node STRUCT<id STRING, label STRING>>"
    )

    class Graph(AmoraModel):
        nodes: List[Node]
        edges: List[Edge]

    graph_struct = struct_for_model(Graph)

    assert isinstance(graph_struct, STRUCT)
    assert (
        graph_struct.get_col_spec()
        == "STRUCT<nodes ARRAY<STRUCT<id STRING, label STRING>>, edges ARRAY<STRUCT<from_node STRUCT<id STRING, label STRING>, to_node STRUCT<id STRING, label STRING>>>>"
    )


def test_nested_repeated_struct():
    class Point(AmoraModel):
        x: float
        y: float

    class Line(AmoraModel):
        points: List[Point] = Field(sa_column=ARRAY(struct_for_model(Point)))
        tags: List[str] = Field(sa_column=ARRAY(String))

    s = struct_for_model(Line)

    assert isinstance(s, STRUCT)
    assert (
        s.get_col_spec()
        == "STRUCT<points ARRAY<STRUCT<x FLOAT64, y FLOAT64>>, tags ARRAY<STRING>>"
    )


def test_array_of_integers():
    zero_to_nine = array(range(10))

    assert isinstance(zero_to_nine.type, BQArray)
    assert isinstance(zero_to_nine.type.item_type, Integer)

    assert compile_statement(zero_to_nine)


def test_array_of_strings():
    ascii_lowercase = array(string.ascii_lowercase)

    assert isinstance(ascii_lowercase.type, BQArray)
    assert isinstance(ascii_lowercase.type.item_type, String)

    assert compile_statement(ascii_lowercase)


def test_array_raises_an_error_if_input_contains_null_values():
    """
    BigQuery raises an error if the query result has an ARRAY which contains NULL elements,
    although such an ARRAY can be used inside the query.

    Read more: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_type
    """

    with pytest.raises(ValueError):
        array([1, None, 3])


@pytest.mark.skip("I don't know how to implement this")
def test_array_raises_an_error_if_input_contains_multiple_types():
    """
    Array elements should have a common supertype

    Read more: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#declaring_an_array_type
    """
    with pytest.raises(ValueError):
        array([1, "2", 3])

    common_supertype_array = array([1, 2.0, 3])
    assert isinstance(common_supertype_array, BQArray)


def test_zip_arrays():
    cte = cte_from_rows(
        [
            {
                "entity": array([1, 2]),
                "f1": array(["f1v1", "f1v2"]),
                "f2": array(["f2v1", "f2v2"]),
                "event_timestamp": datetime.fromisoformat("2021-01-01T01:01:01"),
            },
            {
                "entity": array([2, 3]),
                "f1": array(["f1v3", "f1v4"]),
                "f2": array(["f2v3", "f2v4"]),
                "event_timestamp": datetime.fromisoformat("2022-02-02T02:02:02"),
            },
        ]
    )

    result = run(
        statement=zip_arrays(
            cte.c.entity, cte.c.f1, cte.c.f2, additional_columns=[cte.c.event_timestamp]
        )
    )

    assert [dict(row) for row in result.rows] == [
        {
            "entity": 1,
            "f1": "f1v1",
            "f2": "f2v1",
            "event_timestamp": datetime.fromisoformat("2021-01-01T01:01:01"),
        },
        {
            "entity": 2,
            "f1": "f1v2",
            "f2": "f2v2",
            "event_timestamp": datetime.fromisoformat("2021-01-01T01:01:01"),
        },
        {
            "entity": 2,
            "f1": "f1v3",
            "f2": "f2v3",
            "event_timestamp": datetime.fromisoformat("2022-02-02T02:02:02"),
        },
        {
            "entity": 3,
            "f1": "f1v4",
            "f2": "f2v4",
            "event_timestamp": datetime.fromisoformat("2022-02-02T02:02:02"),
        },
    ]


def test_table_sample():
    sample_df = sample(StepCountBySource)
    assert isinstance(sample_df, pd.DataFrame)
    assert not sample_df.empty
    assert set(sample_df.columns) == {
        c.key for c in StepCountBySource.__table__.columns
    }
