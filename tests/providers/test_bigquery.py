import string
from datetime import date, datetime, time
from typing import List, Optional

import numpy as np
import pandas as pd
import pytest
from google.api_core.exceptions import NotFound
from google.cloud.bigquery.schema import SchemaField
from sqlalchemy import (
    ARRAY,
    TIMESTAMP,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Time,
    select,
)
from sqlalchemy.exc import CompileError
from sqlalchemy.sql.selectable import CTE
from sqlalchemy_bigquery import STRUCT
from sqlalchemy_bigquery.base import BQArray

from amora.compilation import compile_statement
from amora.config import settings
from amora.models import (
    SQLALCHEMY_METADATA_KEY,
    AmoraModel,
    Field,
    MaterializationTypes,
    ModelConfig,
)
from amora.protocols import Compilable
from amora.providers.bigquery import (
    DryRunResult,
    array,
    column_for_schema_field,
    cte_from_dataframe,
    cte_from_rows,
    dry_run,
    estimated_query_cost_in_usd,
    estimated_storage_cost_in_usd,
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
        id: str = Field(String, primary_key=True)

    node_a = Node()
    node_a.id = "a"

    node_b = Node()
    node_b.id = "b"

    node_c = Node()
    node_c.id = "c"

    cte = cte_from_rows([{"node": node_a}, {"node": node_b}, {"node": node_c}])

    assert isinstance(cte, CTE)
    assert compile_statement(cte)
    assert run(cte)


@pytest.mark.skip(
    "Compilation incorrect. Described on issue: https://github.com/mundipagg/amora-data-build-tool/issues/155"
)
def test_cte_from_rows_with_record_repeated_fields():
    class Node(AmoraModel):
        id: str = Field(String, primary_key=True)

    class Edge(AmoraModel):
        from_node: Node = Field(struct_for_model(Node), primary_key=True)
        to_node: Node = Field(struct_for_model(Node))

    node_a = Node()
    node_a.id = "a"

    node_b = Node()
    node_b.id = "b"

    node_c = Node()
    node_c.id = "c"

    edge_a_to_b = Edge()
    edge_a_to_b.from_node = node_a
    edge_a_to_b.to_node = node_b

    edge_b_to_c = Edge()
    edge_b_to_c.from_node = node_b
    edge_b_to_c.to_node = node_c

    cte = cte_from_rows(
        [
            {
                "id": 1,
                "nodes": array([node_a, node_b, node_c]),
                "edges": array(
                    [
                        edge_a_to_b,
                        edge_b_to_c,
                    ]
                ),
                "root_node": node_a,
            },
        ]
    )
    assert isinstance(cte, CTE)
    assert compile_statement(cte)
    assert run(cte)


def test_cte_from_dataframe():
    df = pd.DataFrame(
        np.random.randint(0, 1000, size=(10, 5)), columns=["A", "B", "C", "D", "E"]
    )
    cte = cte_from_dataframe(df)

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
    assert result.referenced_tables == [Health.fully_qualified_name()]


def test_dry_run_on_sourceless_view_model():
    result = dry_run(HeartRateOver100)
    assert isinstance(result, DryRunResult)
    assert result.referenced_tables == [HeartRate.fully_qualified_name()]


def test_dry_run_on_invalid_model():
    class Model(AmoraModel):
        x: int = Field(Integer)
        y: int = Field(Integer)
        id: int = Field(Integer, primary_key=True)

    with pytest.raises(NotFound):
        dry_run(Model)


def test_dry_run_on_unmaterialized_model():
    class ModelA(AmoraModel):
        __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

        id: int = Field(primary_key=True)

    class ModelB(AmoraModel):
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
        dep.fully_qualified_name() for dep in HeartRate.__depends_on__
    ]


def test_schema_for_model():
    class Node(AmoraModel):
        id: int = Field(Integer, primary_key=True)
        label: str = Field(String)

    class Foo(AmoraModel):
        a_boolean: bool = Field(
            Boolean, primary_key=True, doc="You know... ones and zeros"
        )
        a_date: date = Field(Date)
        a_datetime: datetime = Field(DateTime)
        a_float: float = Field(Float)
        a_string: str = Field(String, doc="Words and stuff")
        a_time: time = Field(Time)
        a_timestamp: datetime = Field(TIMESTAMP)
        an_int: int = Field(Integer, primary_key=True)
        an_int_array: List[int] = Field(ARRAY(Integer))
        a_str_array: List[str] = Field(ARRAY(String))
        a_struct_array: List[dict] = Field(
            ARRAY(STRUCT(*[("key", Integer), ("value", String)]))
        )
        a_struct: Node = Field(struct_for_model(Node))

    schema = schema_for_model(Foo)

    assert schema == [
        SchemaField(
            "a_boolean",
            "BOOLEAN",
            "NULLABLE",
            None,
            "You know... ones and zeros",
            (),
            None,
        ),
        SchemaField("a_date", "DATE", "NULLABLE", None, None, (), None),
        SchemaField("a_datetime", "DATETIME", "NULLABLE", None, None, (), None),
        SchemaField("a_float", "FLOAT", "NULLABLE", None, None, (), None),
        SchemaField(
            "a_string", "STRING", "NULLABLE", None, "Words and stuff", (), None
        ),
        SchemaField("a_time", "TIME", "NULLABLE", None, None, (), None),
        SchemaField("a_timestamp", "TIMESTAMP", "NULLABLE", None, None, (), None),
        SchemaField("an_int", "INTEGER", "NULLABLE", None, None, (), None),
        SchemaField("an_int_array", "INTEGER", "REPEATED", None, None, (), None),
        SchemaField("a_str_array", "STRING", "REPEATED", None, None, (), None),
        SchemaField(
            "a_struct_array",
            "RECORD",
            "REPEATED",
            None,
            None,
            (
                SchemaField("key", "INTEGER", "NULLABLE", None, None, (), None),
                SchemaField("value", "STRING", "NULLABLE", None, None, (), None),
            ),
            None,
        ),
        SchemaField(
            "a_struct",
            "RECORD",
            "NULLABLE",
            None,
            None,
            (
                SchemaField("id", "INTEGER", "NULLABLE", None, None, (), None),
                SchemaField("label", "STRING", "NULLABLE", None, None, (), None),
            ),
            None,
        ),
    ]


def test_schema_for_source():
    class Model(AmoraModel):
        a_boolean: bool = Field(Boolean)
        a_float: float = Field(Float)
        a_string: str = Field(String, primary_key=True)

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
    class Model(AmoraModel):
        a_boolean: bool = Field(Boolean)
        a_float: float = Field(Float)
        a_string: str = Field(String, primary_key=True)

    assert schema_for_model_source(Model) is None


def test_column_for_schema_field_on_struct_field():
    struct_field = SchemaField(
        name="point",
        field_type="RECORD",
        mode="NULLABLE",
        fields=(
            SchemaField(
                "id",
                "STRING",
                "NULLABLE",
                None,
                None,
                (),
            ),
            SchemaField(
                "x",
                "INTEGER",
                "NULLABLE",
                None,
                None,
                (),
            ),
            SchemaField(
                "y",
                "INTEGER",
                "NULLABLE",
                None,
                None,
                (),
            ),
        ),
    )
    column = column_for_schema_field(struct_field)

    assert (
        column.metadata[SQLALCHEMY_METADATA_KEY].type.get_col_spec()
        == "STRUCT<id STRING, x INT64, y INT64>"
    )


def test_columns_for_schema_field_on_repeated_struct_field():
    repeated_struct_field = SchemaField(
        name="points",
        field_type="RECORD",
        mode="REPEATED",
        fields=(
            SchemaField(
                "id",
                "STRING",
                "NULLABLE",
                None,
                None,
                (),
            ),
            SchemaField(
                "x",
                "INTEGER",
                "NULLABLE",
                None,
                None,
                (),
            ),
            SchemaField(
                "y",
                "INTEGER",
                "NULLABLE",
                None,
                None,
                (),
            ),
        ),
    )
    column = column_for_schema_field(repeated_struct_field)
    assert (
        repr(column.metadata[SQLALCHEMY_METADATA_KEY].type)
        == "ARRAY(STRUCT(id=String(), x=Integer(), y=Integer()))"
    )


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
        repr(column.metadata[SQLALCHEMY_METADATA_KEY].type)
        == "ARRAY(STRUCT(nodes=ARRAY(STRUCT(id=String(), label=String())), edges=ARRAY(STRUCT(from_node=STRUCT(id=String(), label=String()), to_node=STRUCT(id=String(), label=String())))))"
    )


def test_simple_struct():
    class Point(AmoraModel):
        x: float = Field(Float, primary_key=True)
        y: float = Field(Float)

    point_struct = struct_for_model(Point)

    assert isinstance(point_struct, STRUCT)
    assert point_struct.get_col_spec() == "STRUCT<x FLOAT64, y FLOAT64>"


def test_nested_struct():
    class Node(AmoraModel):
        id: str = Field(String, primary_key=True)
        label: str = Field(String)

    class Edge(AmoraModel):
        from_node: Node = Field(struct_for_model(Node), primary_key=True)
        to_node: Node = Field(struct_for_model(Node), primary_key=True)

    edge_struct = struct_for_model(Edge)

    assert isinstance(edge_struct, STRUCT)
    assert (
        edge_struct.get_col_spec()
        == "STRUCT<from_node STRUCT<id STRING, label STRING>, to_node STRUCT<id STRING, label STRING>>"
    )

    class Graph(AmoraModel):
        nodes: List[Node] = Field(ARRAY(struct_for_model(Node)), primary_key=True)
        edges: List[Edge] = Field(ARRAY(struct_for_model(Edge)))

    graph_struct = struct_for_model(Graph)

    assert isinstance(graph_struct, STRUCT)
    assert (
        graph_struct.get_col_spec()
        == "STRUCT<nodes ARRAY<STRUCT<id STRING, label STRING>>, edges ARRAY<STRUCT<from_node STRUCT<id STRING, label STRING>, to_node STRUCT<id STRING, label STRING>>>>"
    )


def test_nested_repeated_struct():
    class Point(AmoraModel):
        x: float = Field(Float, primary_key=True)
        y: float = Field(Float)

    class Line(AmoraModel):
        points: List[Point] = Field(ARRAY(struct_for_model(Point)), primary_key=True)
        tags: List[str] = Field(ARRAY(String))

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


@pytest.mark.skip("Dados sumiram")
def test_table_sample():
    sample_df = sample(StepCountBySource)
    assert isinstance(sample_df, pd.DataFrame)
    assert sample_df.empty == True
    assert set(sample_df.columns) == {
        c.key for c in StepCountBySource.__table__.columns
    }
