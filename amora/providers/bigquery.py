import decimal
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Union

import pandas as pd
import sqlalchemy
from google.api_core.client_info import ClientInfo
from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    SchemaField,
    Table,
    TableReference,
)
from google.cloud.bigquery.table import RowIterator, _EmptyRowIterator
from pydantic.fields import SHAPE_LIST
from sqlalchemy import func, literal, literal_column, tablesample
from sqlalchemy.orm import aliased
from sqlalchemy.sql import coercions, expression, operators, roles, sqltypes
from sqlalchemy.sql.selectable import CTE
from sqlalchemy.sql.sqltypes import ARRAY
from sqlalchemy_bigquery import STRUCT
from sqlalchemy_bigquery.base import BQArray, BQBinary, unnest
from sqlmodel.main import get_sqlachemy_type
from sqlmodel.sql import sqltypes as sqlmodel_types

from amora import logger
from amora.compilation import compile_statement
from amora.config import settings
from amora.contracts import BaseResult
from amora.models import (
    AmoraModel,
    Column,
    ColumnElement,
    MaterializationTypes,
    Model,
    select,
)
from amora.storage import cache
from amora.types import Compilable
from amora.version import VERSION

Schema = List[SchemaField]
BQTable = Union[Table, TableReference, str]

BIGQUERY_TYPES_TO_PYTHON_TYPES = {
    "ARRAY": list,
    "BIGNUMERIC": decimal.Decimal,  # todo: test coverage
    "NUMERIC": decimal.Decimal,  # todo: test coverage
    "BOOL": bool,
    "BOOLEAN": bool,
    "BYTES": bytes,
    "DATE": date,
    "DATETIME": datetime,
    "FLOAT64": float,
    "FLOAT": float,
    "INT64": int,
    "INTEGER": int,
    "JSON": dict,
    "STRING": str,
    "TIME": time,
    "TIMESTAMP": datetime,
    "RECORD": dict,
    "STRUCT": dict,
}

BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES = {
    "ARRAY": BQArray,
    "BIGNUMERIC": sqltypes.Numeric,
    "BOOL": sqltypes.Boolean,
    "BOOLEAN": sqltypes.Boolean,
    "BYTES": BQBinary,
    "DATE": sqltypes.Date,
    "DATETIME": sqltypes.DateTime,
    "FLOAT": sqltypes.Float,
    "FLOAT64": sqltypes.Float,
    "INT64": sqltypes.Integer,
    "INTEGER": sqltypes.Integer,
    "JSON": sqltypes.JSON,
    "NUMERIC": sqltypes.Numeric,
    "RECORD": STRUCT,
    "STRING": sqltypes.String,
    "STRUCT": STRUCT,
    "TIME": sqltypes.Time,
    "TIMESTAMP": sqltypes.TIMESTAMP,
}


SQLALCHEMY_TYPES_TO_BIGQUERY_TYPES = {
    BQBinary: "BYTES",
    STRUCT: "RECORD",
    sqlmodel_types.AutoString: "STRING",
    sqltypes.Boolean: "BOOLEAN",
    sqltypes.Date: "DATE",
    sqltypes.DateTime: "DATETIME",
    sqltypes.Float: "FLOAT64",
    sqltypes.Integer: "INTEGER",
    sqltypes.JSON: "JSON",
    sqltypes.Numeric: "NUMERIC",
    sqltypes.String: "STRING",
    sqltypes.TIMESTAMP: "TIMESTAMP",
    sqltypes.Time: "TIME",
}


class TimePart(Enum):
    """
    Since BigQuery represents time parts as an unquoted value,
    we need to use `literal_column`

    More on: https://cloud.google.com/bigquery/docs/reference/standard-sql/time_functions#time_trunc
    """

    MICROSECOND = literal_column("MICROSECOND")
    MILLISECOND = literal_column("MILLISECOND")
    SECOND = literal_column("SECOND")
    MINUTE = literal_column("MINUTE")
    HOUR = literal_column("HOUR")


@dataclass
class DryRunResult(BaseResult):
    model: Model
    schema: Schema

    @property
    def estimated_cost(self):
        return estimated_query_cost_in_usd(self.total_bytes)


@dataclass
class RunResult(BaseResult):
    rows: Union[RowIterator, _EmptyRowIterator]
    execution_time_in_ms: int
    to_dataframe: Callable[..., pd.DataFrame]
    schema: Optional[Schema] = None

    @property
    def estimated_cost(self):
        return estimated_query_cost_in_usd(self.total_bytes)


_client = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = Client(
            client_info=ClientInfo(
                client_library_version=VERSION,
                user_agent=f"amora-data-build-tool/{VERSION}",
            )
        )
    return _client


def get_fully_qualified_id(model: Model) -> str:
    return f"{model.metadata.schema}.{model.__tablename__}"


def get_schema(table_id: str) -> Schema:
    """
    Given a `table_id`, returns the `Schema` of the table by querying BigQueries API
    """
    client = get_client()
    table = client.get_table(table_id)
    return table.schema


def column_for_schema_field(schema: SchemaField) -> Column:
    """
    Build a `Column` from a `google.cloud.bigquery.schema.SchemaField`

    Read more: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#tablefieldschema
    """
    if schema.mode == "REPEATED":
        if schema.field_type == "RECORD":
            column_type = ARRAY(struct_for_schema_field(schema))
        else:
            column_type = ARRAY(BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES[schema.field_type])
    else:
        if schema.field_type == "RECORD":
            column_type = struct_for_schema_field(schema)
        else:
            column_type = BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES[schema.field_type]

    return Column(name=schema.name, type_=column_type, comment=schema.description)


def schema_for_struct(struct: STRUCT) -> Schema:
    return [
        SchemaField(
            name=name,
            field_type=SQLALCHEMY_TYPES_TO_BIGQUERY_TYPES[sqla_type.__class__],
        )
        for name, sqla_type in struct._STRUCT_fields
    ]


def schema_field_for_column(column: Column) -> SchemaField:
    fields: Optional[SchemaField] = None

    if isinstance(column.type, ARRAY):
        mode = "REPEATED"
        item_type = column.type.item_type
        field_type = SQLALCHEMY_TYPES_TO_BIGQUERY_TYPES[column.type.item_type.__class__]

        if isinstance(item_type, STRUCT):
            fields = tuple(schema_for_struct(item_type))
    else:
        mode = "NULLABLE"
        column_type = column.type
        field_type = SQLALCHEMY_TYPES_TO_BIGQUERY_TYPES[column_type.__class__]

        if isinstance(column_type, STRUCT):
            fields = tuple(schema_for_struct(column_type))

    return SchemaField(
        name=column.name, field_type=field_type, mode=mode, fields=fields or ()
    )


def schema_for_model(model: Model) -> Schema:
    """
    Given an `AmoraModel`, returns the equivalent bigquery `Schema`
    of the model by parsing the model SQLAlchemy column schema
    """
    columns = model.__table__.columns
    return [schema_field_for_column(col) for col in columns]


def schema_for_model_source(model: Model) -> Optional[Schema]:
    """
    Give an `Amora Model`, returns the bigquery `Schema` of its
    `source` classmethod query result
    """
    source = model.source()
    if source is None:
        return None

    result = dry_run(model)
    if result is None:
        return None

    return result.schema


@logger.log_execution()
def run(statement: Compilable) -> RunResult:
    """
    Executes a given query and returns its results
    and metadata as an `amora.providers.bigquery.RunResult`
    """
    query = compile_statement(statement)
    query_job = get_client().query(query)
    rows = query_job.result()
    execution_time_delta = query_job.ended - query_job.started

    return RunResult(
        execution_time_in_ms=execution_time_delta.microseconds / 1000,
        job_id=query_job.job_id,
        query=query,
        referenced_tables=[
            ".".join(table.to_api_repr().values())
            for table in query_job.referenced_tables
        ],
        rows=rows,
        schema=query_job.schema,
        total_bytes=query_job.total_bytes_billed,
        user_email=query_job.user_email,
        to_dataframe=query_job.to_dataframe,
    )


@logger.log_execution()
def dry_run(model: Model) -> Optional[DryRunResult]:
    """
    You can use the estimate returned by the dry run to calculate query
    costs in the pricing calculator. Also useful to verify user permissions
    and query validity. You are not charged for performing the dry run.

    Read more: https://cloud.google.com/bigquery/docs/dry-run-queries

    E.g:
    ```python
    dry_run(HeartRate)
    ```

    Will result in:

    ```python
    DryRunResult(
        total_bytes_processed=170181834,
        query=\"SELECT\\n  `health`.`creationDate`,\\n  `health`.`device`,\\n  `health`.`endDate`,\\n  `health`.`id`,\\n  `health`.`sourceName`,\\n  `health`.`startDate`,\\n  `health`.`unit`,\\n  `health`.`value`\\nFROM `diogo`.`health`\\nWHERE `health`.`type` = 'HeartRate'\",
        model=HeartRate,
        referenced_tables=["amora-data-build-tool.diogo.health"],
        schema=[
            SchemaField("creationDate", "TIMESTAMP", "NULLABLE", None, (), None),
            SchemaField("device", "STRING", "NULLABLE", None, (), None),
            SchemaField("endDate", "TIMESTAMP", "NULLABLE", None, (), None),
            SchemaField("id", "INTEGER", "NULLABLE", None, (), None),
            SchemaField("sourceName", "STRING", "NULLABLE", None, (), None),
            SchemaField("startDate", "TIMESTAMP", "NULLABLE", None, (), None),
            SchemaField("unit", "STRING", "NULLABLE", None, (), None),
            SchemaField("value", "FLOAT", "NULLABLE", None, (), None),
        ],
    )
    ```
    """
    client = get_client()
    source = model.source()
    if source is None:
        table = client.get_table(get_fully_qualified_id(model))

        if table.table_type == "VIEW":
            query_job = client.query(
                query=table.view_query,
                job_config=QueryJobConfig(dry_run=True, use_query_cache=False),
            )
            return DryRunResult(
                job_id=query_job.job_id,
                model=model,
                query=table.view_query,
                referenced_tables=[
                    ".".join(table.to_api_repr().values())
                    for table in query_job.referenced_tables
                ],
                schema=query_job.schema,
                total_bytes=query_job.total_bytes_processed,
                user_email=query_job.user_email,
            )
        else:
            return DryRunResult(
                job_id=None,
                model=model,
                query=None,
                referenced_tables=[str(table.reference)],
                schema=table.schema,
                total_bytes=table.num_bytes,
                user_email=None,
            )

    query = compile_statement(source)

    query_job = client.query(
        query=query,
        job_config=QueryJobConfig(dry_run=True, use_query_cache=False),
    )
    return DryRunResult(
        job_id=query_job.job_id,
        total_bytes=query_job.total_bytes_processed,
        referenced_tables=[
            ".".join(table.to_api_repr().values())
            for table in query_job.referenced_tables
        ],
        query=query,
        model=model,
        schema=query_job.schema,
        user_email=query_job.user_email,
    )


class fixed_unnest(sqlalchemy.sql.roles.InElementRole, unnest):
    _with_offset = None

    def __init__(self, *args, **kwargs):
        self.name = "unnest"
        super().__init__(*args, **kwargs)

    def table_valued(self, *expr, with_offset: str = None, **kwargs):
        new_func = self._generate()

        if with_offset:
            expr += (with_offset,)
            new_func._with_offset = with_offset

        new_func.type = new_func._table_value_type = sqltypes.TableValueType(*expr)

        return new_func


def cte_from_rows(rows: Iterable[Dict[str, Any]]) -> CTE:
    """
    Returns a table like selectable (CTE) for the given hardcoded values.

    E.g:
    ```python
    rows = [{"numeric_column": "123"}, {"numeric_column": "234"}, {"numeric_column": "345"}]
    cte_from_rows(rows)
    ```

    Will result in the following SQL

    ```sql
        WITH `annon_1` AS (
            SELECT "123" AS numeric_column
            UNION ALL SELECT "234 AS numeric_column
            UNION ALL SELECT "345" AS numeric_column
        )
    ```

    Which would render a table like:

    ```md
    | numeric_column |
    |----------------|
    | 123            |
    | 234            |
    | 345            |
    ```

    Useful both for model writing and testing purposes. Think of `cte_from_rows` as way of generating
    a "temporary table like object", with data available at runtime.

    """

    def gen_selects(rows):
        for row in rows:
            cols = []
            for name, value in row.items():
                if isinstance(value, array):
                    cols.append(value.label(name))
                elif isinstance(value, AmoraModel):
                    cols.append(struct(value).label(name))
                else:
                    cols.append(literal(value).label(name))
            yield select(cols)

    selects = list(gen_selects(rows))

    if len(selects) == 1:
        return selects[0].cte()
    else:
        return selects[0].union_all(*(selects[1:])).cte()


def estimated_query_cost_in_usd(total_bytes: int) -> float:
    """
    By default, queries are billed using the on-demand pricing model,
    where you pay for the data scanned by your queries.

    - This function doesn't take into consideration that the first 1 TB per month is free.
    - By default, the estimation is based on BigQuery's `On-demand analysis` pricing, which may change over time and
    may vary according to regions and your personal contract with GCP.

    You may set `AMORA_GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD` to the appropriate value for your use case.

    More on: https://cloud.google.com/bigquery/pricing#analysis_pricing_models

    :param total_bytes: Total data processed by the query
    :return: The estimated cost in USD, based on `On-demand` price
    """
    total_terabytes = total_bytes / 1024**4
    return total_terabytes * settings.GCP_BIGQUERY_ON_DEMAND_COST_PER_TERABYTE_IN_USD


def estimated_storage_cost_in_usd(total_bytes: int) -> float:
    """
    Storage pricing is the cost to store data that you load into BigQuery.
    `Active storage` includes any table or table partition that has been modified in the last 90 days.

    - This function doesn't take into consideration that the first 10 GB of storage per month is free.
    - By default, the estimation is based on BigQuery's `Active Storage` cost per GB, which may change over time and
    may vary according to regions and your personal contract with GCP.

    You may set `AMORA_GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD` to the appropriate value for your use case.

    More on: https://cloud.google.com/bigquery/pricing#storage

    :param total_bytes: Total bytes stored into the table
    :return: The estimated cost in USD, based on `Active storage` price
    """
    total_gigabytes = total_bytes / 1024**3
    return (
        total_gigabytes * settings.GCP_BIGQUERY_ACTIVE_STORAGE_COST_PER_GIGABYTE_IN_USD
    )


def struct_for_model(model: Union[Model, AmoraModel]) -> STRUCT:
    """
    Build a BigQuery Struct type from an AmoraModel specification

    Read more: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
    """

    def fields():
        for field in model.__fields__.values():
            if field.shape is SHAPE_LIST:
                if issubclass(field.type_, AmoraModel):
                    yield field.name, ARRAY(struct_for_model(field.type_))
                else:
                    yield field.name, ARRAY(get_sqlachemy_type(field))
            elif field.sub_fields is None:
                if issubclass(field.type_, AmoraModel):
                    yield field.name, struct_for_model(field.type_)
                else:
                    yield field.name, get_sqlachemy_type(field)
            else:
                raise NotImplementedError

    return STRUCT(*fields())


def struct_for_schema_field(schema_field: SchemaField) -> STRUCT:
    """
    Build a BigQuery Struct type from a `google.cloud.bigquery.schema.SchemaField`

    Read more: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type
    """

    def fields():
        for field in schema_field.fields:
            if field.mode == "REPEATED":
                if field.field_type == "RECORD":
                    yield field.name, ARRAY(struct_for_schema_field(field))
                else:
                    sqla_type = BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES[field.field_type]
                    yield field.name, ARRAY(sqla_type)
            else:
                if field.field_type == "RECORD":
                    yield field.name, struct_for_schema_field(field)
                else:
                    sqla_type = BIGQUERY_TYPES_TO_SQLALCHEMY_TYPES[field.field_type]
                    yield field.name, sqla_type

    return STRUCT(*fields())


class struct(expression.ClauseList, expression.ColumnElement):  # type: ignore
    """
    A BigQuery STRUCT/RECORD literal.

    !!! warning "Experimental feature. You should probably use struct_for_model"
    """

    __visit_name__ = "struct"

    def __init__(self, model: AmoraModel, **kw):
        self._model = model
        self.type = struct_for_model(model)
        clauses = [
            coercions.expect(
                roles.ExpressionElementRole,
                model.dict(),
                type_=self.type,
            )
        ]
        super().__init__(*clauses, **kw)

    def bind_expression(self, bindvalue):
        return bindvalue

    def self_group(self, against=None):
        if against in (operators.any_op, operators.all_op, operators.getitem):
            return expression.Grouping(self)
        else:
            return self


class array(expression.ClauseList, expression.ColumnElement):  # type: ignore
    """
    A BigQuery ARRAY literal.

    This is used to produce `ARRAY` literals in SQL expressions, e.g.:

    ```python
    from sqlalchemy import select

    from amora.compilation import compile_statement
    from amora.providers.bigquery import array

    stmt = select([array([1, 2]).label("a"), array([3, 4, 5]).label("b")])

    compile_statement(stmt)
    ```
    Produces the SQL:

    ```sql
    SELECT
        ARRAY[1, 2] AS a,
        ARRAY[3, 4, 5]) AS b
    ```

    An instance of `array` will always have the datatype `sqlalchemy_bigquery.base.BQArray`.
    The "inner" type of the array is inferred from the values present, unless the
    ``type_`` keyword argument is passed, e.g.:

    ```python
    array(["foo", "bar"], type_=String)
    ```

    Arrays can also be constructed using `AmoraModel` instances,
    which would compile into a array of structs. E.g:

    ```python
    class Point(AmoraModel):
        x: int
        y: int


    array([Point(x=4, y=4), Point(x=2, y=2)])
    ```
    """

    __visit_name__ = "array"

    def __init__(self, clauses, **kw):
        if clauses and isinstance(clauses[0], AmoraModel):
            clauses = [struct(c) for c in clauses]

        clauses = [coercions.expect(roles.ExpressionElementRole, c) for c in clauses]
        super().__init__(*clauses, **kw)
        self._type_tuple = [arg.type for arg in clauses]
        main_type = kw.pop(
            "type_",
            self._type_tuple[0] if self._type_tuple else sqltypes.NULLTYPE,
        )
        self.type = BQArray(main_type, dimensions=1)

        for type_ in self._type_tuple:
            if type(type_) is sqltypes.NullType:
                raise ValueError("Array cannot have a null element")

    def _bind_param(self, operator, obj, _assume_scalar=False, type_=None):
        if _assume_scalar or operator is operators.getitem:
            # if getitem->slice were called, Indexable produces
            # a Slice object from that
            assert isinstance(obj, int)
            return expression.BindParameter(
                None,
                obj,
                _compared_to_operator=operator,
                type_=type_,
                _compared_to_type=self.type,
                unique=True,
            )

        else:
            return array(
                [
                    self._bind_param(operator, o, _assume_scalar=True, type_=type_)
                    for o in obj
                ]
            )

    def self_group(self, against=None):
        if against in (operators.any_op, operators.all_op, operators.getitem):
            return expression.Grouping(self)
        else:
            return self


def zip_arrays(
    *arr_columns: Column, additional_columns: Optional[List[Column]] = None
) -> Compilable:
    """
    Given at least two array columns of equal size, return a table of the unnest values,
    converting array items into rows. E.g:

    A CTE with 3 array columns: `entity`, `f1`, `f2`

    ```python
    from amora.providers.bigquery import array, cte_from_rows, zip_arrays

    cte = cte_from_rows(
        [
            {
                "entity": array([1, 2]),
                "f1": array(["f1v1", "f1v2"]),
                "f2": array(["f2v1", "f2v2"]),
            }
        ]
    )

    zip_arrays(cte.c.entity, cte.c.f1, cte.c.f2)
    ```

    Will result in the following table:

    | entity | f1   | f2   |
    |--------|------|------|
    | 1      | f1v1 | f2v1 |
    | 2      | f1v2 | f2v2 |

    If additional columns are needed from the original data, those can be selected
    using the optional `additional_columns`:

    ```python
    from amora.providers.bigquery import array, cte_from_rows, zip_arrays

    cte = cte_from_rows(
        [
            {
                "entity": array([1, 2]),
                "f1": array(["f1v1", "f1v2"]),
                "f2": array(["f2v1", "f2v2"]),
                "id": 1,
            },
            {
                "entity": array([3, 4]),
                "f1": array(["f1v3", "f1v4"]),
                "f2": array(["f2v3", "f2v4"]),
                "id": 2,
            },
        ]
    )

    zip_arrays(cte.c.entity, cte.c.f1, cte.c.f2, additional_columns=[cte.c.id])
    ```

    | entity | f1   | f2   | id   |
    |--------|------|------|------|
    | 1      | f1v1 | f2v1 | 1    |
    | 2      | f1v2 | f2v2 | 1    |
    | 3      | f1v3 | f2v3 | 2    |
    | 4      | f1v4 | f2v4 | 2    |

    Read more: [https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays#zipping_arrays](https://cloud.google.com/bigquery/docs/reference/standard-sql/arrays#zipping_arrays)
    """
    offset_alias = "off"
    offset = func.offset(literal_column(offset_alias))

    columns: List[ColumnElement] = [col[offset].label(col.key) for col in arr_columns]
    if additional_columns:
        columns += additional_columns

    return select(columns).join(
        fixed_unnest(arr_columns[0]).table_valued(with_offset=offset_alias),
        onclause=literal(1) == literal(1),
        isouter=True,
    )


@cache(lambda model, percentage=1: f"{model.unique_name}.{percentage}.{date.today()}")
def sample(model: Model, percentage: int = 1) -> pd.DataFrame:
    """
    https://cloud.google.com/bigquery/docs/table-sampling
    """
    if model.__model_config__.materialized is not MaterializationTypes.table:
        raise ValueError(
            "TABLESAMPLE SYSTEM can only be applied directly to base tables. "
            "More on: https://cloud.google.com/bigquery/docs/table-sampling#limitations"
        )

    sampling = literal_column(f"{percentage} PERCENT")
    model_sample = tablesample(model, sampling)  # type: ignore
    sampled_alias = aliased(model, model_sample)
    stmt = select(sampled_alias)

    return run(stmt).to_dataframe()
