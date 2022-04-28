from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional, Union

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
from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    String,
    Time,
    literal,
    literal_column,
)
from sqlalchemy.sql.selectable import CTE
from sqlalchemy_bigquery.base import unnest
from sqlmodel import AutoString

from amora.compilation import compile_statement
from amora.config import settings
from amora.contracts import BaseResult
from amora.models import Model, select
from amora.types import Compilable
from amora.version import VERSION

Schema = List[SchemaField]
BQTable = Union[Table, TableReference, str]

# todo: cobrir todos os tipos
BIGQUERY_TYPES_TO_PYTHON_TYPES = {
    "ARRAY": list,
    "BIGNUMERIC": int,
    "BOOL": bool,
    "BOOLEAN": bool,
    "BYTES": bytes,
    "DATE": date,
    "DATETIME": datetime,
    "FLOAT64": float,
    "FLOAT": float,
    "GEOGRAPHY": str,
    "INT64": int,
    "INTEGER": int,
    "JSON": dict,
    "STRING": str,
    "TIME": time,
    "TIMESTAMP": datetime,
}

SQLALCHEMY_TYPES_TO_BIGQUERY_TYPES = {
    Integer: "INTEGER",
    String: "STRING",
    AutoString: "STRING",
    DateTime: "DATETIME",
    Date: "DATE",
    Time: "TIME",
    Float: "FLOAT",
    Boolean: "BOOLEAN",
    JSON: "JSON",
    TIMESTAMP: "TIMESTAMP",
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


def get_schema_for_model(model: Model) -> Schema:
    """
    Given an `AmoraModel`, returns the equivalent bigquery `Schema`
    of the model by parsing the model SQLAlchemy column schema
    """
    columns = model.__table__.columns
    return [
        SchemaField(
            name=col.name,
            field_type=SQLALCHEMY_TYPES_TO_BIGQUERY_TYPES[col.type.__class__],
        )
        for col in columns
    ]


def get_schema_for_source(model: Model) -> Optional[Schema]:
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
    )


def dry_run(model: Model) -> Optional[DryRunResult]:
    """
    >>> dry_run(HeartRate)
    DryRunResult(
        total_bytes_processed=170181834,
        query="SELECT\n  `health`.`creationDate`,\n  `health`.`device`,\n  `health`.`endDate`,\n  `health`.`id`,\n  `health`.`sourceName`,\n  `health`.`startDate`,\n  `health`.`unit`,\n  `health`.`value`\nFROM `diogo`.`health`\nWHERE `health`.`type` = 'HeartRate'",
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

    You can use the estimate returned by the dry run to calculate query
    costs in the pricing calculator. Also useful to verify user permissions
    and query validity. You are not charged for performing the dry run.

    Read more: https://cloud.google.com/bigquery/docs/dry-run-queries
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
    def __init__(self, *args, **kwargs):
        self.name = "unnest"
        super().__init__(*args, **kwargs)


def cte_from_rows(rows: Iterable[Dict[str, Any]]) -> CTE:
    """
    Returns a table like selectable (CTE) for the given hardcoded values.

    >>> rows = [{"numeric_column": "123"}, {"numeric_column": "234"}, {"numeric_column": "345"}]
    >>> cte_from_rows(rows)

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
    a "temporary table like object", with data avaiable at runtime.

    """
    selects = [
        select([literal(value).label(name) for name, value in row.items()])
        for row in rows
    ]

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
