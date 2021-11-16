from dataclasses import dataclass
from datetime import datetime, date, time
from typing import Optional, List, Literal

from google.cloud.bigquery import Client, QueryJobConfig, SchemaField

from amora.compilation import compile_statement
from amora.models import AmoraModel

Schema = List[SchemaField]

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
    "INTERVAL": "",
    "JSON": dict,
    "NUMERIC": "",
    "STRING": str,
    "TIME": time,
    "TIMESTAMP": datetime,
}


@dataclass
class DryRunResult:
    total_bytes_processed: int
    query: str
    model: AmoraModel
    referenced_tables: List[str]
    schema: Schema

    @property
    def estimated_cost(self):
        raise NotImplementedError


_client = None


def get_client() -> Client:
    global _client
    if _client is None:
        _client = Client()
    return _client


def get_schema(table_id: str) -> Schema:
    client = get_client()
    table = client.get_table(table_id)
    return table.schema


def dry_run(model: AmoraModel) -> Optional[DryRunResult]:
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
    source = model.source()
    if source is None:
        return

    query = compile_statement(source)

    query_job = get_client().query(
        query=query,
        job_config=QueryJobConfig(dry_run=True, use_query_cache=False),
    )
    tables = [table.to_api_repr() for table in query_job.referenced_tables]

    return DryRunResult(
        total_bytes_processed=query_job.total_bytes_processed,
        referenced_tables=[".".join(table.values()) for table in tables],
        query=query,
        model=model,
        schema=query_job.schema,
    )
