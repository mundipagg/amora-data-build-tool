from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import humanize
from google.cloud.bigquery import (
    Client,
    PartitionRange,
    QueryJobConfig,
    RangePartitioning,
    Table,
    TimePartitioning,
)

from amora.models import (
    MaterializationTypes,
    Model,
    ModelConfig,
    amora_model_for_name,
    amora_model_for_target_path,
)
from amora.providers.bigquery import schema_for_model


@dataclass
class Task:
    sql_stmt: str
    model: Model
    target_file_path: Path

    @classmethod
    def for_target(cls, target_file_path: Path) -> "Task":
        return cls(
            sql_stmt=target_file_path.read_text(),
            model=amora_model_for_target_path(target_file_path),
            target_file_path=target_file_path,
        )

    def __repr__(self):
        return f"{self.model.unique_name()} -> {self.sql_stmt}"


@dataclass
class Result:
    model_name: str
    model_config: ModelConfig
    destination_table: Table
    total_bytes_billed: int = 0
    total_bytes_processed: int = 0
    duration: Optional[timedelta] = None

    def __str__(self):
        if self.model_config.materialized == MaterializationTypes.table:
            rows = humanize.intcomma(self.destination_table.num_rows)
            processed_bytes_ = humanize.naturalsize(self.total_bytes_processed)
            duration = humanize.naturaldelta(self.duration)
            table_bytes_ = humanize.naturalsize(self.destination_table.num_bytes)

            return f"[{self.model_name}] Took {duration} to process {processed_bytes_} of data and materialize it into a `Table` with {rows} rows and {table_bytes_}."
        elif self.model_config.materialized == MaterializationTypes.view:
            return f"[{self.model_name}] Materialized as `View`"
        else:
            raise ValueError


def materialize(sql: str, model_name: str, config: ModelConfig) -> Optional[Result]:
    materialization = config.materialized

    if materialization == MaterializationTypes.ephemeral:
        return None

    client = Client()
    client.delete_table(model_name, not_found_ok=True)
    model = amora_model_for_name(model_name)

    if materialization == MaterializationTypes.view:
        view = Table(model_name)
        view.description = config.description
        view.labels = config.labels_dict
        view.view_query = sql

        table = client.create_table(view)
        return Result(
            model_name=model_name, model_config=config, destination_table=table
        )

    if materialization == MaterializationTypes.table:
        table = Table(model_name, schema=schema_for_model(model))
        table.description = config.description
        table.labels = config.labels_dict
        table.clustering_fields = config.cluster_by

        if config.partition_by:
            if config.partition_by.data_type == "int":
                table.range_partitioning = RangePartitioning(
                    range_=PartitionRange(
                        start=config.partition_by.range.get("start"),
                        end=config.partition_by.range.get("end"),
                    ),
                    field=config.partition_by.field,
                )

            else:
                table.time_partitioning = TimePartitioning(
                    field=config.partition_by.field,
                    type_=config.partition_by.granularity.upper(),
                )

        if config.hours_to_expire:
            table.expires = datetime.utcnow() + timedelta(hours=config.hours_to_expire)

        client.create_table(table)

        query_job = client.query(
            sql,
            job_config=QueryJobConfig(destination=table),
        )
        result = query_job.result()

        return Result(
            model_name=model_name,
            model_config=config,
            destination_table=client.get_table(table),
            total_bytes_billed=query_job.total_bytes_billed,
            total_bytes_processed=query_job.total_bytes_processed,
            duration=query_job.ended - query_job.created,
        )

    raise ValueError(
        f"Invalid model materialization configuration. "
        f"Valid types are: `{', '.join((m.name for m in MaterializationTypes))}`. "
        f"Got: `{materialization}`"
    )
