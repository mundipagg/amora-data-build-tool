from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from google.cloud import bigquery
from google.cloud.bigquery import (
    Client,
    QueryJobConfig,
    RangePartitioning,
    Table,
    TimePartitioning,
    TimePartitioningType,
)

from amora.models import (
    MaterializationTypes,
    Model,
    ModelConfig,
    amora_model_for_target_path,
)


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


def materialize(sql: str, model_name: str, config: ModelConfig) -> Optional[Table]:
    materialization = config.materialized

    granularity_map = {
        "day": TimePartitioningType.DAY,
        "hour": TimePartitioningType.HOUR,
        "month": TimePartitioningType.MONTH,
        "year": TimePartitioningType.YEAR,
    }

    if materialization == MaterializationTypes.ephemeral:
        return None

    client = Client()

    if materialization == MaterializationTypes.view:
        view = Table(model_name)
        view.description = config.description
        view.labels = config.labels_dict
        view.view_query = sql

        client.delete_table(model_name, not_found_ok=True)

        return client.create_table(view)

    if materialization == MaterializationTypes.table:

        client.delete_table(model_name, not_found_ok=True)

        load_job_config = QueryJobConfig(
            destination=model_name, write_disposition="WRITE_TRUNCATE"
        )

        if config.partition_by:
            if config.partition_by.data_type == "int":
                load_job_config.range_partitioning = RangePartitioning(
                    range_=bigquery.PartitionRange(
                        config.partition_by.range,
                    ),
                    field=config.partition_by.field,
                )

            else:
                load_job_config.time_partitioning = TimePartitioning(
                    field=config.partition_by.field,
                    type_=granularity_map.get(config.partition_by.granularity),
                )

        if config.cluster_by:
            load_job_config.clustering_fields = config.cluster_by

        query_job = client.query(
            sql,
            job_config=load_job_config,
        )
        query_job.result()

        table = client.get_table(model_name)
        table.description = config.description
        table.labels = config.labels_dict

        return client.update_table(table, ["description", "labels"])

    raise ValueError(
        f"Invalid model materialization configuration. "
        f"Valid types are: `{', '.join((m.name for m in MaterializationTypes))}`. "
        f"Got: `{materialization}`"
    )
