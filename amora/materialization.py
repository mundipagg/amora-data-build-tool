from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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

    if materialization == MaterializationTypes.ephemeral:
        return None

    client = Client()
    client.delete_table(model_name, not_found_ok=True)

    if materialization == MaterializationTypes.view:
        view = Table(model_name)
        view.description = config.description
        view.labels = config.labels_dict
        view.view_query = sql

        return client.create_table(view)

    if materialization == MaterializationTypes.table:
        query_job_config = QueryJobConfig(destination=model_name)

        if config.partition_by:
            if config.partition_by.data_type == "int":
                query_job_config.range_partitioning = RangePartitioning(
                    range_=PartitionRange(
                        start=config.partition_by.range.get("start"),
                        end=config.partition_by.range.get("end"),
                    ),
                    field=config.partition_by.field,
                )

            else:
                query_job_config.time_partitioning = TimePartitioning(
                    field=config.partition_by.field,
                    type_=config.partition_by.granularity.upper(),
                )

        query_job_config.clustering_fields = config.cluster_by

        query_job = client.query(
            sql,
            job_config=query_job_config,
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
