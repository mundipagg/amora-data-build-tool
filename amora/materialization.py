from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from google.cloud.bigquery import Client, QueryJobConfig, Table

from amora.models import MaterializationTypes, Model, amora_model_for_target_path


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
        return f"{self.model.__name__} -> {self.sql_stmt}"


def materialize(sql: str, model: Model) -> Optional[Table]:
    config = model.__model_config__
    materialization = config.materialized

    if materialization == MaterializationTypes.view:
        view = Table(model.unique_name)
        view.description = config.description
        view.labels = config.labels
        view.view_query = sql

        return Client().create_table(view, exists_ok=True)
    elif materialization == MaterializationTypes.table:
        client = Client()
        query_job = client.query(
            sql,
            job_config=QueryJobConfig(
                destination=model.unique_name,
                write_disposition="WRITE_TRUNCATE",
            ),
        )

        result = query_job.result()

        table = client.get_table(model.unique_name)
        table.description = config.description
        table.labels = config.labels

        if config.cluster_by:
            table.clustering_fields = config.cluster_by

        return client.update_table(
            table, ["description", "labels", "clustering_fields"]
        )
    elif materialization == MaterializationTypes.ephemeral:
        return None
    else:
        raise ValueError(
            f"Invalid model materialization configuration. "
            f"Valid types are: `{', '.join((m.name for m in MaterializationTypes))}`. "
            f"Got: `{materialization}`"
        )
