from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from google.cloud.bigquery import Table, Client, QueryJobConfig
import matplotlib.pyplot as plt
import networkx as nx

from amora.config import settings
from amora.models import (
    MaterializationTypes,
    amora_model_for_target_path,
    Model,
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
        return f"{self.model.__name__} -> {self.sql_stmt}"


class DependencyDAG(nx.DiGraph):
    def __iter__(self):
        # todo: validar se podemos substituir por graphlib
        return nx.topological_sort(self)

    @classmethod
    def from_tasks(cls, tasks: Iterable[Task]) -> "DependencyDAG":
        dag = cls()

        for task in tasks:
            dag.add_node(task.model.unique_name)
            for dependency in getattr(task.model, "__depends_on__", []):
                dag.add_edge(dependency.unique_name, task.model.unique_name)

        return dag

    def draw(self) -> None:
        plt.figure(1, figsize=settings.CLI_MATERIALIZATION_DAG_FIGURE_SIZE)
        nx.draw(
            self,
            with_labels=True,
            font_weight="bold",
            font_size="12",
            linewidths=4,
            node_size=150,
            node_color="white",
            font_color="green",
        )
        plt.show()


def materialize(sql: str, model: Model) -> Optional[Table]:
    materialization = model.__model_config__.materialized

    if materialization == MaterializationTypes.view:
        view = Table(model.unique_name)
        view.description = model.__model_config__.description
        view.labels = model.__model_config__.labels
        view.clustering_fields = model.__model_config__.cluster_by
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
        table.description = model.__model_config__.description
        table.labels = model.__model_config__.labels
        table.clustering_fields = model.__model_config__.cluster_by

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
