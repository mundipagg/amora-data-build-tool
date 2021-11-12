from dataclasses import dataclass
from pathlib import Path
from typing import List, Any, Iterable, Optional

from google.cloud.bigquery import Table, Client, QueryJobConfig
import matplotlib.pyplot as plt
import networkx as nx

from amora.compilation import amora_model_for_target_path
from amora.config import settings
from amora.models import list_target_files, AmoraModel, MaterializationTypes


@dataclass
class Task:
    sql_stmt: str
    model: AmoraModel
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
            dag.add_node(task.model.__name__)
            for dependency in getattr(task.model, "__depends_on__", []):
                dag.add_edge(dependency.__name__, task.model.__name__)

        return dag

    def draw(self) -> None:
        nx.draw_spectral(
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


def materialize(sql: str, model: AmoraModel) -> Optional[Table]:
    materialization = model.__model_config__.materialized
    table_id = f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.{model.__tablename__}"

    if materialization == MaterializationTypes.view:
        view = Table(table_id)
        view.view_query = sql

        return Client().create_table(view, exists_ok=True)
    elif materialization == MaterializationTypes.table:
        client = Client()
        query_job = client.query(
            sql,
            job_config=QueryJobConfig(
                destination=table_id, write_disposition="WRITE_TRUNCATE"
            ),
        )

        query_job.result()
        return client.get_table(table_id)
    elif materialization == MaterializationTypes.ephemeral:
        return None
    else:
        raise ValueError(
            f"Invalid model materialization configuration. "
            f"Valid types are: `{', '.join((m.name for m in MaterializationTypes))}`. "
            f"Got: `{materialization}`"
        )
