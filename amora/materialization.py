from google.cloud.bigquery import Table, Client
from amora.config import settings
import networkx as nx
import matplotlib.pyplot as plt

from dataclasses import dataclass
from pathlib import Path
from typing import List, Any, Iterable
from amora.compilation import py_module_for_target_path
from amora.models import list_target_files, AmoraModel


@dataclass
class Task:
    sql_stmt: str
    model: AmoraModel
    target_file_path: Path

    @classmethod
    def for_target(cls, target_file_path: Path) -> "Task":
        with open(target_file_path) as fp:
            sql_stmt = fp.read()

        module = py_module_for_target_path(target_file_path)

        return cls(
            sql_stmt=sql_stmt,
            target_file_path=target_file_path,
            model=module.output,
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


client = Client()


def materialize(sql: str, name: str) -> Table:
    view = Table(f"{settings.TARGET_PROJECT}.{settings.TARGET_SCHEMA}.{name}")
    view.view_query = sql

    return client.create_table(view, exists_ok=True)
