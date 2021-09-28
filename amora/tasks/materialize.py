import networkx as nx
import matplotlib.pyplot as plt

from dataclasses import dataclass
from pathlib import Path
from typing import List, Any, Iterable
from amora.compilation import py_module_for_target_path
from amora.materialization import materialize
from amora.models import list_target_files


@dataclass
class Task:
    sql_stmt: str
    module: Any
    target_file_path: Path

    @classmethod
    def for_target(cls, target_file_path: Path) -> "Task":
        with open(target_file_path) as fp:
            sql_stmt = fp.read()

        module = py_module_for_target_path(target_file_path)

        return cls(
            sql_stmt=sql_stmt,
            target_file_path=target_file_path,
            module=module,
        )


class DependencyDAG(nx.DiGraph):
    def __iter__(self):
        return nx.topological_sort(self)

    @classmethod
    def from_tasks(cls, tasks: Iterable[Task]) -> "DependencyDAG":
        dag = cls()

        for task in tasks:
            model = task.module.output

            dag.add_node(model.__name__)
            for dependency in getattr(model, "__depends_on__", []):
                dag.add_edge(dependency.__name__, model.__name__)

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


if __name__ == "__main__":
    model_to_task = {}

    for target_file_path in list_target_files():
        task = Task.for_target(target_file_path)
        model = task.module.output
        model_to_task[model.__name__] = task

    dag = DependencyDAG.from_tasks(tasks=model_to_task.values())
    dag.draw()

    for model in dag:
        print("------------------------------------")
        try:
            task = model_to_task[model]
        except KeyError:
            print(f"⚠️  Skipping `{model}`")
            continue
        else:
            # todo: deveria ser `task.module.output.__table__.name` ?
            result = materialize(sql=task.sql_stmt, name=task.target_file_path.stem)

            print(f"✅  Created `{model}` as `{result.full_table_id}`")
            print(f"    Rows: {result.num_rows}")
            print(f"    Bytes: {result.num_bytes}")
