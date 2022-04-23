from typing import Iterable, Dict, List

import networkx as nx
from matplotlib import pyplot as plt

from amora.config import settings
from amora.materialization import Task
from amora.utils import list_target_files


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

    @classmethod
    def from_target(cls, models=None) -> "DependencyDAG":
        """
        Builds a DependencyDAG from the files compiled at `settings.AMORA_TARGET_PATH`

        :param models:
        :return:
        """
        model_to_task = {}

        for target_file_path in list_target_files():
            if models and target_file_path.stem not in models:
                continue

            task = Task.for_target(target_file_path)
            model_to_task[task.model.unique_name] = task

        return cls.from_tasks(tasks=model_to_task.values())

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

    def root(self):
        sorted_elements = list(self)
        return sorted_elements[0]
