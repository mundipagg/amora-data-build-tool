from typing import Dict, Iterable, List

import networkx as nx
from matplotlib import pyplot as plt

from amora.config import settings
from amora.materialization import Task
from amora.models import Model
from amora.utils import list_target_files


class DependencyDAG(nx.DiGraph):
    def __iter__(self):
        # todo: validar se podemos substituir por graphlib
        return nx.topological_sort(self)

    @classmethod
    def from_model(cls, model: Model) -> "DependencyDAG":
        """
        Builds the DependencyDAG for a given model
        """
        dag = cls()
        dag.add_node(model.unique_name)

        # fixme: percorrer de forma não recursiva
        def fetch_edges(node: Model):
            for dependency in getattr(node, "__depends_on__", []):
                dag.add_edge(dependency.unique_name, node.unique_name)
                fetch_edges(dependency)

        fetch_edges(model)
        return dag

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

    def to_cytoscape_elements(self) -> List[Dict]:
        """

        Returns itself as a cytoscape schema compatible representation. E.g:

        For a `A --> B` graph:

        ```python
        [
            {"data": {"id": "A", "label": "A"}},
            {"data": {"id": "B", "label": "B"}},
            {"data": {"source": "A", "target": "B"}},
        ]
        ```
        """
        # todo: Estilizar nó de acordo com o tipo de materialização
        # todo: Adicionar metadados para cytoscape
        return [
            *({"data": {"id": node, "label": node}} for node in self.nodes),
            *(
                {"data": {"source": source, "target": target}}
                for source, target in self.edges
            ),
        ]

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
        if not sorted_elements:
            return None
        return sorted_elements[0]
