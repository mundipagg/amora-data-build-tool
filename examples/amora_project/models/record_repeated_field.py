from typing import List

from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig, select
from amora.providers.bigquery import array, cte_from_rows
from amora.types import Compilable


class Node(AmoraModel):
    id: str


class Edge(AmoraModel):
    from_node: Node
    to_node: Node


class RecordRepeatedFields(AmoraModel, table=True):
    __model_config__ = ModelConfig(materialized=MaterializationTypes.view)

    id: int = Field(primary_key=True)
    nodes: List[Node]
    edges: List[Edge]
    root_node: Node

    @classmethod
    def source(cls) -> Compilable:
        rows = cte_from_rows(
            [
                {
                    "id": 1,
                    "nodes": array([Node(id="a"), Node(id="b"), Node(id="c")]),
                    "edges": array(
                        [
                            Edge(from_node="a", to_node="b"),
                            Edge(from_node="b", to_node="c"),
                        ]
                    ),
                    "root_node": Node(id="a"),
                },
            ]
        )
        return select(rows)
