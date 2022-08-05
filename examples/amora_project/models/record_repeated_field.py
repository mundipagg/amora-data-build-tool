from typing import List

from sqlalchemy import ARRAY

from amora.models import (
    AmoraModel,
    Column,
    Field,
    MaterializationTypes,
    ModelConfig,
    select,
)
from amora.providers.bigquery import array, cte_from_rows, struct_for_model
from amora.types import Compilable


class Node(AmoraModel):
    id: str


class Edge(AmoraModel):
    from_node: Node
    to_node: Node


class RecordRepeatedFields(AmoraModel, table=True):
    __model_config__ = ModelConfig(materialized=MaterializationTypes.view)

    id: int = Field(primary_key=True)
    nodes: List[Node] = Field(sa_column=Column(ARRAY(struct_for_model(Node))))
    edges: List[Edge] = Field(sa_column=Column(ARRAY(struct_for_model(Edge))))
    root_node: Node = Field(sa_column=Column(struct_for_model(Node)))

    @classmethod
    def source(cls) -> Compilable:
        rows = cte_from_rows(
            [
                {
                    "id": 1,
                    "nodes": array([Node(id="a"), Node(id="b"), Node(id="c")]),
                    "edges": array(
                        [
                            Edge(from_node=Node(id="a"), to_node=Node(id="b")),
                            Edge(from_node=Node(id="b"), to_node=Node(id="c")),
                        ]
                    ),
                    "root_node": Node(id="a"),
                },
            ]
        )
        return select(rows)
