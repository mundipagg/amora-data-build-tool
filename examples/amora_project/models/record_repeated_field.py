from typing import List

from sqlalchemy import ARRAY, Integer, String, select
from sqlalchemy_bigquery import STRUCT

from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig
from amora.protocols import Compilable
from amora.providers.bigquery import array, cte_from_rows, struct_for_model


class Node(AmoraModel):
    id: str = Field(String, primary_key=True)


class Edge(AmoraModel):
    from_node: Node = Field(struct_for_model(Node), primary_key=True)
    to_node: Node = Field(struct_for_model(Node))


class RecordRepeatedFields(AmoraModel):
    __model_config__ = ModelConfig(materialized=MaterializationTypes.view)

    id: int = Field(Integer, primary_key=True)
    nodes: List[Node] = Field(ARRAY(struct_for_model(Node)))
    edges: List[Edge] = Field(ARRAY(STRUCT))
    root_node: Node = Field(struct_for_model(Node))

    @classmethod
    def source(cls) -> Compilable:
        node_a = Node()
        node_a.id = "a"

        node_b = Node()
        node_b.id = "b"

        node_c = Node()
        node_c.id = "c"

        edge_a_to_b = Edge()
        edge_a_to_b.from_node = node_a
        edge_a_to_b.to_node = node_b

        edge_b_to_c = Edge()
        edge_b_to_c.from_node = node_b
        edge_b_to_c.to_node = node_c

        rows = cte_from_rows(
            [
                {
                    "id": 1,
                    "nodes": array([node_a, node_b, node_c]),
                    "edges": array(
                        [
                            edge_a_to_b,
                            edge_b_to_c,
                        ]
                    ),
                    "root_node": node_a,
                },
            ]
        )

        return select(rows)
