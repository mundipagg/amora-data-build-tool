from typing import List

from sqlalchemy import ARRAY, Column, Integer, String

from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig, select
from amora.providers.bigquery import array, cte_from_rows
from amora.types import Compilable


class ArrayRepeatedFields(AmoraModel, table=True):
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        description="Example model with array columns",
    )

    str_arr: List[str] = Field(sa_column=Column(ARRAY(String)))
    int_arr: List[int] = Field(sa_column=Column(ARRAY(Integer)))
    id: str = Field(primary_key=True)

    @classmethod
    def source(cls) -> Compilable:
        rows = cte_from_rows(
            [
                {
                    "str_arr": array(list("amora")),
                    "int_arr": array(range(0, 5)),
                    "id": 1,
                },
                {
                    "str_arr": array(list("diogo")),
                    "int_arr": array(range(5, 10)),
                    "id": 2,
                },
            ]
        )
        return select(rows)
