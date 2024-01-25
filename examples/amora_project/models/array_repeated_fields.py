from typing import List

from sqlalchemy import ARRAY, Integer, String, select

from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig
from amora.protocols import Compilable
from amora.providers.bigquery import array, cte_from_rows


class ArrayRepeatedFields(AmoraModel):
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        description="Example model with array columns",
    )

    str_arr: List[str] = Field(ARRAY(String))
    int_arr: List[int] = Field(ARRAY(Integer))
    id: str = Field(String, primary_key=True)

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
