from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Float, Integer, String, select

from amora.models import (AmoraModel, Field, Label, MaterializationTypes,
                          ModelConfig)
from amora.protocols import Compilable
from examples.amora_project.models.heart_rate import HeartRate


class HeartRateOver100(AmoraModel):
    __depends_on__ = [HeartRate]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        labels={Label("freshness", "daily")},
    )

    unit: str = Field(String, doc="Unidade de medida")
    value: float = Field(Float, doc="Valor observado")
    creationDate: datetime = Field(TIMESTAMP, doc="Data de inserção dos dados")
    id: int = Field(Integer, primary_key=True, doc="Identificador único da medida")

    @classmethod
    def source(cls) -> Optional[Compilable]:
        return select(
            [HeartRate.id, HeartRate.unit, HeartRate.creationDate, HeartRate.value]
        ).where(HeartRate.value >= 100)
