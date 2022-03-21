from datetime import datetime
from typing import Optional

from amora.models import (
    AmoraModel,
    select,
    MaterializationTypes,
    ModelConfig,
    Field,
)
from amora.types import Compilable
from examples.amora_project.models.heart_rate import HeartRate


class HeartRateOver100(AmoraModel, table=True):
    __tablename__ = "heart_rate_over_100"
    __depends_on__ = [HeartRate]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.view,
        labels={"freshness": "daily"},
    )

    unit: str
    value: float
    creationDate: datetime
    id: int = Field(primary_key=True)

    @classmethod
    def source(cls) -> Optional[Compilable]:
        return select(
            [HeartRate.id, HeartRate.unit, HeartRate.creationDate, HeartRate.value]
        ).where(HeartRate.value >= 100)
