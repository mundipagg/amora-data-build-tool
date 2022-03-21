from datetime import datetime

from amora.models import (
    AmoraModel,
    MaterializationTypes,
    ModelConfig,
    Field,
)
from tests.models.heart_rate import HeartRate


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
