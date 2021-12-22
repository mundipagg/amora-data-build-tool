from datetime import datetime

from amora.models import AmoraModel, ModelConfig, MaterializationTypes
from sqlmodel import Field


class Health(AmoraModel, table=True):
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        description="Health data exported by the Apple Health App",
    )

    id: int = Field(primary_key=True)
    type: str
    sourceName: str
    sourceVersion: str
    unit: str
    creationDate: datetime
    startDate: datetime
    endDate: datetime
    value: float
    device: str
