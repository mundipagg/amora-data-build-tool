from datetime import datetime

from amora.compilation import Compilable
from amora.models import (
    AmoraModel,
    ModelConfig,
    PartitionConfig,
    MaterializationTypes,
)
from examples.amora_project.models.health import Health
from sqlalchemy import MetaData
from sqlmodel import Field, select


class Steps(AmoraModel, table=True):
    __depends_on__ = [Health]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="creationDate", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by="sourceName",
        tags=["daily"],
    )

    metadata = MetaData(schema="stg-tau-rex.diogo")

    creationDate: datetime
    device: str
    endDate: datetime
    id: int = Field(primary_key=True)
    sourceName: str
    startDate: datetime
    unit: str
    value: float

    @classmethod
    def source(cls) -> Compilable:
        return select(
            [
                Health.creationDate,
                Health.device,
                Health.endDate,
                Health.id,
                Health.sourceName,
                Health.startDate,
                Health.unit,
                Health.value,
            ]
        ).where(Health.type == "StepCount")
