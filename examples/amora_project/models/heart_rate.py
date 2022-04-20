from datetime import datetime

from sqlalchemy import TIMESTAMP

from amora.types import Compilable
from amora.models import (
    AmoraModel,
    ModelConfig,
    PartitionConfig,
    MaterializationTypes,
    Column,
)
from examples.amora_project.models.health import Health
from sqlmodel import Field, select


class HeartRate(AmoraModel, table=True):
    __tablename__ = "heart_rate"
    __depends_on__ = [Health]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="creationDate", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by=["sourceName"],
        labels={"freshness": "daily"},
    )

    id: int = Field(primary_key=True, description="Identificador único da medida")
    sourceName: str = Field(description="Origem dos dados")
    unit: str = Field(description="Unidade de medida", default="count/min")
    value: float = Field(description="Valor observado")
    device: str = Field(description="Dispositivo de origem dos dados")
    creationDate: datetime = Field(
        description="Data de inserção dos dados", sa_column=Column(TIMESTAMP)
    )
    startDate: datetime = Field(
        description="Data do início da medida", sa_column=Column(TIMESTAMP)
    )
    endDate: datetime = Field(
        description="Data do fim da medida", sa_column=Column(TIMESTAMP)
    )

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
        ).where(Health.type == "HeartRate")
