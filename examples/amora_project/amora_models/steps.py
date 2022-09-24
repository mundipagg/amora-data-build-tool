from datetime import datetime

from amora_models.health import Health
from sqlalchemy import TIMESTAMP
from sqlmodel import Field, select

from amora.compilation import Compilable
from amora.models import (
    AmoraModel,
    Column,
    Label,
    MaterializationTypes,
    ModelConfig,
    PartitionConfig,
)


class Steps(AmoraModel, table=True):
    __depends_on__ = [Health]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="creationDate", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by=["sourceName"],
        labels={Label("freshness", "daily")},
        description="Health automatically counts your steps, walking, and "
        "running distances. This table stores step measurement events",
    )

    id: int = Field(primary_key=True, description="Identificador único da medida")
    sourceName: str = Field(description="Origem dos dados")
    unit: str = Field(description="Unidade de medida", default="count")
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
        ).where(Health.type == "StepCount")
