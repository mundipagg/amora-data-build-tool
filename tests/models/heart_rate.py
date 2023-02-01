from datetime import datetime

from sqlalchemy import TIMESTAMP, Float, Integer, String, select

from amora.models import (AmoraModel, Field, Label, MaterializationTypes,
                          ModelConfig, PartitionConfig)
from amora.protocols import Compilable
from tests.models.health import Health


class HeartRate(AmoraModel):
    __depends_on__ = [Health]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        partition_by=PartitionConfig(
            field="creationDate", data_type="TIMESTAMP", granularity="day"
        ),
        cluster_by=["sourceName"],
        labels={Label("freshness", "daily")},
    )

    id: int = Field(Integer, primary_key=True, doc="Identificador único da medida")
    sourceName: str = Field(String, doc="Origem dos dados")
    unit: str = Field(String, doc="Unidade de medida", default="count/min")
    value: float = Field(Float, doc="Valor observado")
    device: str = Field(String, doc="Dispositivo de origem dos dados")
    creationDate: datetime = Field(TIMESTAMP, doc="Data de inserção dos dados")
    startDate: datetime = Field(TIMESTAMP, doc="Data do início da medida")
    endDate: datetime = Field(TIMESTAMP, doc="Data do fim da medida")

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
