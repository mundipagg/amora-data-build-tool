from datetime import datetime

from sqlalchemy import MetaData
from sqlmodel import select, SQLModel, Field

from amora.compilation import Compilable
from amora.models import ModelConfig, PartitionConfig
from dbt.models.health import Health


class HeartRate(SQLModel, table=True):
    __tablename__ = "heart_rate"
    __depends_on__ = [Health]
    __config__ = ModelConfig(
        materialized="table",
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


def source() -> Compilable:
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


output = HeartRate
