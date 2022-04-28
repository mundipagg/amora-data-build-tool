from datetime import datetime

from sqlmodel import Field, select

from amora.models import AmoraModel, MaterializationTypes, ModelConfig, PartitionConfig
from amora.types import Compilable

from tests.models.health import Health


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
        ).where(Health.type == "HeartRate")
