from sqlmodel import Field, func, select

from amora.models import AmoraModel, MaterializationTypes, ModelConfig
from amora.types import Compilable
from examples.amora_project.models.heart_rate import HeartRate


class HeartRateAgg(AmoraModel, table=True):
    __depends_on__ = [HeartRate]
    __tablename__ = "heart_rate_agg"
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    avg: float
    sum: float
    count: int
    year: int = Field(primary_key=True)
    month: int = Field(primary_key=True)

    @classmethod
    def source(cls) -> Compilable:
        return select(
            [
                func.avg(HeartRate.value).label("avg"),
                func.sum(HeartRate.value).label("sum"),
                func.count(HeartRate.value).label("count"),
                func.extract("year", HeartRate.creationDate).label("year"),
                func.extract("month", HeartRate.creationDate).label("month"),
            ]
        ).group_by(
            func.extract("year", HeartRate.creationDate),
            func.extract("month", HeartRate.creationDate),
        )
