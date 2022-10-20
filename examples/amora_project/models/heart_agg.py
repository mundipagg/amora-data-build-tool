from sqlalchemy import Float, Integer, func, select

from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig
from amora.protocols import Compilable
from examples.amora_project.models.heart_rate import HeartRate


class HeartRateAgg(AmoraModel):
    __tablename__override__ = "heart_rate_agg"
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    avg: float = Field(Float)
    sum: float = Field(Float)
    count: int = Field(Float)
    year: int = Field(Integer, primary_key=True)
    month: int = Field(Integer, primary_key=True)

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
