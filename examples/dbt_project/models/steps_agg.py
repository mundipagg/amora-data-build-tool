from amora.compilation import Compilable
from amora.models import AmoraModel, MaterializationTypes, ModelConfig
from dbt.models.steps import Steps
from sqlalchemy import MetaData
from sqlmodel import func, select, Field


class StepsAgg(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __tablename__ = "steps_agg"
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    _avg: float
    _sum: float
    _count: float
    year: int = Field(primary_key=True)
    month: int = Field(primary_key=True)

    metadata = MetaData(schema="stg-tau-rex.diogo")

    @classmethod
    def source(cls) -> Compilable:
        sub = select(
            [
                func.avg(Steps.value).label("_avg"),
                func.sum(Steps.value).label("_sum"),
                func.count(Steps.value).label("_count"),
                func.extract("year", Steps.creationDate).label("year"),
                func.extract("month", Steps.creationDate).label("month"),
            ]
        ).group_by(
            func.extract("year", Steps.creationDate),
            func.extract("month", Steps.creationDate),
        )
        return sub
