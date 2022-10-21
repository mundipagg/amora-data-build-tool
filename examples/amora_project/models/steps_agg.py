from sqlalchemy import Float, Integer, func, select

from amora.compilation import Compilable
from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig
from examples.amora_project.models.steps import Steps


class StepsAgg(AmoraModel):
    __tablename__override__ = "steps_agg"
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    avg: float = Field(Float)
    sum: float = Field(Float)
    count: int = Field(Integer)
    year: int = Field(Integer, primary_key=True)
    month: int = Field(Integer, primary_key=True)

    @classmethod
    def source(cls) -> Compilable:
        sub = select(
            [
                func.avg(Steps.value).label("avg"),
                func.sum(Steps.value).label("sum"),
                func.count(Steps.value).label("count"),
                func.extract("year", Steps.creationDate).label("year"),
                func.extract("month", Steps.creationDate).label("month"),
            ]
        ).group_by(
            func.extract("year", Steps.creationDate),
            func.extract("month", Steps.creationDate),
        )
        return sub
