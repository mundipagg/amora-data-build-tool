from sqlmodel import Field, func, select

from amora.compilation import Compilable
from amora.models import AmoraModel, MaterializationTypes, ModelConfig
from examples.amora_project.models.steps import Steps


class StepsAgg(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __tablename__ = "steps_agg"
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    avg: float
    sum: float
    count: int
    year: int = Field(primary_key=True)
    month: int = Field(primary_key=True)

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
