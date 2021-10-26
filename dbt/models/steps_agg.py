from amora.compilation import Compilable
from amora.models import AmoraModel
from dbt.models.steps import Steps
from sqlalchemy import MetaData, PrimaryKeyConstraint
from sqlmodel import func, select


class StepsAgg(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __table_args__ = (PrimaryKeyConstraint("year", "month"), {})
    __tablename__ = "steps_agg"

    _avg: float
    _sum: float
    _count: float
    year: int
    month: int

    metadata = MetaData(schema="amora-data-build-tool.diogo")

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
