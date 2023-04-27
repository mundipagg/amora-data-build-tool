from sqlalchemy import Integer, func, select

from amora.models import AmoraModel, Field
from amora.protocols import Compilable

from tests.models.heart_rate import HeartRate


class HeartRateAgg(AmoraModel):
    __depends_on__ = [HeartRate]
    avg: float
    sum: float
    count: int
    year: int = Field(Integer, primary_key=True)
    month: int = Field(Integer, primary_key=True)

    @classmethod
    def source(cls) -> Compilable:
        return select(
            func.avg(HeartRate.value).label("avg"),
            func.sum(HeartRate.value).label("sum"),
            func.count(HeartRate.value).label("count"),
            func.extract("year", HeartRate.creationDate).label("year"),
            func.extract("month", HeartRate.creationDate).label("month"),
        ).group_by(
            func.extract("year", HeartRate.creationDate),
            func.extract("month", HeartRate.creationDate),
        )
