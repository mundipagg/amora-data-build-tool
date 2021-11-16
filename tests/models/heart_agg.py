from amora.compilation import Compilable
from amora.models import AmoraModel
from tests.models.heart_rate import HeartRate
from sqlmodel import func, select, Field


class HeartRateAgg(AmoraModel, table=True):
    __depends_on__ = [HeartRate]
    __tablename__ = "heart_rate_agg"

    _avg: float
    _sum: float
    _count: float
    year: int = Field(primary_key=True)
    month: int = Field(primary_key=True)

    @classmethod
    def source(cls) -> Compilable:
        return select(
            [
                func.avg(HeartRate.value).label("_avg"),
                func.sum(HeartRate.value).label("_sum"),
                func.count(HeartRate.value).label("_count"),
                func.extract("year", HeartRate.creationDate).label("year"),
                func.extract("month", HeartRate.creationDate).label("month"),
            ]
        ).group_by(
            func.extract("year", HeartRate.creationDate),
            func.extract("month", HeartRate.creationDate),
        )
