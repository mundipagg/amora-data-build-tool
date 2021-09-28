from sqlalchemy import PrimaryKeyConstraint

from amora.compilation import Compilable

from sqlmodel import func, select, SQLModel

from dbt.models.heart_rate import HeartRate


class HeartRateAgg(SQLModel, table=True):
    __depends_on__ = [HeartRate]
    __table_args__ = (PrimaryKeyConstraint("year", "month"), {})
    __tablename__ = "heart_rate_agg"

    _avg: float
    _sum: float
    _count: float
    year: int
    month: int


def source() -> Compilable:
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


output = HeartRateAgg
