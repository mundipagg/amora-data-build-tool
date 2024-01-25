from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Float, Integer, String, func, select

from amora.feature_store.decorators import feature_view
from amora.models import AmoraModel, Field, Label, MaterializationTypes, ModelConfig
from amora.protocols import Compilable
from amora.questions import question
from amora.transformations import datetime_trunc_hour
from amora.visualization import BigNumber, LineChart

from tests.models.steps import Steps


@feature_view
class StepCountBySource(AmoraModel):
    __depends_on__ = [Steps]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        labels={
            Label("quality", "golden"),
            Label("upstream", "apple_health"),
            Label("domain", "health"),
            Label("downstream", "dashboards"),
        },
    )

    value_avg: float = Field(Float, doc="Average step count of the hour")
    value_sum: float = Field(Float, doc="Sum of the step counts of the hour")
    value_count: int = Field(Integer, doc="Count of step count samples of the hour")

    source_name: str = Field(String, primary_key=True, doc="Source of the metric")
    event_timestamp: datetime = Field(
        TIMESTAMP,
        doc="Moment if time of which those features where observed",
        primary_key=True,
    )

    @classmethod
    def source(cls) -> Optional[Compilable]:
        datetime_trunc = func.timestamp(datetime_trunc_hour(Steps.creationDate))
        return select(
            func.avg(Steps.value).label(cls.__table__.columns.value_avg.key),
            func.sum(Steps.value).label(cls.__table__.columns.value_sum.key),
            func.count(Steps.value).label(cls.__table__.columns.value_count.key),
            Steps.sourceName,
            datetime_trunc.label(cls.__table__.columns.event_timestamp.key),
        ).group_by(cls.source_name, cls.event_timestamp)

    @classmethod
    def feature_view_entities(cls):
        return [cls.source_name]

    @classmethod
    def feature_view_features(cls):
        return [
            cls.value_avg,
            cls.value_sum,
            cls.value_count,
        ]

    @classmethod
    def feature_view_event_timestamp(cls):
        return cls.event_timestamp

    @classmethod
    def feature_view_fa_icon(cls):
        return "fa-person-running"


@question(view_config=BigNumber())
def how_many_data_points_where_acquired():
    return select(
        func.sum(StepCountBySource.value_count).label("total"),
        StepCountBySource.source_name,
    ).group_by(StepCountBySource.source_name)


@question(
    view_config=LineChart(x_func=lambda df: df["day"], y_func=lambda df: df["total"])
)
def how_many_data_points_where_acquired_per_day():
    day = func.date(StepCountBySource.event_timestamp).label("day")
    return select(
        func.sum(StepCountBySource.value_count).label("total"),
        StepCountBySource.source_name,
        day,
    ).group_by(StepCountBySource.source_name, day)
