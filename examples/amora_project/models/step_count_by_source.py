from datetime import datetime
from typing import Optional

import humanize
from sqlalchemy import TIMESTAMP, Column, Integer, func, literal

from amora.feature_store.decorators import feature_view
from amora.models import (
    AmoraModel,
    Field,
    Label,
    MaterializationTypes,
    ModelConfig,
    select,
)
from amora.protocols import Compilable
from amora.questions import question
from amora.transformations import datetime_trunc_hour
from amora.visualization import BigNumber, LineChart, PieChart
from examples.amora_project.models.steps import Steps


@feature_view
class StepCountBySource(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        labels=[
            Label("quality", "golden"),
            Label("upstream", "apple_health"),
            Label("domain", "health"),
        ],
    )

    value_avg: float = Field(
        sa_column_kwargs=dict(comment="Average step count of the hour")
    )
    value_sum: float = Field(
        sa_column_kwargs=dict(comment="Sum of the step counts of the hour")
    )
    value_count: float = Field(
        sa_column_kwargs=dict(comment="Count of step count samples of the hour")
    )

    source_name: str = Field(
        primary_key=True, sa_column_kwargs=dict(comment="Source of the metric")
    )
    event_timestamp: datetime = Field(
        primary_key=True,
        sa_column=Column(
            TIMESTAMP, comment="Moment if time of which those features where observed"
        ),
    )

    @classmethod
    def source(cls) -> Optional[Compilable]:
        datetime_trunc = func.timestamp(datetime_trunc_hour(Steps.creationDate))
        return select(
            [
                func.avg(Steps.value).label(cls.value_avg.key),
                func.sum(Steps.value).label(cls.value_sum.key),
                func.count(Steps.value).label(cls.value_count.key),
                Steps.sourceName.label(cls.source_name.key),
                datetime_trunc.label(cls.event_timestamp.key),
            ]
        ).group_by(cls.source_name.key, cls.event_timestamp.key)

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
    return select(func.sum(StepCountBySource.value_count).label("total"))


@question()
def what_are_the_available_data_sources():
    return select(StepCountBySource.source_name).distinct()


@question(
    view_config=BigNumber(
        value_func=lambda data: humanize.naturaldate(data["event_timestamp"][0])
    )
)
def what_is_the_observation_starting_point():
    return select(func.min(StepCountBySource.event_timestamp).label("event_timestamp"))


@question(
    BigNumber(value_func=lambda data: humanize.naturaldate(data["event_timestamp"][0]))
)
def what_is_the_latest_data_point():
    return select(func.max(StepCountBySource.event_timestamp).label("event_timestamp"))


@question(view_config=PieChart(values="total", names="source_name"))
def what_is_the_total_step_count_to_date():
    """
    Qual o total de passos dados até hoje?
    """
    return select(
        func.sum(StepCountBySource.value_sum).label("total"),
        StepCountBySource.source_name,
    ).group_by(StepCountBySource.source_name)


@question(
    view_config=BigNumber(
        value_func=lambda data: humanize.intword(data["total_in_kilometers"][0])
        + " Kilometers"
    )
)
def what_is_the_current_estimated_walked_distance():
    avg_step_length_in_cm = literal(79, type_=Integer)
    estimation_in_cm = func.sum(StepCountBySource.value_sum) * avg_step_length_in_cm

    return select(
        estimation_in_cm.label("total_in_centimeters"),
        (estimation_in_cm / 100).label("total_in_meters"),
        (estimation_in_cm / 100000).label("total_in_kilometers"),
        StepCountBySource.source_name,
    ).group_by(StepCountBySource.source_name)


@question(
    view_config=LineChart(
        x_func=lambda data: data["event_timestamp"],
        y_func=lambda data: data["value_sum"],
    )
)
def what_are_the_values_observed_on_the_iphone():
    return (
        select(StepCountBySource.value_sum, StepCountBySource.event_timestamp)
        .where(StepCountBySource.source_name == "iPhone")
        .order_by(StepCountBySource.event_timestamp)
    )
