from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Column, func

from amora.feature_store.decorators import feature_view
from amora.models import (
    AmoraModel,
    Field,
    Label,
    MaterializationTypes,
    ModelConfig,
    select,
)
from amora.transformations import datetime_trunc_hour
from amora.types import Compilable

from tests.models.steps import Steps


@feature_view
class StepCountBySource(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __model_config__ = ModelConfig(
        materialized=MaterializationTypes.table,
        labels=[
            Label("quality", "golden"),
            Label("upstream", "apple_health"),
            Label("downstream", "dashboards"),
            Label("domain", "health"),
        ],
    )

    value_avg: float
    value_sum: float
    value_count: float

    source_name: str = Field(primary_key=True)
    event_timestamp: datetime = Field(primary_key=True, sa_column=Column(TIMESTAMP))

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
