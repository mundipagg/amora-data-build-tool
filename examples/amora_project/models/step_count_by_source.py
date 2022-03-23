import datetime
from typing import Optional

from sqlalchemy import func

from amora.feature_store.decorators import feature_view
from amora.models import AmoraModel, ModelConfig, MaterializationTypes, select, Field
from amora.types import Compilable
from examples.amora_project.models.steps import Steps


@feature_view
class StepCountBySource(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    value_avg: float
    value_sum: float
    value_count: float

    source_name: str = Field(primary_key=True)

    date: datetime.date = Field(primary_key=True)

    @classmethod
    def source(cls) -> Optional[Compilable]:
        return select(
            [
                func.avg(Steps.value).label(cls.value_avg.key),
                func.sum(Steps.value).label(cls.value_sum.key),
                func.count(Steps.value).label(cls.value_count.key),
                Steps.sourceName.label(cls.source_name.key),
                func.date(Steps.creationDate).label(cls.date.key),
            ]
        ).group_by([cls.source_name.key, cls.date.key])

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
        return cls.date
