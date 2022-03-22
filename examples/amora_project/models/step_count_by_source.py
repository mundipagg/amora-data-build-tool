import datetime
from typing import Optional

from sqlalchemy import func

from amora.feature_store import feature_view
from amora.models import AmoraModel, ModelConfig, MaterializationTypes, select, Field
from amora.types import Compilable
from examples.amora_project.models.steps import Steps


@feature_view
class StepCountBySource(AmoraModel, table=True):
    __depends_on__ = [Steps]
    __model_config__ = ModelConfig(materialized=MaterializationTypes.table)

    value_avg: float = Field(is_feature=True)
    value_sum: float = Field(is_feature=True)
    value_count: float = Field(is_feature=True)

    source_name: str = Field(is_entity=True)

    date: datetime.date

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
