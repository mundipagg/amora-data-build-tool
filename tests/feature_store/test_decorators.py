from datetime import datetime

import pytest
from feast import FeatureView, Feature, ValueType

from amora.feature_store.decorators import feature_view
from amora.feature_store.registry import FEATURE_REGISTRY
from amora.models import AmoraModel, Field


def test_feature_view_raises_ValueError_if_model_isnt_a_valid_feature_view_source():
    class Model(AmoraModel, table=True):
        x: int
        y: int
        id: int = Field(primary_key=True)

    with pytest.raises(ValueError):
        feature_view(Model)


def test_feature_view_on_valid_source_model():
    assert FEATURE_REGISTRY == {}

    @feature_view
    class DriverActivity(AmoraModel, table=True):
        datetime_trunc_day: datetime = Field(primary_key=True)
        driver: str = Field(primary_key=True)
        rating: float
        trips_today: int

        @classmethod
        def feature_view_entities(cls):
            return [cls.driver]

        @classmethod
        def feature_view_features(cls):
            return [cls.trips_today, cls.rating]

        @classmethod
        def feature_view_event_timestamp(cls) -> str:
            return cls.datetime_trunc_day.key

    fv = FEATURE_REGISTRY[DriverActivity]
    assert isinstance(fv, FeatureView)

    assert fv.name == "driver_activity"
    assert fv.input.event_timestamp_column == DriverActivity.datetime_trunc_day.key
    assert fv.entities == ["driver"]
    assert fv.features == [
        Feature(name=DriverActivity.trips_today.key, dtype=ValueType.INT64),
        Feature(name=DriverActivity.rating.key, dtype=ValueType.FLOAT),
    ]
