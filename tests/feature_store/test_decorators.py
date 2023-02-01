from datetime import datetime
from typing import List

import pytest
from feast import FeatureView
from feast import Field as FeastField
from feast import ValueType
from feast.types import from_value_type
from sqlalchemy import ARRAY, DateTime, Float, Integer, String

from amora.feature_store.decorators import feature_view
from amora.feature_store.feature_view import name_for_model
from amora.feature_store.registry import FEATURE_REGISTRY
from amora.models import AmoraModel, Field, ModelConfig


def test_feature_view_raises_ValueError_if_model_isnt_a_valid_feature_view_source():
    class Model(AmoraModel):
        x: int = Field(Integer)
        y: int = Field(Integer)
        id: int = Field(Integer, primary_key=True)

    with pytest.raises(ValueError):
        feature_view(Model)


def test_feature_view_on_valid_source_model():
    @feature_view
    class DriverActivity(AmoraModel):
        __model_config__ = ModelConfig(
            owner="John Doe <john@example.com>", description="Description"
        )
        datetime_trunc_day: datetime = Field(DateTime, primary_key=True)
        driver: str = Field(String, primary_key=True)
        rating: float = Field(Float)
        trips_today: int = Field(Integer)
        a_str_arr_field: List[str] = Field(ARRAY(String))

        @classmethod
        def feature_view_entities(cls):
            return [cls.driver]

        @classmethod
        def feature_view_features(cls):
            return [cls.trips_today, cls.rating, cls.a_str_arr_field]

        @classmethod
        def feature_view_event_timestamp(cls):
            return cls.datetime_trunc_day

    feature_view_name = name_for_model(DriverActivity)
    fv, _, _ = FEATURE_REGISTRY[feature_view_name]

    assert isinstance(fv, FeatureView)
    assert fv.name == feature_view_name
    assert fv.batch_source.timestamp_field == DriverActivity.datetime_trunc_day.key
    assert fv.entities == ["driver"]
    assert fv.features == [
        FeastField(
            name=DriverActivity.trips_today.key, dtype=from_value_type(ValueType.INT64)
        ),
        FeastField(
            name=DriverActivity.rating.key, dtype=from_value_type(ValueType.FLOAT)
        ),
        FeastField(
            name=DriverActivity.a_str_arr_field.key,
            dtype=from_value_type(ValueType.STRING_LIST),
        ),
    ]
    assert fv.owner == "John Doe <john@example.com>"
    assert fv.description == "Description"
