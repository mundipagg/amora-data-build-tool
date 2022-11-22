from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import TIMESTAMP, Float, Integer, String

from amora.feature_store.decorators import feature_view
from amora.meta_queries import summarize
from amora.models import AmoraModel, Field, MaterializationTypes, ModelConfig
from amora.providers.bigquery import cte_from_dataframe


@pytest.fixture(scope="module")
def step_count_by_source_100_rows() -> pd.DataFrame:
    """
    Returns:
        value_avg,value_sum,value_count,source_name,event_timestamp
        2.0,2.0,1,Diogo iPhone,2021-02-04 05:00:00.000000 UTC
        2.0,2.0,1,Diogo iPhone,2020-12-20 18:00:00.000000 UTC
        2.0,2.0,1,Diogo iPhone,2021-04-06 16:00:00.000000 UTC
        2.0,2.0,1,Diogo iPhone,2021-06-03 12:00:00.000000 UTC
        2.0,2.0,1,Diogo iPhone,2020-12-18 18:00:00.000000 UTC
        2.0,2.0,1,Diogo iPhone,2020-11-11 02:00:00.000000 UTC

    """
    csv_file_path = Path(__file__).parent.joinpath(
        "seeds/step_count_by_source_100_rows.csv"
    )
    assert csv_file_path.exists()
    return pd.read_csv(csv_file_path)


@pytest.fixture(scope="module")
def simple_model(step_count_by_source_100_rows):
    class SimpleModel(AmoraModel):
        __tablename__override__ = uuid4().hex
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.ephemeral,
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
        def source(cls):
            return cte_from_dataframe(df=step_count_by_source_100_rows)

    yield SimpleModel


@pytest.fixture(scope="module")
def simple_model_summary():
    return [
        {
            "column_name": "value_avg",
            "column_type": "FLOAT",
            "min": "2",
            "max": "8",
            "unique_count": 7,
            "avg": "5.83",
            "stddev": 2.035319447292059,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "value_sum",
            "column_type": "FLOAT",
            "min": "2",
            "max": "8",
            "unique_count": 7,
            "avg": "5.83",
            "stddev": 2.035319447292059,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "value_count",
            "column_type": "INTEGER",
            "min": "1",
            "max": "1",
            "unique_count": 1,
            "avg": "1",
            "stddev": 0.0,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "source_name",
            "column_type": "VARCHAR",
            "min": "Diogo iPhone",
            "max": "iPhone",
            "unique_count": 3,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "event_timestamp",
            "column_type": "VARCHAR",
            "min": "2019-12-20 02:00:00.000000 UTC",
            "max": "2021-07-21 16:00:00.000000 UTC",
            "unique_count": 100,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
    ]


def test_summarize_simple_model(simple_model, simple_model_summary):
    summary = summarize(simple_model).to_dict(orient="records")
    assert sorted(summary, key=lambda column: column["column_name"]) == sorted(
        simple_model_summary, key=lambda column: column["column_name"]
    )


@pytest.fixture(scope="module")
def feature_view_model(step_count_by_source_100_rows):
    @feature_view
    class FeatureViewModel(AmoraModel):
        __tablename__override__ = uuid4().hex
        __model_config__ = ModelConfig(
            materialized=MaterializationTypes.ephemeral,
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
        def source(cls):
            return cte_from_dataframe(df=step_count_by_source_100_rows)

    yield FeatureViewModel


@pytest.fixture(scope="module")
def feature_view_model_summary():
    return [
        {
            "column_name": "value_avg",
            "column_type": "FLOAT",
            "min": "2",
            "max": "8",
            "unique_count": 7,
            "avg": "5.83",
            "stddev": 2.035319447292059,
            "null_percentage": 0.0,
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "value_sum",
            "column_type": "FLOAT",
            "min": "2",
            "max": "8",
            "unique_count": 7,
            "avg": "5.83",
            "stddev": 2.035319447292059,
            "null_percentage": 0.0,
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "value_count",
            "column_type": "INTEGER",
            "min": "1",
            "max": "1",
            "unique_count": 1,
            "avg": "1",
            "stddev": 0.0,
            "null_percentage": 0.0,
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "source_name",
            "column_type": "VARCHAR",
            "min": "Diogo iPhone",
            "max": "iPhone",
            "unique_count": 3,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": True,
            "is_fv_event_timestamp": False,
        },
        {
            "column_name": "event_timestamp",
            "column_type": "VARCHAR",
            "min": "2019-12-20 02:00:00.000000 UTC",
            "max": "2021-07-21 16:00:00.000000 UTC",
            "unique_count": 100,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": True,
        },
    ]


def test_summarize_feature_view_model(feature_view_model, feature_view_model_summary):
    summary = summarize(feature_view_model).to_dict(orient="records")
    assert sorted(summary, key=lambda column: column["column_name"]) == sorted(
        feature_view_model_summary, key=lambda column: column["column_name"]
    )


def test_summarize_sourceless_ephemeral_model():
    class SourcelessModel(AmoraModel):
        __model_config__ = ModelConfig(materialized=MaterializationTypes.ephemeral)
        foo: float = Field(Float, primary_key=True)
        bar: float = Field(Float)

    with pytest.raises(ValueError):
        summarize(SourcelessModel)
