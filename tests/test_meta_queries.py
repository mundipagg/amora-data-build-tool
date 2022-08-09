import pytest

from amora.meta_queries import summarize

from tests.models.health import Health
from tests.models.step_count_by_source import StepCountBySource


@pytest.fixture(scope="module")
def health_model_summary():
    return [
        {
            "avg": None,
            "column_name": "endDate",
            "column_type": "DATETIME",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "2021-07-23 03:14:19+00",
            "min": "2019-12-08 09:49:32+00",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 878606,
        },
        {
            "avg": None,
            "column_name": "type",
            "column_type": "VARCHAR",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "WalkingStepLength",
            "min": "ActiveEnergyBurned",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 15,
        },
        {
            "avg": "82.164803980394083",
            "column_name": "value",
            "column_type": "FLOAT",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "1742",
            "min": "0",
            "null_percentage": 0.0,
            "stddev": 37.98340562839273,
            "unique_count": 8101,
        },
        {
            "avg": None,
            "column_name": "sourceName",
            "column_type": "VARCHAR",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "iPhone",
            "min": "Diogo iPhone",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 4,
        },
        {
            "avg": None,
            "column_name": "sourceVersion",
            "column_type": "VARCHAR",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "202106210942",
            "min": "12.0.1",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 25,
        },
        {
            "avg": None,
            "column_name": "creationDate",
            "column_type": "DATETIME",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "2021-07-23 03:14:19+00",
            "min": "2019-12-09 13:47:53+00",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 537208,
        },
        {
            "avg": None,
            "column_name": "unit",
            "column_type": "VARCHAR",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "km/hr",
            "min": "%",
            "null_percentage": 0.35722006413009627,
            "stddev": None,
            "unique_count": 10,
        },
        {
            "avg": None,
            "column_name": "startDate",
            "column_type": "DATETIME",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "2021-07-23 03:14:19+00",
            "min": "2019-12-08 09:48:52+00",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 878421,
        },
        {
            "avg": None,
            "column_name": "device",
            "column_type": "VARCHAR",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "<<HKDevice: 0x28229ff70>, name:iPhone, manufacturer:Apple Inc., "
            "model:iPhone, hardware:iPhone12,5, software:14.4.2>",
            "min": "<<HKDevice: 0x282200190>, name:Mi Smart Band 4, hardware:V0.25.17.5, "
            "software:V1.0.9.66, "
            "localIdentifier:3C779DE0-B720-D2F6-47B2-51F4DFC484BF>",
            "null_percentage": 38.112076247579886,
            "stddev": None,
            "unique_count": 423,
        },
        {
            "avg": "525025.99999996088",
            "column_name": "id",
            "column_type": "INTEGER",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "1050052",
            "min": "0",
            "null_percentage": 0.0,
            "stddev": 303124.3354442229,
            "unique_count": 1050053,
        },
    ]


def test_summarize(health_model_summary):
    summary = summarize(Health)
    assert sorted(
        summary.to_dict(orient="records"), key=lambda column: column["column_name"]
    ) == sorted(health_model_summary, key=lambda column: column["column_name"])


@pytest.fixture(scope="module")
def step_count_by_source_model_summary():
    return [
        {
            "avg": None,
            "column_name": "source_name",
            "column_type": "VARCHAR",
            "is_fv_entity": True,
            "is_fv_event_timestamp": False,
            "is_fv_feature": False,
            "max": "iPhone",
            "min": "Diogo iPhone",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 3,
        },
        {
            "avg": "663.75700483091839",
            "column_name": "value_sum",
            "column_type": "FLOAT",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": True,
            "max": "132968",
            "min": "1",
            "null_percentage": 0.0,
            "stddev": 3996.362789365462,
            "unique_count": 1130,
        },
        {
            "avg": "4.5374396135265735",
            "column_name": "value_count",
            "column_type": "FLOAT",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": True,
            "max": "1374",
            "min": "1",
            "null_percentage": 0.0,
            "stddev": 36.39923587492331,
            "unique_count": 70,
        },
        {
            "avg": "126.42742197839384",
            "column_name": "value_avg",
            "column_type": "FLOAT",
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
            "is_fv_feature": True,
            "max": "1742",
            "min": "1",
            "null_percentage": 0.0,
            "stddev": 175.07633740644545,
            "unique_count": 1422,
        },
        {
            "avg": None,
            "column_name": "event_timestamp",
            "column_type": "TIMESTAMP",
            "is_fv_entity": False,
            "is_fv_event_timestamp": True,
            "is_fv_feature": False,
            "max": "2021-07-23 02:00:00+00",
            "min": "2019-12-09 13:00:00+00",
            "null_percentage": 0.0,
            "stddev": None,
            "unique_count": 3775,
        },
    ]


def test_summarize_feature_view_model(step_count_by_source_model_summary):
    summary = summarize(StepCountBySource).to_dict(orient="records")
    assert sorted(summary, key=lambda column: column["column_name"]) == sorted(
        step_count_by_source_model_summary, key=lambda column: column["column_name"]
    )
