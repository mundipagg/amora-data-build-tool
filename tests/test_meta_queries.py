from amora.meta_queries import summarize, summarize_column

from tests.models.array_repeated_fields import ArrayRepeatedFields
from tests.models.health import Health
from tests.models.step_count_by_source import StepCountBySource


def test_summarize():
    summary = summarize(Health)
    assert summary.to_dict(orient="records") == [
        {
            "min": "2019-12-09 13:47:53+00",
            "max": "2021-07-23 03:14:19+00",
            "unique_count": 537208,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "creationDate",
            "column_type": "DATETIME",
        },
        {
            "min": "<<HKDevice: 0x282200190>, name:Mi Smart Band 4, hardware:V0.25.17.5, software:V1.0.9.66, localIdentifier:3C779DE0-B720-D2F6-47B2-51F4DFC484BF>",
            "max": "<<HKDevice: 0x28229ff70>, name:iPhone, manufacturer:Apple Inc., model:iPhone, hardware:iPhone12,5, software:14.4.2>",
            "unique_count": 423,
            "avg": None,
            "stddev": None,
            "null_percentage": 61.58241210360449,
            "column_name": "device",
            "column_type": "VARCHAR",
        },
        {
            "min": "2019-12-08 09:49:32+00",
            "max": "2021-07-23 03:14:19+00",
            "unique_count": 878606,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "endDate",
            "column_type": "DATETIME",
        },
        {
            "min": "0",
            "max": "1050052",
            "unique_count": 1050053,
            "avg": "525025.99999996088",
            "stddev": 303124.3354442229,
            "null_percentage": 0.0,
            "column_name": "id",
            "column_type": "INTEGER",
        },
        {
            "min": "Diogo iPhone",
            "max": "iPhone",
            "unique_count": 4,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "sourceName",
            "column_type": "VARCHAR",
        },
        {
            "min": "12.0.1",
            "max": "202106210942",
            "unique_count": 25,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "sourceVersion",
            "column_type": "VARCHAR",
        },
        {
            "min": "2019-12-08 09:48:52+00",
            "max": "2021-07-23 03:14:19+00",
            "unique_count": 878421,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "startDate",
            "column_type": "DATETIME",
        },
        {
            "min": "ActiveEnergyBurned",
            "max": "WalkingStepLength",
            "unique_count": 15,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "type",
            "column_type": "VARCHAR",
        },
        {
            "min": "%",
            "max": "km/hr",
            "unique_count": 10,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.3585007005625527,
            "column_name": "unit",
            "column_type": "VARCHAR",
        },
        {
            "min": "0",
            "max": "1742",
            "unique_count": 8101,
            "avg": "82.164803980394083",
            "stddev": 37.98340562839273,
            "null_percentage": 0.0,
            "column_name": "value",
            "column_type": "FLOAT",
        },
    ]


def test_summarize_feature_view_model():
    summary = summarize(StepCountBySource)
    assert summary.to_dict(orient="records") == [
        {
            "min": "2019-12-09 13:00:00+00",
            "max": "2021-07-23 02:00:00+00",
            "unique_count": 3775,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "event_timestamp",
            "column_type": "TIMESTAMP",
            "is_fv_feature": False,
            "is_fv_entity": False,
            "is_fv_event_timestamp": True,
        },
        {
            "min": "1",
            "max": "1742",
            "unique_count": 1422,
            "avg": "126.42742197839384",
            "stddev": 175.07633740644545,
            "null_percentage": 0.0,
            "column_name": "value_avg",
            "column_type": "FLOAT",
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "min": "1",
            "max": "132968",
            "unique_count": 1130,
            "avg": "663.75700483091839",
            "stddev": 3996.362789365462,
            "null_percentage": 0.0,
            "column_name": "value_sum",
            "column_type": "FLOAT",
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "min": "1",
            "max": "1374",
            "unique_count": 70,
            "avg": "4.5374396135265735",
            "stddev": 36.39923587492331,
            "null_percentage": 0.0,
            "column_name": "value_count",
            "column_type": "FLOAT",
            "is_fv_feature": True,
            "is_fv_entity": False,
            "is_fv_event_timestamp": False,
        },
        {
            "min": "Diogo iPhone",
            "max": "iPhone",
            "unique_count": 3,
            "avg": None,
            "stddev": None,
            "null_percentage": 0.0,
            "column_name": "source_name",
            "column_type": "VARCHAR",
            "is_fv_feature": False,
            "is_fv_entity": True,
            "is_fv_event_timestamp": False,
        },
    ]


def test_summarize_array_column():
    col_summary = summarize_column(ArrayRepeatedFields, ArrayRepeatedFields.int_arr)
    assert col_summary.empty
